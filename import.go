package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ImportTask represents a single import task
type ImportTask struct {
	Database string
	Table    string
	PageInfo PageInfo
}

// TableSQLCache caches SQL statements and column information for a table
type TableSQLCache struct {
	AllColumns       []string
	ColumnList       string
	Placeholders     string
	SelectTemplate   string
	InsertTemplate   string
	ClusteredColumns string
}

// DetailedProgress represents detailed progress information for export and import
type DetailedProgress struct {
	TotalPages                 int64
	ExportCompletedPages       int64
	ExportFailedPages          int64
	ExportRunningPages         int64
	ImportCompletedPages       int64
	ImportFailedPages          int64
	ImportRunningPages         int64
	ExportCompletionPercentage float64
	ImportCompletionPercentage float64
}

// ImportManager manages the import operations
type ImportManager struct {
	sourceDB           *sql.DB
	destDB             *sql.DB
	threads            int
	tableName          string
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	importTaskChan     chan ImportTask
	missingTables      []string
	missingTablesMutex sync.Mutex

	// SQL cache for tables
	sqlCache      map[string]*TableSQLCache
	sqlCacheMutex sync.RWMutex

	// Row statistics
	totalImportedRows int64
	lastImportedRows  int64
	lastStatsTime     time.Time
	statsMutex        sync.RWMutex

	skipCheckExportStatus bool
	tableToProcess        []string
}

// NewImportManager creates a new import manager
func NewImportManager(sourceDB, destDB *sql.DB, threads int, tableName string, skipCheckExportStatus bool) *ImportManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ImportManager{
		sourceDB:       sourceDB,
		destDB:         destDB,
		threads:        threads,
		tableName:      tableName,
		ctx:            ctx,
		cancel:         cancel,
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
		lastStatsTime:  time.Now(),
		sqlCache:       make(map[string]*TableSQLCache),
	}
}

// GetTablesToProcess returns the list of tables to process for import
func (m *ImportManager) GetTablesToProcess() ([]string, error) {
	var tables []string
	query := `
	SELECT DISTINCT site_table 
	FROM sitemerge.export_import_summary 
	WHERE site_database = ? AND import_status != 'success' %s`

	if m.skipCheckExportStatus {
		query = fmt.Sprintf(query, "")
	} else {
		query = fmt.Sprintf(query, " AND export_status = 'success'")
	}

	if m.tableName != "" {
		// Process specific table
		tables = append(tables, m.tableName)
	} else {
		// Get all tables that are ready for import (export completed successfully)
		rows, err := m.sourceDB.Query(query, dbConfig.Database)
		if err != nil {
			return nil, fmt.Errorf("failed to get tables from export_import_summary: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				return nil, fmt.Errorf("failed to scan table name: %w", err)
			}
			tables = append(tables, table)
		}
	}

	return tables, nil
}

// InitializeImportSummary initializes the import status for tables ready for import
func (m *ImportManager) InitializeImportSummary(tables []string) error {
	for _, table := range tables {
		// Update import status to pending for tables that have successful export
		updateSQL := `
			UPDATE sitemerge.export_import_summary 
			SET import_status = 'pending'
			WHERE site_database = ? AND site_table = ? and import_status != 'success'`

		_, err := m.sourceDB.Exec(updateSQL, dbConfig.Database, table)
		if err != nil {
			return fmt.Errorf("failed to initialize import status for table %s: %w", table, err)
		}

		// Initialize page operation status for this table
		if err := m.initializePageOperationStatus(dbConfig.Database, table); err != nil {
			return fmt.Errorf("failed to initialize page operation status for table %s: %w", table, err)
		}
	}

	fmt.Printf("✅ Initialized import status for %d tables\n", len(tables))
	return nil
}

// initializePageOperationStatus initializes page operation status for a table
func (m *ImportManager) initializePageOperationStatus(database, table string) error {
	// Insert page status records for all pages of this table
	insertSQL := `
		INSERT INTO sitemerge.page_operation_status 
		(site_database, site_table, page_id, page_num, export_status, import_status)
		SELECT site_database, site_table, id, page_num, 'success', 'pending'
		FROM sitemerge.page_info 
		WHERE site_database = ? AND site_table = ?
		ON DUPLICATE KEY UPDATE 
			import_status = CASE 
				WHEN import_status != 'success' THEN 'pending' 
				ELSE import_status 
			END,
			updated_at = NOW()`

	_, err := m.sourceDB.Exec(insertSQL, database, table)
	if err != nil {
		return fmt.Errorf("failed to initialize page operation status: %w", err)
	}

	// Update total pages count in summary table
	if err := m.updateTotalPagesCount(database, table); err != nil {
		return fmt.Errorf("failed to update total pages count: %w", err)
	}

	return nil
}

// updateTotalPagesCount updates the total pages count in summary table
func (m *ImportManager) updateTotalPagesCount(database, table string) error {
	var totalPages int64
	err := m.sourceDB.QueryRow(`
		SELECT COUNT(*) 
		FROM sitemerge.page_info 
		WHERE site_database = ? AND site_table = ?`,
		database, table).Scan(&totalPages)

	if err != nil {
		return fmt.Errorf("failed to count total pages: %w", err)
	}

	_, err = m.sourceDB.Exec(`
		UPDATE sitemerge.export_import_summary 
		SET page_number = ? 
		WHERE site_database = ? AND site_table = ?`,
		totalPages, database, table)

	return err
}

// ImportScheduler periodically scans for import tasks
func (m *ImportManager) ImportScheduler() {
	defer m.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.scheduleImportTasks()
		}
	}
}

// scheduleImportTasks scans for import tasks and schedules them
func (m *ImportManager) scheduleImportTasks() {
	// First, reset any running tasks that have been stuck for too long
	m.resetStuckImportTasks()

	// DEBUG: Add logging to understand what's happening
	log.Printf("DEBUG: Starting to schedule import tasks...")

	// Get pending import tasks based on page status
	tasks, err := m.getPendingImportTasks()
	if err != nil {
		log.Printf("Error getting pending import tasks: %v", err)
		return
	}

	// DEBUG: Log the number of tasks found
	log.Printf("DEBUG: Found %d pending import tasks", len(tasks))

	// Schedule tasks
	scheduledCount := 0
	for _, task := range tasks {
		select {
		case m.importTaskChan <- task:
			scheduledCount++
		case <-m.ctx.Done():
			log.Printf("DEBUG: Context cancelled, stopping task scheduling")
			return
		}
	}

	// DEBUG: Log how many tasks were actually scheduled
	if scheduledCount > 0 {
		log.Printf("DEBUG: Successfully scheduled %d import tasks", scheduledCount)
	}
}

// resetStuckImportTasks resets import tasks that have been stuck in running state
func (m *ImportManager) resetStuckImportTasks() {
	_, err := m.sourceDB.Exec(`
		UPDATE sitemerge.page_operation_status 
		SET import_status = 'pending', 
			import_start_time = NULL,
			import_retry_count = import_retry_count + 1
		WHERE import_status = 'running' 
		AND import_start_time < DATE_SUB(NOW(), INTERVAL 10 MINUTE)
		AND import_retry_count < 3`)

	if err != nil {
		log.Printf("Error resetting stuck import tasks: %v", err)
	}
}

// getPendingImportTasks gets import tasks that are ready to be processed
func (m *ImportManager) getPendingImportTasks() ([]ImportTask, error) {
	query := `
		SELECT pi.id, pi.site_database, pi.site_table, pi.page_num, 
			   pi.start_key, pi.end_key, pi.page_size
		FROM sitemerge.page_info pi
		INNER JOIN sitemerge.page_operation_status pos 
			ON pi.site_database = pos.site_database 
			AND pi.site_table = pos.site_table 
			AND pi.id = pos.page_id
		WHERE pos.export_status = 'success' 
		AND pos.import_status IN ('pending', 'failed')
		AND pos.import_retry_count < 3
		ORDER BY pi.site_database, pi.site_table, pi.page_num
		LIMIT 500`

	// DEBUG: Log the query and database
	log.Printf("DEBUG: Executing getPendingImportTasks query for database: %s", dbConfig.Database)
	log.Printf("DEBUG: Query: %s", query)

	rows, err := m.sourceDB.Query(query)
	if err != nil {
		log.Printf("DEBUG: Query failed with error: %v", err)
		return nil, fmt.Errorf("failed to query pending import tasks: %w", err)
	}
	defer rows.Close()

	var tasks []ImportTask
	for rows.Next() {
		var pageInfo PageInfo
		var database, table string

		err := rows.Scan(&pageInfo.ID, &database, &table, &pageInfo.PageNum,
			&pageInfo.StartKey, &pageInfo.EndKey, &pageInfo.PageSize)
		if err != nil {
			log.Printf("Error scanning import task: %v", err)
			continue
		}

		pageInfo.SiteDatabase = database
		pageInfo.SiteTable = table

		task := ImportTask{
			Database: database,
			Table:    table,
			PageInfo: pageInfo,
		}

		tasks = append(tasks, task)
	}

	// DEBUG: Log what we found
	log.Printf("DEBUG: Query returned %d tasks", len(tasks))
	if len(tasks) > 0 {
		log.Printf("DEBUG: First task: database=%s, table=%s, page_num=%d, page_id=%d",
			tasks[0].Database, tasks[0].Table, tasks[0].PageInfo.PageNum, tasks[0].PageInfo.ID)
	}

	return tasks, nil
}

// ImportWorker processes import tasks
func (m *ImportManager) ImportWorker() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case task := <-m.importTaskChan:
			m.processImportTask(task)
		}
	}
}

// processImportTask processes a single import task
func (m *ImportManager) processImportTask(task ImportTask) {

	// Atomically mark this page as running
	if !m.markImportPageAsRunning(task.Database, task.Table, task.PageInfo.ID) {
		// Page is already being processed by another worker or doesn't meet criteria
		return
	}

	// Get cached SQL statements for this table
	sqlCache, err := m.getTableSQLCache(task.Database, task.Table)
	if err != nil {
		log.Printf("Error getting SQL cache for table %s.%s: %v", task.Database, task.Table, err)
		m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to get SQL cache: %v", err))
		return
	}

	// Build SELECT query to get data from source database
	whereClause := ""
	var args []interface{}

	if sqlCache.ClusteredColumns == "_tidb_rowid" {
		// For tables without clustered index, use _tidb_rowid
		whereClause = fmt.Sprintf("WHERE %s >= ? AND %s <= ?", sqlCache.ClusteredColumns, sqlCache.ClusteredColumns)
		args = append(args, task.PageInfo.StartKey, task.PageInfo.EndKey)
	} else {
		// For tables with clustered index, handle potentially composite keys
		columns := strings.Split(sqlCache.ClusteredColumns, ",")
		if len(columns) == 1 {
			// Single column clustered index
			whereClause = fmt.Sprintf("WHERE %s >= ? AND %s <= ?", strings.TrimSpace(columns[0]), strings.TrimSpace(columns[0]))
			args = append(args, task.PageInfo.StartKey, task.PageInfo.EndKey)
		} else {
			// For composite keys, we use the first column for range filtering
			// This is a simplified approach - in reality, composite key handling would be more complex
			firstColumn := strings.TrimSpace(columns[0])
			whereClause = fmt.Sprintf("WHERE %s >= ? AND %s <= ?", firstColumn, firstColumn)
			args = append(args, task.PageInfo.StartKey, task.PageInfo.EndKey)
		}
	}

	selectSQL := fmt.Sprintf(sqlCache.SelectTemplate, whereClause)

	//start := time.Now()
	// Execute SELECT query on source database
	rows, err := m.sourceDB.Query(selectSQL, args...)
	if err != nil {
		log.Printf("Error selecting data from source table %s.%s: %v", task.Database, task.Table, err)
		m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to select data: %v", err))
		return
	}
	//elapsed := time.Since(start)
	//log.Printf("Time taken to select data: %v", elapsed)
	defer rows.Close()

	// Prepare data for batch insert
	var batchData [][]interface{}
	for rows.Next() {
		// Create a slice to hold all column values
		columnValues := make([]interface{}, len(sqlCache.AllColumns))
		columnPointers := make([]interface{}, len(sqlCache.AllColumns))

		// Create pointers to the column values
		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}

		// Scan the row
		if err := rows.Scan(columnPointers...); err != nil {
			log.Printf("Error scanning row from table %s.%s: %v", task.Database, task.Table, err)
			continue
		}

		batchData = append(batchData, columnValues)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows from table %s.%s: %v", task.Database, task.Table, err)
		m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to iterate rows: %v", err))
		return
	}

	// If no data found, still mark as successful
	if len(batchData) == 0 {
		fmt.Printf("✅ No data found for page %d of table %s.%s\n", task.PageInfo.PageNum, task.Database, task.Table)
		m.markImportPageAsSuccess(task.Database, task.Table, task.PageInfo.ID)
		return
	}

	//start = time.Now()
	// Insert data into destination database using direct batch insert
	insertedRows := 0
	if len(batchData) > 0 {
		// Build batch insert SQL with multiple VALUES
		valuesClause := strings.Repeat("("+sqlCache.Placeholders+"),", len(batchData))
		valuesClause = valuesClause[:len(valuesClause)-1] // Remove last comma

		batchInsertSQL := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES %s",
			dstDbConfig.Database, task.Table, sqlCache.ColumnList, valuesClause)

		// Flatten all row data into a single slice
		var allValues []interface{}
		for _, rowData := range batchData {
			allValues = append(allValues, rowData...)
		}

		// Execute single batch insert directly
		_, err := m.destDB.Exec(batchInsertSQL, allValues...)
		if err != nil {
			// Check if it's a duplicate key error
			if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "duplicate key") {
				// log.Printf("Warning: Duplicate key error for table %s.%s page %d: %v",
				// 	task.Database, task.Table, task.PageInfo.PageNum, err)
				// Still mark as success since data already exists
				insertedRows = len(batchData)
			} else {
				log.Printf("Error executing batch insert for table %s.%s: %v", task.Database, task.Table, err)
				m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to execute batch insert: %v", err))
				return
			}
		} else {
			insertedRows = len(batchData)
		}
	}
	//elapsed = time.Since(start)
	//log.Printf("Time taken to insert data: %v", elapsed)

	// Update row statistics
	m.statsMutex.Lock()
	m.totalImportedRows += int64(insertedRows)
	m.statsMutex.Unlock()

	// Mark this page as successfully imported
	m.markImportPageAsSuccess(task.Database, task.Table, task.PageInfo.ID)
}

// printImportProgress prints current import progress for all tables
func (m *ImportManager) printImportProgress() {
	// Calculate import rate
	m.statsMutex.Lock()
	currentTime := time.Now()
	currentImportedRows := m.totalImportedRows
	elapsedTime := currentTime.Sub(m.lastStatsTime).Seconds()

	var importRate float64
	if elapsedTime > 0 {
		newRows := currentImportedRows - m.lastImportedRows
		importRate = float64(newRows) / elapsedTime
	}

	// Update last stats
	m.lastImportedRows = currentImportedRows
	m.lastStatsTime = currentTime
	m.statsMutex.Unlock()

	// Query table progress
	query := `
		SELECT site_database, site_table, page_number, 
			   import_completed_pages, import_failed_pages, import_running_pages
		FROM sitemerge.export_import_summary 
		WHERE import_status IN ('running', 'success') 
		ORDER BY site_database, site_table`

	rows, err := m.sourceDB.Query(query)
	if err != nil {
		log.Printf("	: %v", err)
		return
	}
	defer rows.Close()

	fmt.Printf("\n📊 Import Progress Summary (Total Rows: %d, Rate: %.1f rows/sec):\n",
		currentImportedRows, importRate)
	fmt.Printf("%-20s %-20s %-12s %-12s %-12s %-12s %-12s\n",
		"Database", "Table", "Total", "Completed", "Failed", "Running", "Progress")
	fmt.Printf("%s\n", strings.Repeat("-", 108))

	completedTables := 0

	for rows.Next() {
		var database, table string
		var totalPages, completedPages, failedPages, runningPages int64

		err := rows.Scan(&database, &table, &totalPages, &completedPages, &failedPages, &runningPages)
		if err != nil {
			log.Printf("Error scanning progress row: %v", err)
			continue
		}

		if !slices.Contains(m.tableToProcess, table) {
			continue
		}

		var progressPercent float64
		if totalPages > 0 {
			progressPercent = float64(completedPages) * 100.0 / float64(totalPages)
		}

		var progressStr string
		if completedPages >= totalPages {
			progressStr = "✅ 100.0%"
			completedTables++
		} else {
			progressStr = fmt.Sprintf("🔄 %.1f%%", progressPercent)
		}

		fmt.Printf("%-20s %-20s %-12d %-12d %-12d %-12d %-12s\n",
			database, table, totalPages, completedPages, failedPages, runningPages, progressStr)
	}

	fmt.Printf("\n📈 Overall Progress: %d/%d tables completed\n", completedTables, len(m.tableToProcess))
	fmt.Println(strings.Repeat("=", 108))
}

// getTableClusteredColumns retrieves the clustered columns for a table
func (m *ImportManager) getTableClusteredColumns(database, table string) (string, error) {
	var clusteredColumns sql.NullString
	query := `
		SELECT clustered_columns 
		FROM sitemerge.sitemerge_table_index_info 
		WHERE site_database = ? AND site_table = ?`

	err := m.sourceDB.QueryRow(query, database, table).Scan(&clusteredColumns)
	if err != nil {
		return "", fmt.Errorf("failed to get clustered columns: %w", err)
	}

	if clusteredColumns.Valid {
		return clusteredColumns.String, nil
	}

	return "_tidb_rowid", nil
}

// getTableColumns retrieves all column names for a table
// TODO: cache the columns in memory
func (m *ImportManager) getTableColumns(database, table string) ([]string, error) {
	query := `
		SELECT COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
		ORDER BY ORDINAL_POSITION`

	rows, err := m.sourceDB.Query(query, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %w", err)
		}
		columns = append(columns, columnName)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s.%s", database, table)
	}

	return columns, nil
}

// getTableSQLCache retrieves or creates cached SQL statements for a table
func (m *ImportManager) getTableSQLCache(database, table string) (*TableSQLCache, error) {
	cacheKey := fmt.Sprintf("%s.%s", database, table)

	// Try to get from cache first (read lock)
	m.sqlCacheMutex.RLock()
	if cache, exists := m.sqlCache[cacheKey]; exists {
		m.sqlCacheMutex.RUnlock()
		return cache, nil
	}
	m.sqlCacheMutex.RUnlock()

	// Cache miss, need to create new cache entry (write lock)
	m.sqlCacheMutex.Lock()
	defer m.sqlCacheMutex.Unlock()

	// Double-check in case another goroutine created it while we were waiting
	if cache, exists := m.sqlCache[cacheKey]; exists {
		return cache, nil
	}

	// Get table structure information
	clusteredColumns, err := m.getTableClusteredColumns(database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get clustered columns: %w", err)
	}

	// Get all columns for the table
	allColumns, err := m.getTableColumns(database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %w", err)
	}

	// Create column list and placeholders
	columnList := strings.Join(allColumns, ", ")
	placeholders := strings.Repeat("?,", len(allColumns))
	placeholders = placeholders[:len(placeholders)-1] // Remove last comma

	// Create SQL templates
	selectTemplate := fmt.Sprintf("SELECT %s FROM %s.%s %%s ORDER BY %s",
		columnList, database, table, clusteredColumns)
	insertTemplate := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		"%s", table, columnList, placeholders) // %s will be replaced with destination database

	// Create cache entry
	cache := &TableSQLCache{
		AllColumns:       allColumns,
		ColumnList:       columnList,
		Placeholders:     placeholders,
		SelectTemplate:   selectTemplate,
		InsertTemplate:   insertTemplate,
		ClusteredColumns: clusteredColumns,
	}

	// Store in cache
	m.sqlCache[cacheKey] = cache

	return cache, nil
}

// markImportPageAsRunning marks a page import as running
func (m *ImportManager) markImportPageAsRunning(database, table string, pageID int64) bool {
	var running bool
	err := m.sourceDB.QueryRow(`
		SELECT COUNT(*) > 0 
		FROM sitemerge.page_operation_status 
		WHERE site_database = ? AND site_table = ? AND page_id = ? AND import_status = 'running'`,
		database, table, pageID).Scan(&running)
	if err != nil {
		log.Printf("Error checking page status: %v", err)
		return false
	}

	if running {
		return false
	}

	// Also check if the page is already successfully imported
	var success bool
	err = m.sourceDB.QueryRow(`
		SELECT COUNT(*) > 0 
		FROM sitemerge.page_operation_status 
		WHERE site_database = ? AND site_table = ? AND page_id = ? AND import_status = 'success'`,
		database, table, pageID).Scan(&success)
	if err != nil {
		log.Printf("Error checking page success status: %v", err)
		return false
	}

	if success {
		return false
	}

	_, err = m.sourceDB.Exec(`
		UPDATE sitemerge.page_operation_status 
		SET import_status = 'running', import_start_time = NOW()
		WHERE site_database = ? AND site_table = ? AND page_id = ?`,
		database, table, pageID)
	if err != nil {
		log.Printf("Error marking page as running: %v", err)
		return false
	}

	return true
}

// markImportPageAsFailed marks a page import as failed with error message
func (m *ImportManager) markImportPageAsFailed(database, table string, pageID int64, errorMsg string) {
	_, err := m.sourceDB.Exec(`
		UPDATE sitemerge.page_operation_status 
		SET import_status = 'failed', import_error = ?
		WHERE site_database = ? AND site_table = ? AND page_id = ?`,
		errorMsg, database, table, pageID)
	if err != nil {
		log.Printf("Error updating page status to failed: %v", err)
	}
}

// markImportPageAsSuccess marks a page import as successful
func (m *ImportManager) markImportPageAsSuccess(database, table string, pageID int64) {
	tx, err := m.sourceDB.Begin()
	if err != nil {
		log.Printf("Error beginning transaction: %v", err)
		return
	}
	defer tx.Rollback()

	// Update page status
	_, err = tx.Exec(`
		UPDATE sitemerge.page_operation_status 
		SET import_status = 'success', import_end_time = NOW()
		WHERE site_database = ? AND site_table = ? AND page_id = ?`,
		database, table, pageID)
	if err != nil {
		log.Printf("Error updating page status: %v", err)
		return
	}

	// Count actual completed pages instead of using a counter
	var completedPages, totalPages int64
	err = tx.QueryRow(`
		SELECT 
			(SELECT COUNT(*) FROM sitemerge.page_operation_status 
			 WHERE site_database = ? AND site_table = ? AND import_status = 'success'),
			(SELECT COUNT(*) FROM sitemerge.page_info 
			 WHERE site_database = ? AND site_table = ?)`,
		database, table, database, table).Scan(&completedPages, &totalPages)

	if err != nil {
		log.Printf("Error counting pages: %v", err)
		return
	}

	// Update summary with accurate counts
	_, err = tx.Exec(`
		UPDATE sitemerge.export_import_summary 
		SET import_completed_pages = ?,
			page_number = ?,
			import_running_pages = (
				SELECT COUNT(*) FROM sitemerge.page_operation_status 
				WHERE site_database = ? AND site_table = ? AND import_status = 'running'
			)
		WHERE site_database = ? AND site_table = ?`,
		completedPages, totalPages, database, table, database, table)

	if err != nil {
		log.Printf("Error updating summary: %v", err)
		return
	}

	// Check if import is completed for this table
	if completedPages >= totalPages {
		// Mark entire table import as completed
		_, err = tx.Exec(`
			UPDATE sitemerge.export_import_summary 
			SET import_status = 'success', import_time = NOW()
			WHERE site_database = ? AND site_table = ?`,
			database, table)
		if err == nil {
			fmt.Printf("🎉 Import completed for entire table %s.%s (%d/%d pages)\n",
				database, table, completedPages, totalPages)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
	}
}

// Run starts the import process
func (m *ImportManager) Run() error {

	// Get tables to process
	tables, err := m.GetTablesToProcess()
	if err != nil {
		return err
	}

	m.tableToProcess = tables

	if len(tables) == 0 {
		return fmt.Errorf("no tables found to process (export must be completed first)")
	}

	// Initialize import summary
	if err := m.InitializeImportSummary(tables); err != nil {
		return err
	}

	// Print operation parameters
	fmt.Printf("🚀 Starting IMPORT operation with parameters:\n")
	fmt.Printf("   Source DB: %s:%d/%s\n", dbConfig.Host, dbConfig.Port, dbConfig.Database)
	fmt.Printf("   Dest DB: %s:%d/%s\n", dstDbConfig.Host, dstDbConfig.Port, dstDbConfig.Database)
	fmt.Printf("   There are %d tables to import, Tables: %v\n", len(m.tableToProcess), m.tableToProcess)
	fmt.Printf("   Threads: %d\n", m.threads)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n🛑 Received shutdown signal, gracefully shutting down...")
		m.cancel()
	}()

	// Start scheduler
	m.wg.Add(1)
	go m.ImportScheduler()

	// Start worker threads
	for i := 0; i < m.threads; i++ {
		m.wg.Add(1)
		go m.ImportWorker()
	}

	// Start progress reporter
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				m.printImportProgress()
			}
		}
	}()

	// Wait for completion or cancellation
	m.wg.Wait()

	// Close channels
	close(m.importTaskChan)

	// Print missing tables if any
	if len(m.missingTables) > 0 {
		fmt.Printf("\n⚠️  Missing tables in page_info:\n")
		for _, table := range m.missingTables {
			fmt.Printf("   - %s\n", table)
		}
	}

	fmt.Printf("✅ IMPORT operation completed\n")
	return nil
}
