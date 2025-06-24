package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
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

	// Row statistics
	totalImportedRows int64
	lastImportedRows  int64
	lastStatsTime     time.Time
	statsMutex        sync.RWMutex
}

// NewImportManager creates a new import manager
func NewImportManager(sourceDB, destDB *sql.DB, threads int, tableName string) *ImportManager {
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
	}
}

// CreateExportImportSummaryTable creates the export_import_summary table if it doesn't exist
func (m *ImportManager) CreateExportImportSummaryTable() error {
	// Create export_import_summary table
	createSummaryTableSQL := `
		CREATE TABLE IF NOT EXISTS sitemerge.export_import_summary (
			task_id BIGINT AUTO_INCREMENT PRIMARY KEY,
			site_database VARCHAR(255) NOT NULL,
			site_table VARCHAR(255) NOT NULL,
			total_count BIGINT DEFAULT 0,
			export_count BIGINT DEFAULT 0,
			last_success_end_key BIGINT DEFAULT NULL,
			last_success_timestamp DATETIME DEFAULT NULL,
			export_status ENUM('pending', 'running', 'success', 'failed') DEFAULT 'pending',
			export_time DATETIME DEFAULT NULL,
			export_error TEXT,
			import_status ENUM('pending', 'running', 'success', 'failed') DEFAULT 'pending',
			import_time DATETIME DEFAULT NULL,
			import_error TEXT,
			-- Page-level statistics
			page_number BIGINT DEFAULT 0,
			export_completed_pages BIGINT DEFAULT 0,
			export_failed_pages BIGINT DEFAULT 0,
			export_running_pages BIGINT DEFAULT 0,
			import_completed_pages BIGINT DEFAULT 0,
			import_failed_pages BIGINT DEFAULT 0,
			import_running_pages BIGINT DEFAULT 0,
			UNIQUE KEY db_table (site_database, site_table)
		);`

	_, err := m.sourceDB.Exec(createSummaryTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create export_import_summary table: %w", err)
	}

	// Create page_operation_status table
	createPageStatusTableSQL := `
		CREATE TABLE IF NOT EXISTS sitemerge.page_operation_status (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			site_database VARCHAR(255) NOT NULL,
			site_table VARCHAR(255) NOT NULL,
			page_id BIGINT NOT NULL,
			page_num INT NOT NULL,
			
			-- Export related status
			export_status ENUM('pending', 'running', 'success', 'failed') DEFAULT 'pending',
			export_start_time DATETIME,
			export_end_time DATETIME,
			export_error TEXT,
			export_retry_count INT DEFAULT 0,
			
			-- Import related status
			import_status ENUM('pending', 'running', 'success', 'failed', 'skipped') DEFAULT 'pending',
			import_start_time DATETIME,
			import_end_time DATETIME,
			import_error TEXT,
			import_retry_count INT DEFAULT 0,
			
			-- Common fields
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			
			UNIQUE KEY uk_page (site_database, site_table, page_id),
			INDEX idx_export_status (site_database, site_table, export_status),
			INDEX idx_import_status (site_database, site_table, import_status),
			INDEX idx_combined_status (site_database, site_table, export_status, import_status)
		);`

	_, err = m.sourceDB.Exec(createPageStatusTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create page_operation_status table: %w", err)
	}

	fmt.Println("✅ Export/Import summary and page status tables created or already exist")
	return nil
}

// GetTablesToProcess returns the list of tables to process for import
func (m *ImportManager) GetTablesToProcess() ([]string, error) {
	var tables []string

	if m.tableName != "" {
		// Process specific table
		tables = append(tables, m.tableName)
	} else {
		// Get all tables that are ready for import (export completed successfully)
		query := `
			SELECT DISTINCT site_table 
			FROM sitemerge.export_import_summary 
			WHERE site_database = ? AND export_status = 'success'`

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
			WHERE site_database = ? AND site_table = ? AND export_status = 'success'`

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
				WHEN export_status = 'success' THEN 'pending' 
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

	// Get pending import tasks based on page status
	tasks, err := m.getPendingImportTasks()
	if err != nil {
		log.Printf("Error getting pending import tasks: %v", err)
		return
	}

	// Schedule tasks
	for _, task := range tasks {
		select {
		case m.importTaskChan <- task:
		case <-m.ctx.Done():
			return
		}
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
		LIMIT 100`

	rows, err := m.sourceDB.Query(query)
	if err != nil {
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
	fmt.Printf("🔄 Importing %s.%s page %d (keys: %d-%d)\n",
		task.Database, task.Table, task.PageInfo.PageNum, task.PageInfo.StartKey, task.PageInfo.EndKey)

	// Atomically mark this page as running
	if !m.markImportPageAsRunning(task.Database, task.Table, task.PageInfo.ID) {
		// Page is already being processed by another worker or doesn't meet criteria
		return
	}

	// Get table structure information (clustered columns)
	clusteredColumns, err := m.getTableClusteredColumns(task.Database, task.Table)
	if err != nil {
		log.Printf("Error getting clustered columns for table %s.%s: %v", task.Database, task.Table, err)
		m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to get table structure: %v", err))
		return
	}

	// Get all columns for the table
	allColumns, err := m.getTableColumns(task.Database, task.Table)
	if err != nil {
		log.Printf("Error getting table columns for table %s.%s: %v", task.Database, task.Table, err)
		m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to get table columns: %v", err))
		return
	}

	// Create column list for SELECT and INSERT statements
	columnList := strings.Join(allColumns, ", ")
	placeholders := strings.Repeat("?,", len(allColumns))
	placeholders = placeholders[:len(placeholders)-1] // Remove last comma

	// Build SELECT query to get data from source database
	whereClause := ""
	var args []interface{}

	if clusteredColumns == "_tidb_rowid" {
		// For tables without clustered index, use _tidb_rowid
		whereClause = fmt.Sprintf("WHERE %s >= ? AND %s <= ?", clusteredColumns, clusteredColumns)
		args = append(args, task.PageInfo.StartKey, task.PageInfo.EndKey)
	} else {
		// For tables with clustered index, handle potentially composite keys
		columns := strings.Split(clusteredColumns, ",")
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

	selectSQL := fmt.Sprintf("SELECT %s FROM %s.%s %s ORDER BY %s",
		columnList, task.Database, task.Table, whereClause, clusteredColumns)

	// Execute SELECT query on source database
	rows, err := m.sourceDB.Query(selectSQL, args...)
	if err != nil {
		log.Printf("Error selecting data from source table %s.%s: %v", task.Database, task.Table, err)
		m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to select data: %v", err))
		return
	}
	defer rows.Close()

	// Prepare data for batch insert
	var batchData [][]interface{}
	for rows.Next() {
		// Create a slice to hold all column values
		columnValues := make([]interface{}, len(allColumns))
		columnPointers := make([]interface{}, len(allColumns))

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

	// Insert data into destination database
	insertSQL := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		dstDbConfig.Database, task.Table, columnList, placeholders)

	// Use transaction for batch insert
	tx, err := m.destDB.Begin()
	if err != nil {
		log.Printf("Error beginning transaction for table %s.%s: %v", task.Database, task.Table, err)
		m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to begin transaction: %v", err))
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		log.Printf("Error preparing insert statement for table %s.%s: %v", task.Database, task.Table, err)
		m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to prepare insert: %v", err))
		return
	}
	defer stmt.Close()

	// Execute batch insert
	insertedRows := 0
	for _, rowData := range batchData {
		_, err := stmt.Exec(rowData...)
		if err != nil {
			log.Printf("Error inserting row into table %s.%s: %v", task.Database, task.Table, err)
			// Continue with other rows instead of failing the entire batch
			continue
		}
		insertedRows++
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction for table %s.%s: %v", task.Database, task.Table, err)
		m.markImportPageAsFailed(task.Database, task.Table, task.PageInfo.ID, fmt.Sprintf("Failed to commit transaction: %v", err))
		return
	}

	fmt.Printf("✅ Successfully imported %d/%d rows for page %d of table %s.%s\n",
		insertedRows, len(batchData), task.PageInfo.PageNum, task.Database, task.Table)

	// Update row statistics
	m.statsMutex.Lock()
	m.totalImportedRows += int64(insertedRows)
	m.statsMutex.Unlock()

	// Mark this page as successfully imported
	m.markImportPageAsSuccess(task.Database, task.Table, task.PageInfo.ID)
}

// getDetailedProgress gets detailed progress information for export and import
func (m *ImportManager) getDetailedProgress(database, table string) (*DetailedProgress, error) {
	var progress DetailedProgress
	err := m.sourceDB.QueryRow(`
		SELECT 
			page_number,
			export_completed_pages, export_failed_pages, export_running_pages,
			import_completed_pages, import_failed_pages, import_running_pages,
			CASE 
				WHEN page_number > 0 THEN (export_completed_pages * 100.0 / page_number)
				ELSE 0 
			END as export_completion_percentage,
			CASE 
				WHEN page_number > 0 THEN (import_completed_pages * 100.0 / page_number)
				ELSE 0 
			END as import_completion_percentage
		FROM sitemerge.export_import_summary 
		WHERE site_database = ? AND site_table = ?`,
		database, table).Scan(
		&progress.TotalPages,
		&progress.ExportCompletedPages, &progress.ExportFailedPages, &progress.ExportRunningPages,
		&progress.ImportCompletedPages, &progress.ImportFailedPages, &progress.ImportRunningPages,
		&progress.ExportCompletionPercentage, &progress.ImportCompletionPercentage)

	return &progress, err
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
		log.Printf("Error querying import progress: %v", err)
		return
	}
	defer rows.Close()

	fmt.Printf("\n📊 Import Progress Summary (Total Rows: %d, Rate: %.1f rows/sec):\n",
		currentImportedRows, importRate)
	fmt.Printf("%-20s %-20s %-12s %-12s %-12s %-12s %-12s\n",
		"Database", "Table", "Total", "Completed", "Failed", "Running", "Progress")
	fmt.Printf("%s\n", strings.Repeat("-", 108))

	totalTables := 0
	completedTables := 0

	for rows.Next() {
		var database, table string
		var totalPages, completedPages, failedPages, runningPages int64

		err := rows.Scan(&database, &table, &totalPages, &completedPages, &failedPages, &runningPages)
		if err != nil {
			log.Printf("Error scanning progress row: %v", err)
			continue
		}

		totalTables++

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

	fmt.Printf("\n📈 Overall Progress: %d/%d tables completed\n", completedTables, totalTables)
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

	// Atomically update summary counters
	_, err = tx.Exec(`
		UPDATE sitemerge.export_import_summary 
		SET import_completed_pages = import_completed_pages + 1,
			import_running_pages = import_running_pages - 1
		WHERE site_database = ? AND site_table = ?`,
		database, table)
	if err != nil {
		log.Printf("Error updating summary counters: %v", err)
		return
	}

	// Check if import is completed for this table
	var importCompletedPages, totalPages int64
	err = tx.QueryRow(`
		SELECT import_completed_pages, page_number 
		FROM sitemerge.export_import_summary 
		WHERE site_database = ? AND site_table = ?`,
		database, table).Scan(&importCompletedPages, &totalPages)

	if err == nil && importCompletedPages >= totalPages {
		// Mark entire table import as completed
		_, err = tx.Exec(`
			UPDATE sitemerge.export_import_summary 
			SET import_status = 'success', import_time = NOW()
			WHERE site_database = ? AND site_table = ?`,
			database, table)
		if err == nil {
			fmt.Printf("🎉 Import completed for entire table %s.%s (%d/%d pages)\n",
				database, table, importCompletedPages, totalPages)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
	}
}

// Run starts the import process
func (m *ImportManager) Run() error {
	// Create export_import_summary table
	if err := m.CreateExportImportSummaryTable(); err != nil {
		return err
	}

	// Get tables to process
	tables, err := m.GetTablesToProcess()
	if err != nil {
		return err
	}

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
	fmt.Printf("   Tables: %v\n", tables)
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
