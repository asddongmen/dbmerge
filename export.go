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

// ExportImportSummary represents the export/import status table“
type ExportImportSummary struct {
	TaskID       int64  `db:"task_id"`
	SiteDatabase string `db:"site_database"`
	SiteTable    string `db:"site_table"`
	// Page number of the table
	PageNumber int `db:"page_number"`
	// Total row count of the table
	TotalCount int64 `db:"total_count"`
	// Exported row count of the table
	ExportCount          int64      `db:"export_count"`
	LastSuccessEndKey    *int64     `db:"last_success_end_key"`
	LastSuccessTimestamp *time.Time `db:"last_success_timestamp"`
	ExportStatus         string     `db:"export_status"`
	ExportTime           *time.Time `db:"export_time"`
	ExportError          *string    `db:"export_error"`
	ImportStatus         string     `db:"import_status"`
	ImportTime           *time.Time `db:"import_time"`
	ImportError          *string    `db:"import_error"`
}

// PageInfo represents the page information for data processing
type PageInfo struct {
	ID           int64  `db:"id"`
	SiteDatabase string `db:"site_database"`
	SiteTable    string `db:"site_table"`
	PageNum      int    `db:"page_num"`
	StartKey     int64  `db:"start_key"`
	EndKey       int64  `db:"end_key"`
	PageSize     int    `db:"page_size"`
}

// ExportTask represents a single export task
type ExportTask struct {
	Database string
	Table    string
	PageInfo PageInfo
}

// ExportManager manages the export operations
type ExportManager struct {
	sourceDB           *sql.DB
	threads            int
	tableName          string
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	exportTaskChan     chan ExportTask
	missingTables      []string
	missingTablesMutex sync.Mutex

	// Statistics
	totalExportedRows int64
	completedTables   map[string]bool
	lastExportedRows  int64
	lastStatsTime     time.Time
	statsMutex        sync.RWMutex

	// Table tracking
	tableStatus      map[string]*TableExportStatus
	tableStatusMutex sync.RWMutex

	// Tables to process
	tablesToProcess    []string
	tablesProcessMutex sync.RWMutex
}

// TableExportStatus tracks the export status of each table
type TableExportStatus struct {
	Database    string
	Table       string
	IsCompleted bool
}

// NewExportManager creates a new export manager
func NewExportManager(sourceDB *sql.DB, threads int, tableName string) *ExportManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ExportManager{
		sourceDB:        sourceDB,
		threads:         threads,
		tableName:       tableName,
		ctx:             ctx,
		cancel:          cancel,
		exportTaskChan:  make(chan ExportTask, 100),
		missingTables:   make([]string, 0),
		completedTables: make(map[string]bool),
		lastStatsTime:   time.Now(),
		tableStatus:     make(map[string]*TableExportStatus),
	}
}

// CreateExportImportSummaryTable creates the export_import_summary table if it doesn't exist
func (m *ExportManager) CreateExportImportSummaryTable() error {
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS sitemerge.export_import_summary (
			task_id BIGINT AUTO_INCREMENT PRIMARY KEY,
			site_database VARCHAR(255) NOT NULL,
			site_table VARCHAR(255) NOT NULL,
			page_number INT NOT NULL,
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
			import_completed_pages BIGINT DEFAULT 0,
			import_failed_pages BIGINT DEFAULT 0,
			import_running_pages BIGINT DEFAULT 0,

			UNIQUE KEY db_table (site_database, site_table)
		);`

	_, err := m.sourceDB.Exec(createTableSQL)
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
			export_completed_pages BIGINT DEFAULT 0,
			export_failed_pages BIGINT DEFAULT 0,
			export_running_pages BIGINT DEFAULT 0,
			export_retry_count INT DEFAULT 0,
			
			-- Import related status
			import_status ENUM('pending', 'running', 'success', 'failed', 'skipped') DEFAULT 'pending',
			import_start_time DATETIME,
			import_end_time DATETIME,
			import_completed_pages BIGINT DEFAULT 0,
			import_failed_pages BIGINT DEFAULT 0,
			import_running_pages BIGINT DEFAULT 0,
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

// GetTablesToProcess returns the list of tables to process for export
func (m *ExportManager) GetTablesToProcess() ([]string, error) {
	var tables []string

	if m.tableName != "" {
		// Process specific table
		tables = append(tables, m.tableName)
	} else {
		// Get all tables from the database
		query := `
			SELECT DISTINCT site_table 
			FROM sitemerge.page_info_progress 
			WHERE site_database = ?`

		rows, err := m.sourceDB.Query(query, dbConfig.Database)
		if err != nil {
			return nil, fmt.Errorf("failed to get tables from page_info_progress: %w", err)
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

// InitializeExportSummary initializes the export_import_summary table with pending records for export
func (m *ExportManager) InitializeExportSummary(tables []string) error {
	for _, table := range tables {
		// Get total count from page_info_progress
		var totalCount int64
		query := `
			SELECT COALESCE(processed_rows, 0) 
			FROM sitemerge.page_info_progress 
			WHERE site_database = ? AND site_table = ?`

		err := m.sourceDB.QueryRow(query, dbConfig.Database, table).Scan(&totalCount)
		if err != nil {
			if err == sql.ErrNoRows {
				totalCount = 0
			} else {
				return fmt.Errorf("failed to get total count for table %s: %w", table, err)
			}
		}

		// Insert or update the record
		insertSQL := `
			INSERT INTO sitemerge.export_import_summary 
			(site_database, site_table, page_number, total_count, export_status, import_status)
			VALUES (?, ?, ?, ?, 'pending', 'pending')
			ON DUPLICATE KEY UPDATE 
			total_count = VALUES(total_count),
			export_status = 'pending'`

		_, err = m.sourceDB.Exec(insertSQL, dbConfig.Database, table, 0, totalCount)
		if err != nil {
			return fmt.Errorf("failed to initialize export_import_summary for table %s: %w", table, err)
		}
	}

	fmt.Printf("✅ Initialized export_import_summary records for %d tables\n", len(tables))
	return nil
}

// ExportScheduler periodically scans for pending export tasks
func (m *ExportManager) ExportScheduler() {
	defer m.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.scheduleExportTasks()
		}
	}
}

// scheduleExportTasks scans for pending export tasks and schedules them
func (m *ExportManager) scheduleExportTasks() {
	// Get pending and running export tasks
	query := `
		SELECT site_database, site_table 
		FROM sitemerge.export_import_summary 
		WHERE export_status IN ('pending', 'running')
		ORDER BY site_table`

	rows, err := m.sourceDB.Query(query)
	if err != nil {
		log.Printf("Error getting pending export tasks: %v", err)
		return
	}
	defer rows.Close()

	var tables []struct {
		database string
		table    string
	}

	for rows.Next() {
		var database, table string
		if err := rows.Scan(&database, &table); err != nil {
			log.Printf("Error scanning export task: %v", err)
			continue
		}

		// Update status to running (only if it was pending)
		_, err = m.sourceDB.Exec(`
			UPDATE sitemerge.export_import_summary 
			SET export_status = 'running' 
			WHERE site_database = ? AND site_table = ? AND export_status = 'pending'`,
			database, table)
		if err != nil {
			log.Printf("Error updating export status to running: %v", err)
			continue
		}

		tables = append(tables, struct {
			database string
			table    string
		}{database, table})
	}

	if len(tables) == 0 {
		// No pending or running tables, check if all tables are completed and exit if so
		m.checkAllTablesCompleteAndExit()
		return
	}

	// Select up to 8 tables per round
	const maxTablesPerRound = 8
	const tasksPerTable = 1000

	selectedTables := tables
	if len(tables) > maxTablesPerRound {
		selectedTables = tables[:maxTablesPerRound]
	}

	// Schedule tasks for selected tables
	totalScheduled := 0
	for _, t := range selectedTables {
		tasks := m.getExportTasksForTable(t.database, t.table, tasksPerTable)

		scheduledForTable := 0
		for _, task := range tasks {
			m.exportTaskChan <- task
			scheduledForTable++
			totalScheduled++
		}
	}

	// Check completion for all running tables
	for _, t := range tables {
		m.checkAndMarkTableCompleteIfNeeded(t.database, t.table)
	}

	// Check if all tables are completed and exit if so
	m.checkAllTablesCompleteAndExit()
}

// getExportTasksForTable gets export tasks for a specific table
func (m *ExportManager) getExportTasksForTable(database, table string, maxTasks int) []ExportTask {
	// Query pages that haven't been processed successfully
	query := `
		SELECT pi.id, pi.site_database, pi.site_table, pi.page_num, pi.start_key, pi.end_key, pi.page_size
		FROM sitemerge.page_info pi
		LEFT JOIN sitemerge.page_operation_status pos 
			ON pi.site_database = pos.site_database 
			AND pi.site_table = pos.site_table 
			AND pi.id = pos.page_id
		WHERE pi.site_database = ? AND pi.site_table = ? 
		AND (pos.export_status IS NULL OR pos.export_status != 'success')
		ORDER BY pi.id
		LIMIT ?`

	rows, err := m.sourceDB.Query(query, database, table, maxTasks)
	if err != nil {
		log.Printf("Error getting tasks for table %s.%s: %v", database, table, err)
		return nil
	}
	defer rows.Close()

	var tasks []ExportTask
	for rows.Next() {
		var pageInfo PageInfo
		if err := rows.Scan(&pageInfo.ID, &pageInfo.SiteDatabase, &pageInfo.SiteTable,
			&pageInfo.PageNum, &pageInfo.StartKey, &pageInfo.EndKey, &pageInfo.PageSize); err != nil {
			log.Printf("Error scanning page info: %v", err)
			continue
		}

		task := ExportTask{
			Database: database,
			Table:    table,
			PageInfo: pageInfo,
		}
		tasks = append(tasks, task)
	}

	return tasks
}

// checkAllTablesCompleteAndExit checks if all tables are completed and exits if so
func (m *ExportManager) checkAllTablesCompleteAndExit() {
	// Get the tables to process
	m.tablesProcessMutex.RLock()
	tables := make([]string, len(m.tablesToProcess))
	copy(tables, m.tablesToProcess)
	m.tablesProcessMutex.RUnlock()

	if len(tables) == 0 {
		return
	}

	// Check if all tables are completed
	var completedCount int
	err := m.sourceDB.QueryRow(`
		SELECT COUNT(*) 
		FROM sitemerge.export_import_summary 
		WHERE site_database = ? 
		AND site_table IN (`+strings.Repeat("?,", len(tables)-1)+`?) 
		AND export_status = 'success'`,
		append([]interface{}{dbConfig.Database}, stringSliceToInterfaceSlice(tables)...)...).Scan(&completedCount)

	if err != nil {
		log.Printf("Error checking completed tables count: %v", err)
		return
	}

	// If all tables are completed, initiate shutdown
	if completedCount == len(tables) {
		fmt.Printf("\n🎉 All %d tables have been successfully exported! Initiating shutdown...\n", len(tables))
		m.cancel()
	}
}

// stringSliceToInterfaceSlice converts []string to []interface{}
func stringSliceToInterfaceSlice(slice []string) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// checkAndMarkTableCompleteIfNeeded checks if a table should be marked as complete
func (m *ExportManager) checkAndMarkTableCompleteIfNeeded(database, table string) {
	// Count remaining pages that haven't been processed
	var remainingPages int64
	err := m.sourceDB.QueryRow(`
		SELECT COUNT(*) 
		FROM sitemerge.page_info pi
		LEFT JOIN sitemerge.page_operation_status pos 
			ON pi.site_database = pos.site_database 
			AND pi.site_table = pos.site_table 
			AND pi.id = pos.page_id
		WHERE pi.site_database = ? AND pi.site_table = ? 
		AND (pos.export_status IS NULL OR pos.export_status != 'success')`,
		database, table).Scan(&remainingPages)

	if err != nil {
		log.Printf("Error checking remaining pages for table %s.%s: %v", database, table, err)
		return
	}

	// Only mark as complete if no remaining pages
	if remainingPages == 0 {
		tableKey := fmt.Sprintf("%s.%s", database, table)

		// Mark table status as completed
		m.tableStatusMutex.Lock()
		if status, exists := m.tableStatus[tableKey]; exists {
			status.IsCompleted = true
		}
		m.tableStatusMutex.Unlock()

		// Call the full completion check
		m.checkAndMarkTableComplete(database, table)
	}
}

// ExportWorker processes export tasks
func (m *ExportManager) ExportWorker() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case task := <-m.exportTaskChan:
			m.processExportTask(task)
		}
	}
}

// processExportTask processes a single export task
func (m *ExportManager) processExportTask(task ExportTask) {
	// Insert page operation status at the start of processing
	_, err := m.sourceDB.Exec(`
		INSERT INTO sitemerge.page_operation_status 
		(site_database, site_table, page_id, page_num, export_status, import_status, export_start_time)
		VALUES (?, ?, ?, ?, 'running', 'pending', NOW())
		ON DUPLICATE KEY UPDATE 
			export_status = 'running',
			export_start_time = NOW(),
			updated_at = NOW()`,
		task.Database, task.Table, task.PageInfo.ID, task.PageInfo.PageNum)

	if err != nil {
		log.Printf("Error inserting page operation status: %v", err)
		return
	}

	// TODO: Implement actual export logic here
	// For now, just simulate the export by marking it as complete
	// fmt.Printf("🔄 Exporting %s.%s page %d (keys: %d-%d)\n",
	// 	task.Database, task.Table, task.PageInfo.PageNum, task.PageInfo.StartKey, task.PageInfo.EndKey)

	// Calculate exported rows (using page size as approximation)
	exportedRows := int64(task.PageInfo.PageSize)

	// Mark export page as success and update statistics
	m.markExportPageAsSuccess(task.Database, task.Table, task.PageInfo, exportedRows)
}

// markExportPageAsSuccess marks a page export as successful
func (m *ExportManager) markExportPageAsSuccess(database, table string, pageInfo PageInfo, exportedRows int64) {
	tx, err := m.sourceDB.Begin()
	if err != nil {
		log.Printf("Error beginning transaction: %v", err)
		return
	}
	defer tx.Rollback()

	// Update page status
	_, err = tx.Exec(`
		UPDATE sitemerge.page_operation_status 
		SET export_status = 'success', export_end_time = NOW()
		WHERE site_database = ? AND site_table = ? AND page_id = ?`,
		database, table, pageInfo.ID)
	if err != nil {
		log.Printf("Error updating page status: %v", err)
		return
	}

	// Atomically update summary counters
	_, err = tx.Exec(`
		UPDATE sitemerge.export_import_summary 
		SET export_count = export_count + ?,
			page_number = page_number + 1,
			last_success_end_key = ?,
			last_success_timestamp = NOW()
		WHERE site_database = ? AND site_table = ?`, pageInfo.PageSize,
		pageInfo.EndKey, database, table)
	if err != nil {
		log.Printf("Error updating summary counters: %v", err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		return
	}

	// Update statistics
	m.statsMutex.Lock()
	m.totalExportedRows += exportedRows
	m.statsMutex.Unlock()

	// Check if this table should be marked as complete
	m.checkAndMarkTableCompleteIfNeeded(database, table)
}

// StatsReporter periodically prints export statistics
func (m *ExportManager) StatsReporter() {
	defer m.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.printExportStats()
		}
	}
}

// printExportStats prints current export statistics
func (m *ExportManager) printExportStats() {
	m.statsMutex.Lock()
	totalRows := m.totalExportedRows
	completedTablesCount := len(m.completedTables)
	currentTime := time.Now()
	timeDiff := currentTime.Sub(m.lastStatsTime).Seconds()

	var qps float64
	if timeDiff > 0 {
		rowsDiff := totalRows - m.lastExportedRows
		qps = float64(rowsDiff) / timeDiff
	}

	m.lastExportedRows = totalRows
	m.lastStatsTime = currentTime
	m.statsMutex.Unlock()

	// Get active tasks count
	activeTasksCount := len(m.exportTaskChan)

	// Get running tables count
	var runningTablesCount int
	err := m.sourceDB.QueryRow(`
		SELECT COUNT(*) 
		FROM sitemerge.export_import_summary 
		WHERE export_status = 'running'`).Scan(&runningTablesCount)
	if err != nil {
		runningTablesCount = -1 // Error indicator
	}

	// Get pending pages count for debugging
	var pendingPagesCount int

	// Get the tables to process
	m.tablesProcessMutex.RLock()
	tables := make([]string, len(m.tablesToProcess))
	copy(tables, m.tablesToProcess)
	m.tablesProcessMutex.RUnlock()

	if len(tables) > 0 {
		// Create placeholders for IN clause
		placeholders := make([]string, len(tables))
		args := make([]interface{}, len(tables)+1)
		args[0] = dbConfig.Database

		for i, table := range tables {
			placeholders[i] = "?"
			args[i+1] = table
		}

		query := fmt.Sprintf(`
			SELECT COUNT(*) 
			FROM sitemerge.page_info pi
			LEFT JOIN sitemerge.page_operation_status pos 
				ON pi.site_database = pos.site_database 
				AND pi.site_table = pos.site_table 
				AND pi.id = pos.page_id
			WHERE pi.site_database = ? 
			AND pi.site_table IN (%s)
			AND (pos.export_status IS NULL OR pos.export_status != 'success')`,
			strings.Join(placeholders, ","))

		err = m.sourceDB.QueryRow(query, args...).Scan(&pendingPagesCount)
	} else {
		pendingPagesCount = 0
		err = nil
	}

	if err != nil {
		log.Printf("Error getting pending pages count: %v", err)
		pendingPagesCount = -1 // Error indicator
	}

	fmt.Printf("📊 Export Stats: Rows: %d | Completed: %d | Running: %d | Pending Pages: %d | QPS: %.2f | Queue: %d\n",
		totalRows, completedTablesCount, runningTablesCount, pendingPagesCount, qps, activeTasksCount)
}

// Run starts the export process
func (m *ExportManager) Run() error {
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
		return fmt.Errorf("no tables found to process")
	}

	// Store tables to process for statistics
	m.tablesProcessMutex.Lock()
	m.tablesToProcess = tables
	m.tablesProcessMutex.Unlock()

	// Initialize export_import_summary
	if err := m.InitializeExportSummary(tables); err != nil {
		return err
	}

	// Print operation parameters
	fmt.Printf("🚀 Starting EXPORT operation with parameters:\n")
	fmt.Printf("   Source DB: %s:%d/%s\n", dbConfig.Host, dbConfig.Port, dbConfig.Database)
	fmt.Printf("   Tables: %v\n", tables)
	fmt.Printf("   Threads: %d\n", m.threads)

	// Add diagnostic information
	// m.printDiagnosticInfo(tables)

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
	go m.ExportScheduler()

	// Start stats reporter
	m.wg.Add(1)
	go m.StatsReporter()

	// Start worker threads
	for i := 0; i < m.threads; i++ {
		m.wg.Add(1)
		go m.ExportWorker()
	}

	// Wait for completion or cancellation
	m.wg.Wait()

	// Close channels
	close(m.exportTaskChan)

	// Print missing tables if any
	if len(m.missingTables) > 0 {
		fmt.Printf("\n⚠️  Missing tables in page_info:\n")
		for _, table := range m.missingTables {
			fmt.Printf("   - %s\n", table)
		}
	}

	// Print final statistics
	m.statsMutex.RLock()
	totalRows := m.totalExportedRows
	completedTablesCount := len(m.completedTables)
	m.statsMutex.RUnlock()

	fmt.Printf("\n📈 Final Export Statistics:\n")
	fmt.Printf("   Total exported rows: %d\n", totalRows)
	fmt.Printf("   Completed tables: %d\n", completedTablesCount)

	fmt.Printf("✅ EXPORT operation completed\n")
	return nil
}

// printDiagnosticInfo prints diagnostic information about the page_info table
func (m *ExportManager) printDiagnosticInfo(tables []string) {
	fmt.Printf("\n🔍 Diagnostic Information:\n")

	for _, table := range tables {
		// Check page_info table
		var pageCount int64
		var totalPageSize int64
		err := m.sourceDB.QueryRow(`
			SELECT COUNT(*), COALESCE(SUM(page_size), 0)
			FROM sitemerge.page_info 
			WHERE site_database = ? AND site_table = ?`,
			dbConfig.Database, table).Scan(&pageCount, &totalPageSize)

		if err != nil {
			log.Printf("Error getting page_info for table %s: %v", table, err)
			continue
		}

		// Check page_info_progress table
		var processedRows, totalRows int64
		err = m.sourceDB.QueryRow(`
			SELECT COALESCE(processed_rows, 0), COALESCE(total_rows, 0)
			FROM sitemerge.page_info_progress 
			WHERE site_database = ? AND site_table = ?`,
			dbConfig.Database, table).Scan(&processedRows, &totalRows)

		if err != nil {
			log.Printf("Error getting page_info_progress for table %s: %v", table, err)
			continue
		}

		fmt.Printf("   Table %s.%s:\n", dbConfig.Database, table)
		fmt.Printf("     - Page count: %d\n", pageCount)
		fmt.Printf("     - Total page size (sum): %d rows\n", totalPageSize)
		fmt.Printf("     - Page_info_progress processed_rows: %d\n", processedRows)
		fmt.Printf("     - Page_info_progress total_rows: %d\n", totalRows)

		// Check actual table row count
		var actualRows int64
		err = m.sourceDB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, dbConfig.Database, table)).Scan(&actualRows)
		if err != nil {
			log.Printf("Error getting actual row count for table %s: %v", table, err)
		} else {
			fmt.Printf("     - Actual table rows: %d\n", actualRows)
		}
	}
	fmt.Printf("\n")
}

// checkAndMarkTableComplete checks if all pages are processed and marks table as complete
func (m *ExportManager) checkAndMarkTableComplete(database, table string) {
	// Count total pages for this table
	var totalPages int64
	err := m.sourceDB.QueryRow(`
		SELECT COUNT(*) 
		FROM sitemerge.page_info 
		WHERE site_database = ? AND site_table = ?`,
		database, table).Scan(&totalPages)

	if err != nil {
		log.Printf("Error counting total pages for table %s.%s: %v", database, table, err)
		return
	}

	// Count successfully processed pages
	var successPages int64
	err = m.sourceDB.QueryRow(`
		SELECT COUNT(*) 
		FROM sitemerge.page_info pi
		INNER JOIN sitemerge.page_operation_status pos 
			ON pi.site_database = pos.site_database 
			AND pi.site_table = pos.site_table 
			AND pi.id = pos.page_id
		WHERE pi.site_database = ? AND pi.site_table = ? 
		AND pos.export_status = 'success'`,
		database, table).Scan(&successPages)

	if err != nil {
		log.Printf("Error counting success pages for table %s.%s: %v", database, table, err)
		return
	}

	// Count remaining pages that haven't been processed
	var remainingPages int64
	err = m.sourceDB.QueryRow(`
		SELECT COUNT(*) 
		FROM sitemerge.page_info pi
		LEFT JOIN sitemerge.page_operation_status pos 
			ON pi.site_database = pos.site_database 
			AND pi.site_table = pos.site_table 
			AND pi.id = pos.page_id
		WHERE pi.site_database = ? AND pi.site_table = ? 
		AND (pos.export_status IS NULL OR pos.export_status != 'success')`,
		database, table).Scan(&remainingPages)

	if err != nil {
		log.Printf("Error checking remaining pages for table %s.%s: %v", database, table, err)
		return
	}

	log.Printf("🔍 Table %s.%s completion check: Total=%d, Success=%d, Remaining=%d",
		database, table, totalPages, successPages, remainingPages)

	if remainingPages == 0 && totalPages > 0 {
		// All pages processed, mark table as complete
		_, err = m.sourceDB.Exec(`
			UPDATE sitemerge.export_import_summary 
			SET export_status = 'success', export_time = NOW()
			WHERE site_database = ? AND site_table = ?`,
			database, table)
		if err != nil {
			log.Printf("Error marking table as complete: %v", err)
		} else {
			fmt.Printf("🎉 Export completed for entire table %s.%s (%d/%d pages processed)\n",
				database, table, successPages, totalPages)

			// Update completed tables tracking
			tableKey := fmt.Sprintf("%s.%s", database, table)
			m.statsMutex.Lock()
			m.completedTables[tableKey] = true
			m.statsMutex.Unlock()
		}
	} else if totalPages == 0 {
		// No pages found for this table, mark as failed
		_, err = m.sourceDB.Exec(`
			UPDATE sitemerge.export_import_summary 
			SET export_status = 'failed', export_error = 'No page info found for this table'
			WHERE site_database = ? AND site_table = ?`,
			database, table)
		if err != nil {
			log.Printf("Error marking table as failed: %v", err)
		} else {
			log.Printf("❌ Table %s.%s marked as failed: no page info found", database, table)

			// Record missing table
			tableKey := fmt.Sprintf("%s.%s", database, table)
			m.missingTablesMutex.Lock()
			m.missingTables = append(m.missingTables, tableKey)
			m.missingTablesMutex.Unlock()
		}
	}
}

// scheduleExportTasksForTable schedules export tasks for a specific table (legacy method, now unused)
