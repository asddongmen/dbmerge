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

// ExportImportSummary represents the export/import status table
type ExportImportSummary struct {
	TaskID               int64      `db:"task_id"`
	SiteDatabase         string     `db:"site_database"`
	SiteTable            string     `db:"site_table"`
	TotalCount           int64      `db:"total_count"`
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

// ImportTask represents a single import task
type ImportTask struct {
	Database string
	Table    string
	PageInfo PageInfo
}

// ExportImportManager manages the export/import operations
type ExportImportManager struct {
	sourceDB           *sql.DB
	destDB             *sql.DB
	threads            int
	tableName          string
	action             string
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	exportTaskChan     chan ExportTask
	importTaskChan     chan ImportTask
	missingTables      []string
	missingTablesMutex sync.Mutex
}

// NewExportImportManager creates a new export/import manager
func NewExportImportManager(sourceDB, destDB *sql.DB, threads int, tableName, action string) *ExportImportManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ExportImportManager{
		sourceDB:       sourceDB,
		destDB:         destDB,
		threads:        threads,
		tableName:      tableName,
		action:         action,
		ctx:            ctx,
		cancel:         cancel,
		exportTaskChan: make(chan ExportTask, 100),
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
	}
}

// CreateExportImportSummaryTable creates the export_import_summary table if it doesn't exist
func (m *ExportImportManager) CreateExportImportSummaryTable() error {
	createTableSQL := `
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
			UNIQUE KEY db_table (site_database, site_table)
		);`

	_, err := m.sourceDB.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create export_import_summary table: %w", err)
	}

	fmt.Println("✅ Export/Import summary table created or already exists")
	return nil
}

// GetTablesToProcess returns the list of tables to process
func (m *ExportImportManager) GetTablesToProcess() ([]string, error) {
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

// InitializeExportImportSummary initializes the export_import_summary table with pending records
func (m *ExportImportManager) InitializeExportImportSummary(tables []string) error {
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
			(site_database, site_table, total_count, export_status, import_status)
			VALUES (?, ?, ?, 'pending', 'pending')
			ON DUPLICATE KEY UPDATE 
			total_count = VALUES(total_count)`

		_, err = m.sourceDB.Exec(insertSQL, dbConfig.Database, table, totalCount)
		if err != nil {
			return fmt.Errorf("failed to initialize export_import_summary for table %s: %w", table, err)
		}
	}

	fmt.Printf("✅ Initialized export_import_summary records for %d tables\n", len(tables))
	return nil
}

// ExportScheduler periodically scans for pending export tasks
func (m *ExportImportManager) ExportScheduler() {
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
func (m *ExportImportManager) scheduleExportTasks() {
	if m.action != "export" {
		return
	}

	// Get pending export tasks
	query := `
		SELECT site_database, site_table 
		FROM sitemerge.export_import_summary 
		WHERE export_status = 'pending'`

	rows, err := m.sourceDB.Query(query)
	if err != nil {
		log.Printf("Error getting pending export tasks: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var database, table string
		if err := rows.Scan(&database, &table); err != nil {
			log.Printf("Error scanning export task: %v", err)
			continue
		}

		// Update status to running
		_, err = m.sourceDB.Exec(`
			UPDATE sitemerge.export_import_summary 
			SET export_status = 'running' 
			WHERE site_database = ? AND site_table = ? AND export_status = 'pending'`,
			database, table)
		if err != nil {
			log.Printf("Error updating export status to running: %v", err)
			continue
		}

		// Get page info for this table
		m.scheduleExportTasksForTable(database, table)
	}
}

// scheduleExportTasksForTable schedules export tasks for a specific table
func (m *ExportImportManager) scheduleExportTasksForTable(database, table string) {
	query := `
		SELECT id, site_database, site_table, page_num, start_key, end_key, page_size
		FROM sitemerge.page_info 
		WHERE site_database = ? AND site_table = ?`

	rows, err := m.sourceDB.Query(query, database, table)
	if err != nil {
		log.Printf("Error getting page info for table %s.%s: %v", database, table, err)
		return
	}
	defer rows.Close()

	hasPages := false
	for rows.Next() {
		hasPages = true
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

		select {
		case m.exportTaskChan <- task:
		case <-m.ctx.Done():
			return
		}
	}

	if !hasPages {
		// Record missing table
		m.missingTablesMutex.Lock()
		m.missingTables = append(m.missingTables, fmt.Sprintf("%s.%s", database, table))
		m.missingTablesMutex.Unlock()

		// Mark as failed
		_, err = m.sourceDB.Exec(`
			UPDATE sitemerge.export_import_summary 
			SET export_status = 'failed', export_error = 'No page info found for this table'
			WHERE site_database = ? AND site_table = ?`,
			database, table)
		if err != nil {
			log.Printf("Error updating export status to failed: %v", err)
		}
	}
}

// ImportScheduler periodically scans for import tasks
func (m *ExportImportManager) ImportScheduler() {
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
func (m *ExportImportManager) scheduleImportTasks() {
	if m.action != "import" {
		return
	}

	// Get tables ready for import
	query := `
		SELECT site_database, site_table 
		FROM sitemerge.export_import_summary 
		WHERE export_status = 'success' AND import_status = 'pending'`

	rows, err := m.sourceDB.Query(query)
	if err != nil {
		log.Printf("Error getting pending import tasks: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var database, table string
		if err := rows.Scan(&database, &table); err != nil {
			log.Printf("Error scanning import task: %v", err)
			continue
		}

		// Update status to running
		_, err = m.sourceDB.Exec(`
			UPDATE sitemerge.export_import_summary 
			SET import_status = 'running' 
			WHERE site_database = ? AND site_table = ? AND import_status = 'pending'`,
			database, table)
		if err != nil {
			log.Printf("Error updating import status to running: %v", err)
			continue
		}

		// Get page info for this table
		m.scheduleImportTasksForTable(database, table)
	}
}

// scheduleImportTasksForTable schedules import tasks for a specific table
func (m *ExportImportManager) scheduleImportTasksForTable(database, table string) {
	query := `
		SELECT id, site_database, site_table, page_num, start_key, end_key, page_size
		FROM sitemerge.page_info 
		WHERE site_database = ? AND site_table = ?`

	rows, err := m.sourceDB.Query(query, database, table)
	if err != nil {
		log.Printf("Error getting page info for table %s.%s: %v", database, table, err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var pageInfo PageInfo
		if err := rows.Scan(&pageInfo.ID, &pageInfo.SiteDatabase, &pageInfo.SiteTable,
			&pageInfo.PageNum, &pageInfo.StartKey, &pageInfo.EndKey, &pageInfo.PageSize); err != nil {
			log.Printf("Error scanning page info: %v", err)
			continue
		}

		task := ImportTask{
			Database: database,
			Table:    table,
			PageInfo: pageInfo,
		}

		select {
		case m.importTaskChan <- task:
		case <-m.ctx.Done():
			return
		}
	}
}

// ExportWorker processes export tasks
func (m *ExportImportManager) ExportWorker() {
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
func (m *ExportImportManager) processExportTask(task ExportTask) {
	// TODO: Implement actual export logic here
	// For now, just simulate the export by marking it as complete
	fmt.Printf("🔄 Exporting %s.%s page %d (keys: %d-%d)\n",
		task.Database, task.Table, task.PageInfo.PageNum, task.PageInfo.StartKey, task.PageInfo.EndKey)

	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	// Check if all pages for this table are completed
	m.checkAndUpdateExportStatus(task.Database, task.Table)
}

// ImportWorker processes import tasks
func (m *ExportImportManager) ImportWorker() {
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
func (m *ExportImportManager) processImportTask(task ImportTask) {
	// TODO: Implement actual import logic here
	// For now, just simulate the import by marking it as complete
	fmt.Printf("🔄 Importing %s.%s page %d (keys: %d-%d)\n",
		task.Database, task.Table, task.PageInfo.PageNum, task.PageInfo.StartKey, task.PageInfo.EndKey)

	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	// Check if all pages for this table are completed
	m.checkAndUpdateImportStatus(task.Database, task.Table)
}

// checkAndUpdateExportStatus checks if all export tasks for a table are completed
func (m *ExportImportManager) checkAndUpdateExportStatus(database, table string) {
	// Get total pages for this table
	var totalPages int64
	err := m.sourceDB.QueryRow(`
		SELECT COUNT(*) FROM sitemerge.page_info 
		WHERE site_database = ? AND site_table = ?`,
		database, table).Scan(&totalPages)
	if err != nil {
		log.Printf("Error getting total pages for table %s.%s: %v", database, table, err)
		return
	}

	// This is a simplified check - in a real implementation, you would need to track
	// completion of individual pages. For now, we'll mark as success immediately.
	now := time.Now()
	_, err = m.sourceDB.Exec(`
		UPDATE sitemerge.export_import_summary 
		SET export_status = 'success', export_time = ?, export_count = total_count
		WHERE site_database = ? AND site_table = ? AND export_status = 'running'`,
		now, database, table)
	if err != nil {
		log.Printf("Error updating export status to success: %v", err)
	} else {
		fmt.Printf("✅ Export completed for table %s.%s\n", database, table)
	}
}

// checkAndUpdateImportStatus checks if all import tasks for a table are completed
func (m *ExportImportManager) checkAndUpdateImportStatus(database, table string) {
	// Get total pages for this table
	var totalPages int64
	err := m.sourceDB.QueryRow(`
		SELECT COUNT(*) FROM sitemerge.page_info 
		WHERE site_database = ? AND site_table = ?`,
		database, table).Scan(&totalPages)
	if err != nil {
		log.Printf("Error getting total pages for table %s.%s: %v", database, table, err)
		return
	}

	// This is a simplified check - in a real implementation, you would need to track
	// completion of individual pages. For now, we'll mark as success immediately.
	now := time.Now()
	_, err = m.sourceDB.Exec(`
		UPDATE sitemerge.export_import_summary 
		SET import_status = 'success', import_time = ?
		WHERE site_database = ? AND site_table = ? AND import_status = 'running'`,
		now, database, table)
	if err != nil {
		log.Printf("Error updating import status to success: %v", err)
	} else {
		fmt.Printf("✅ Import completed for table %s.%s\n", database, table)
	}
}

// Run starts the export/import process
func (m *ExportImportManager) Run() error {
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

	// Initialize export_import_summary
	if err := m.InitializeExportImportSummary(tables); err != nil {
		return err
	}

	// Print operation parameters
	fmt.Printf("🚀 Starting %s operation with parameters:\n", strings.ToUpper(m.action))
	fmt.Printf("   Source DB: %s:%d/%s\n", dbConfig.Host, dbConfig.Port, dbConfig.Database)
	fmt.Printf("   Tables: %v\n", tables)
	fmt.Printf("   Threads: %d\n", m.threads)
	fmt.Printf("   Action: %s\n", m.action)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n🛑 Received shutdown signal, gracefully shutting down...")
		m.cancel()
	}()

	// Start schedulers
	if m.action == "export" {
		m.wg.Add(1)
		go m.ExportScheduler()
	}

	if m.action == "import" {
		m.wg.Add(1)
		go m.ImportScheduler()
	}

	// Start worker threads
	for i := 0; i < m.threads; i++ {
		if m.action == "export" {
			m.wg.Add(1)
			go m.ExportWorker()
		}

		if m.action == "import" {
			m.wg.Add(1)
			go m.ImportWorker()
		}
	}

	// Wait for completion or cancellation
	m.wg.Wait()

	// Close channels
	close(m.exportTaskChan)
	close(m.importTaskChan)

	// Print missing tables if any
	if len(m.missingTables) > 0 {
		fmt.Printf("\n⚠️  Missing tables in page_info:\n")
		for _, table := range m.missingTables {
			fmt.Printf("   - %s\n", table)
		}
	}

	fmt.Printf("✅ %s operation completed\n", strings.ToUpper(m.action))
	return nil
}
