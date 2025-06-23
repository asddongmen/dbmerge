package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
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
}

// NewExportManager creates a new export manager
func NewExportManager(sourceDB *sql.DB, threads int, tableName string) *ExportManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ExportManager{
		sourceDB:       sourceDB,
		threads:        threads,
		tableName:      tableName,
		ctx:            ctx,
		cancel:         cancel,
		exportTaskChan: make(chan ExportTask, 100),
		missingTables:  make([]string, 0),
	}
}

// CreateExportImportSummaryTable creates the export_import_summary table if it doesn't exist
func (m *ExportManager) CreateExportImportSummaryTable() error {
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
			(site_database, site_table, total_count, export_status, import_status)
			VALUES (?, ?, ?, 'pending', 'pending')
			ON DUPLICATE KEY UPDATE 
			total_count = VALUES(total_count),
			export_status = 'pending'`

		_, err = m.sourceDB.Exec(insertSQL, dbConfig.Database, table, totalCount)
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
func (m *ExportManager) scheduleExportTasksForTable(database, table string) {
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
	// TODO: Implement actual export logic here
	// For now, just simulate the export by marking it as complete
	fmt.Printf("🔄 Exporting %s.%s page %d (keys: %d-%d)\n",
		task.Database, task.Table, task.PageInfo.PageNum, task.PageInfo.StartKey, task.PageInfo.EndKey)

	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	// Check if all pages for this table are completed
	m.checkAndUpdateExportStatus(task.Database, task.Table)
}

// checkAndUpdateExportStatus checks if all export tasks for a table are completed
func (m *ExportManager) checkAndUpdateExportStatus(database, table string) {
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

	// Initialize export_import_summary
	if err := m.InitializeExportSummary(tables); err != nil {
		return err
	}

	// Print operation parameters
	fmt.Printf("🚀 Starting EXPORT operation with parameters:\n")
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
	go m.ExportScheduler()

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

	fmt.Printf("✅ EXPORT operation completed\n")
	return nil
}
