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

// ImportTask represents a single import task
type ImportTask struct {
	Database string
	Table    string
	PageInfo PageInfo
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
	}
}

// CreateExportImportSummaryTable creates the export_import_summary table if it doesn't exist
func (m *ImportManager) CreateExportImportSummaryTable() error {
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
	}

	fmt.Printf("✅ Initialized import status for %d tables\n", len(tables))
	return nil
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
func (m *ImportManager) scheduleImportTasksForTable(database, table string) {
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

	if !hasPages {
		// Record missing table
		m.missingTablesMutex.Lock()
		m.missingTables = append(m.missingTables, fmt.Sprintf("%s.%s", database, table))
		m.missingTablesMutex.Unlock()

		// Mark as failed
		_, err = m.sourceDB.Exec(`
			UPDATE sitemerge.export_import_summary 
			SET import_status = 'failed', import_error = 'No page info found for this table'
			WHERE site_database = ? AND site_table = ?`,
			database, table)
		if err != nil {
			log.Printf("Error updating import status to failed: %v", err)
		}
	}
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
	// TODO: Implement actual import logic here
	// For now, just simulate the import by marking it as complete
	fmt.Printf("🔄 Importing %s.%s page %d (keys: %d-%d)\n",
		task.Database, task.Table, task.PageInfo.PageNum, task.PageInfo.StartKey, task.PageInfo.EndKey)

	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	// Check if all pages for this table are completed
	m.checkAndUpdateImportStatus(task.Database, task.Table)
}

// checkAndUpdateImportStatus checks if all import tasks for a table are completed
func (m *ImportManager) checkAndUpdateImportStatus(database, table string) {
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
