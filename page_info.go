package main

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	pageInfoTableName = "sitemerge.page_info"
	progressTableName = "sitemerge.page_info_progress"
	batchSize         = 10000

	createPageInfoTableSQL = `
CREATE TABLE IF NOT EXISTS sitemerge.page_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    site_database VARCHAR(255),
    site_table VARCHAR(255),
    page_num INT,
    start_key BIGINT,
    end_key BIGINT,
    page_size INT,
    UNIQUE KEY site_table_page_num(site_database, site_table, page_num)
)`

	createProgressTableSQL = `
CREATE TABLE IF NOT EXISTS sitemerge.page_info_progress (
    id INT AUTO_INCREMENT PRIMARY KEY,
    site_database VARCHAR(255),
    site_table VARCHAR(255),
    status ENUM('processing', 'completed', 'failed') DEFAULT 'processing',
    last_end_key BIGINT DEFAULT 0,
    total_rows BIGINT DEFAULT 0,
    processed_rows BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY site_table_progress(site_database, site_table)
)`

	getTablesSQL = `
SELECT site_database, site_table, clustered_columns, table_rows
FROM sitemerge.sitemerge_table_index_info
ORDER BY site_database, site_table`
)

// TableToProcess represents a table that needs page info generation
type TableToProcess struct {
	SiteDatabase     string
	SiteTable        string
	ClusteredColumns string
	TableRows        int64
}

// ProgressStatus represents the progress status of a table
type ProgressStatus struct {
	Status        string
	LastEndKey    int64
	ProcessedRows int64
}

// ResumePoint represents a resume point for interrupted processing
type ResumePoint struct {
	SiteDatabase     string
	SiteTable        string
	ClusteredColumns string
	TableRows        int64
	LastEndKey       int64
	ProcessedRows    int64
}

// PageInfo represents page information
type PageInfo struct {
	PageNum  int
	StartKey int64
	EndKey   int64
	PageSize int
}

// TableTask represents a task for processing a single table
type TableTask struct {
	ResumePoint ResumePoint
	IsResume    bool
}

// TaskResult represents the result of processing a table task
type TaskResult struct {
	TableName   string
	Success     bool
	Error       error
	PagesCount  int
	ProcessTime time.Duration
}

// PageInfoGenerator manages page information generation with multi-threading support
type PageInfoGenerator struct {
	db       *sql.DB
	pageSize int
	threads  int
	// Statistics tracking
	mu              sync.Mutex
	processedTables int
	totalTables     int
	totalPages      int
	failedTables    int
	// New statistics fields
	totalProcessedRows int64
	startTime          time.Time
	currentTables      map[int]string // workerID -> current table name
	stopStatsChan      chan bool
}

// NewPageInfoGenerator creates a new page info generator
func NewPageInfoGenerator(db *sql.DB, pageSize int, threads int) *PageInfoGenerator {
	return &PageInfoGenerator{
		db:            db,
		pageSize:      pageSize,
		threads:       threads,
		currentTables: make(map[int]string),
		stopStatsChan: make(chan bool),
	}
}

// startStatsGoroutine starts a goroutine that prints statistics every 5 seconds
func (p *PageInfoGenerator) startStatsGoroutine() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.printCurrentStats()
			case <-p.stopStatsChan:
				return
			}
		}
	}()
}

// stopStatsGoroutine stops the statistics printing goroutine
func (p *PageInfoGenerator) stopStatsGoroutine() {
	close(p.stopStatsChan)
}

// printCurrentStats prints current processing statistics
func (p *PageInfoGenerator) printCurrentStats() {
	p.mu.Lock()
	defer p.mu.Unlock()

	elapsed := time.Since(p.startTime)
	if elapsed.Seconds() == 0 {
		return
	}

	// Calculate QPS
	qps := float64(p.totalProcessedRows) / elapsed.Seconds()

	// Get current processing tables
	var currentTableNames []string
	for _, tableName := range p.currentTables {
		if tableName != "" {
			currentTableNames = append(currentTableNames, tableName)
		}
	}

	log.Println("📊 " + strings.Repeat("=", 50))
	log.Printf("📊 PROCESSING STATISTICS (Every 5s)")
	log.Printf("📊 Processed tables: %d/%d (%.1f%%)",
		p.processedTables, p.totalTables,
		float64(p.processedTables)/float64(p.totalTables)*100)

	if len(currentTableNames) > 0 {
		log.Printf("📊 Currently processing: %v", currentTableNames)
	} else {
		log.Printf("📊 Currently processing: No active tables")
	}

	log.Printf("📊 Total processed rows: %d", p.totalProcessedRows)
	log.Printf("📊 Processing speed: %.2f rows/sec", qps)
	log.Printf("📊 Elapsed time: %.1f seconds", elapsed.Seconds())
	log.Println("📊 " + strings.Repeat("=", 50))
}

// Run executes the page info generation process with multi-threading support
func (p *PageInfoGenerator) Run() error {
	startTime := time.Now()
	p.startTime = startTime // Initialize start time for statistics

	log.Println("Starting SiteMerge Page Info Generation Process")
	log.Printf("Page size: %d", p.pageSize)
	log.Printf("Batch size: %d", batchSize)
	log.Printf("Worker threads: %d", p.threads)
	log.Println()

	// Start statistics goroutine
	p.startStatsGoroutine()
	defer p.stopStatsGoroutine()

	// Step 1: Create page_info table
	if err := p.createPageInfoTables(); err != nil {
		return fmt.Errorf("failed to create page info tables: %w", err)
	}

	// Step 2: Check for resume point
	resumePoint, err := p.getResumePoint()
	if err != nil {
		return fmt.Errorf("failed to get resume point: %w", err)
	}

	// Step 3: Get all tables to process (sorted)
	tables, err := p.printAndSortTablesByDatabase()
	if err != nil {
		return fmt.Errorf("failed to get tables to process: %w", err)
	}

	if len(tables) == 0 {
		log.Println("No tables found to process")
		return nil
	}

	// Step 4: Prepare tasks
	var tasks []TableTask

	// Add resume task if exists
	if resumePoint != nil {
		log.Printf("Found interrupted processing for table `%s`.`%s`",
			resumePoint.SiteDatabase, resumePoint.SiteTable)
		log.Printf("Resuming from key %d with %d rows already processed",
			resumePoint.LastEndKey, resumePoint.ProcessedRows)
		log.Println()

		tasks = append(tasks, TableTask{
			ResumePoint: *resumePoint,
			IsResume:    true,
		})
	}

	// Filter out already completed tables and add them as tasks
	remainingTables := p.filterRemainingTables(tables)
	if len(remainingTables) == 0 && resumePoint == nil {
		log.Println("All tables have been processed!")
		return nil
	}

	log.Printf("Found %d remaining tables to process", len(remainingTables))
	log.Println()

	// Add remaining tables as tasks
	for _, table := range remainingTables {
		// Skip if already added as resume task
		if resumePoint != nil &&
			table.SiteDatabase == resumePoint.SiteDatabase &&
			table.SiteTable == resumePoint.SiteTable {
			continue
		}

		tasks = append(tasks, TableTask{
			ResumePoint: ResumePoint{
				SiteDatabase:     table.SiteDatabase,
				SiteTable:        table.SiteTable,
				ClusteredColumns: table.ClusteredColumns,
				TableRows:        table.TableRows,
				LastEndKey:       0,
				ProcessedRows:    0,
			},
			IsResume: false,
		})
	}

	if len(tasks) == 0 {
		log.Println("No tasks to process")
		return nil
	}

	p.totalTables = len(tasks)
	log.Printf("Total tasks to process: %d", len(tasks))
	log.Println()

	// Step 5: Process tasks with worker pool
	if err := p.processTasksWithWorkerPool(tasks); err != nil {
		return fmt.Errorf("failed to process tasks: %w", err)
	}

	// Step 6: Print summary
	elapsed := time.Since(startTime)
	log.Println("=" + strings.Repeat("=", 59))
	log.Println("PROCESSING SUMMARY")
	log.Println("=" + strings.Repeat("=", 59))
	log.Printf("Tasks processed: %d/%d", p.processedTables, p.totalTables)
	log.Printf("Failed tasks: %d", p.failedTables)
	log.Printf("Total pages generated: %d", p.totalPages)
	log.Printf("Total time elapsed: %.2f seconds", elapsed.Seconds())
	log.Printf("Average time per table: %.2f seconds", elapsed.Seconds()/float64(p.totalTables))
	log.Println("Processing completed successfully!")

	return nil
}

// processTasksWithWorkerPool processes tasks using a worker pool pattern
func (p *PageInfoGenerator) processTasksWithWorkerPool(tasks []TableTask) error {
	// Create channels
	taskChan := make(chan TableTask, len(tasks))
	resultChan := make(chan TaskResult, len(tasks))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < p.threads; i++ {
		wg.Add(1)
		go p.worker(i+1, taskChan, resultChan, &wg)
	}

	// Send tasks to workers
	go func() {
		defer close(taskChan)
		for _, task := range tasks {
			taskChan <- task
		}
	}()

	// Collect results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results
	for result := range resultChan {
		p.mu.Lock()
		p.processedTables++
		if result.Success {
			p.totalPages += result.PagesCount
			log.Printf("✅ [%d/%d] Table %s completed (%d pages, %.2fs)",
				p.processedTables, p.totalTables, result.TableName,
				result.PagesCount, result.ProcessTime.Seconds())
		} else {
			p.failedTables++
			log.Printf("❌ [%d/%d] Table %s failed: %v (%.2fs)",
				p.processedTables, p.totalTables, result.TableName,
				result.Error, result.ProcessTime.Seconds())
		}
		p.mu.Unlock()
	}

	return nil
}

// worker processes tasks from the task channel
func (p *PageInfoGenerator) worker(workerID int, taskChan <-chan TableTask, resultChan chan<- TaskResult, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("🔄 Worker %d started", workerID)

	for task := range taskChan {
		startTime := time.Now()
		tableName := fmt.Sprintf("%s.%s", task.ResumePoint.SiteDatabase, task.ResumePoint.SiteTable)

		// Update current table being processed
		p.mu.Lock()
		p.currentTables[workerID] = tableName
		p.mu.Unlock()

		log.Printf("🔄 Worker %d processing table %s", workerID, tableName)

		pagesCount, err := p.processSingleTable(task.ResumePoint, task.IsResume)
		processTime := time.Since(startTime)

		// Clear current table when done
		p.mu.Lock()
		p.currentTables[workerID] = ""
		p.mu.Unlock()

		result := TaskResult{
			TableName:   tableName,
			Success:     err == nil,
			Error:       err,
			PagesCount:  pagesCount,
			ProcessTime: processTime,
		}

		resultChan <- result
	}

	log.Printf("🔄 Worker %d finished", workerID)
}

func (p *PageInfoGenerator) createPageInfoTables() error {
	// Create page_info table
	if _, err := p.db.Exec(createPageInfoTableSQL); err != nil {
		return fmt.Errorf("failed to create page info table: %w", err)
	}

	// Create progress table
	if _, err := p.db.Exec(createProgressTableSQL); err != nil {
		return fmt.Errorf("failed to create progress table: %w", err)
	}

	log.Printf("Page info table '%s' created or already exists", pageInfoTableName)
	log.Printf("Progress table '%s' created or already exists", progressTableName)
	return nil
}

func (p *PageInfoGenerator) getTablesToProcess() ([]TableToProcess, error) {
	rows, err := p.db.Query(getTablesSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []TableToProcess
	for rows.Next() {
		var table TableToProcess
		var tableRows sql.NullInt64
		err := rows.Scan(&table.SiteDatabase, &table.SiteTable,
			&table.ClusteredColumns, &tableRows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table row: %w", err)
		}

		if tableRows.Valid {
			table.TableRows = tableRows.Int64
		}
		tables = append(tables, table)
	}

	log.Printf("Found %d tables to process", len(tables))
	return tables, nil
}

func (p *PageInfoGenerator) getResumePoint() (*ResumePoint, error) {
	query := `
SELECT p.site_database, p.site_table, s.clustered_columns, 
       s.table_rows, p.last_end_key, p.processed_rows
FROM sitemerge.page_info_progress p
JOIN sitemerge.sitemerge_table_index_info s ON p.site_database = s.site_database AND p.site_table = s.site_table
WHERE p.status = 'processing'
ORDER BY p.updated_at ASC
LIMIT 1`

	var resumePoint ResumePoint
	var tableRows sql.NullInt64
	err := p.db.QueryRow(query).Scan(
		&resumePoint.SiteDatabase,
		&resumePoint.SiteTable,
		&resumePoint.ClusteredColumns,
		&tableRows,
		&resumePoint.LastEndKey,
		&resumePoint.ProcessedRows,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get resume point: %w", err)
	}

	if tableRows.Valid {
		resumePoint.TableRows = tableRows.Int64
	}

	return &resumePoint, nil
}

func (p *PageInfoGenerator) getProgressStatus(dbName, tableName string) (*ProgressStatus, error) {
	query := `
SELECT status, last_end_key, processed_rows 
FROM sitemerge.page_info_progress 
WHERE site_database = ? AND site_table = ?`

	var status ProgressStatus
	err := p.db.QueryRow(query, dbName, tableName).Scan(
		&status.Status, &status.LastEndKey, &status.ProcessedRows)

	if err == sql.ErrNoRows {
		return &ProgressStatus{Status: "new", LastEndKey: 0, ProcessedRows: 0}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get progress status: %w", err)
	}

	return &status, nil
}

func (p *PageInfoGenerator) updateProgress(dbName, tableName, status string,
	lastEndKey, totalRows, processedRows int64) error {

	upsertSQL := `
INSERT INTO sitemerge.page_info_progress 
(site_database, site_table, status, last_end_key, total_rows, processed_rows)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE 
status = VALUES(status),
last_end_key = VALUES(last_end_key),
total_rows = VALUES(total_rows),
processed_rows = VALUES(processed_rows),
updated_at = CURRENT_TIMESTAMP`

	_, err := p.db.Exec(upsertSQL, dbName, tableName, status, lastEndKey, totalRows, processedRows)
	return err
}

func (p *PageInfoGenerator) filterRemainingTables(tables []TableToProcess) []TableToProcess {
	var remaining []TableToProcess
	for _, table := range tables {
		status, err := p.getProgressStatus(table.SiteDatabase, table.SiteTable)
		if err != nil {
			log.Printf("Error checking status for %s.%s: %v",
				table.SiteDatabase, table.SiteTable, err)
			continue
		}

		if status.Status != "completed" {
			remaining = append(remaining, table)
		}
	}
	return remaining
}

func (p *PageInfoGenerator) clearExistingPageInfo(dbName, tableName string) error {
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE site_database = ? AND site_table = ?", pageInfoTableName)
	result, err := p.db.Exec(deleteSQL, dbName, tableName)
	if err != nil {
		return fmt.Errorf("failed to clear existing page info: %w", err)
	}

	deletedRows, _ := result.RowsAffected()
	if deletedRows > 0 {
		log.Printf("  Cleared %d existing page records", deletedRows)
	}
	return nil
}

func (p *PageInfoGenerator) processSingleTable(resumePoint ResumePoint, isResume bool) (int, error) {
	dbName := resumePoint.SiteDatabase
	tableName := resumePoint.SiteTable
	divideColumn := resumePoint.ClusteredColumns
	tableRows := resumePoint.TableRows
	resumeFromKey := resumePoint.LastEndKey
	existingProcessedRows := resumePoint.ProcessedRows

	if isResume {
		log.Printf("  Resuming table `%s`.`%s` from key %d (already processed %d rows)",
			dbName, tableName, resumeFromKey, existingProcessedRows)
	} else {
		log.Printf("  Processing table `%s`.`%s` (%d rows)", dbName, tableName, tableRows)
		// Clear existing page info for this table if starting fresh
		if err := p.clearExistingPageInfo(dbName, tableName); err != nil {
			return 0, err
		}
	}

	// Update progress status to 'processing'
	if err := p.updateProgress(dbName, tableName, "processing", resumeFromKey, tableRows, existingProcessedRows); err != nil {
		return 0, fmt.Errorf("failed to update progress: %w", err)
	}

	var lastMaxID *int64
	if resumeFromKey > 0 {
		lastMaxID = &resumeFromKey
	}

	batchCount := 0
	var allValues []int64 // Store all values across batches
	processedRows := existingProcessedRows

	// First, collect all data in batches
	for {
		// Generate raw data for current batch
		batchValues, maxEndKey, err := p.generatePageInfoBatch(dbName, tableName, divideColumn, lastMaxID)
		if err != nil {
			return 0, fmt.Errorf("failed to generate page info batch: %w", err)
		}

		if len(batchValues) == 0 {
			break
		}

		allValues = append(allValues, batchValues...)
		batchCount++
		processedRows += int64(len(batchValues))

		// Update global statistics
		p.mu.Lock()
		p.totalProcessedRows += int64(len(batchValues))
		p.mu.Unlock()

		// Update progress periodically
		if err := p.updateProgress(dbName, tableName, "processing", maxEndKey, tableRows, processedRows); err != nil {
			return 0, fmt.Errorf("failed to update progress: %w", err)
		}

		// Check if we've processed all data
		if len(batchValues) < batchSize {
			break
		}

		// Update lastMaxID for next iteration
		lastMaxID = &maxEndKey
	}

	// Now process all values to generate pages
	totalPages := 0
	if len(allValues) > 0 {
		// Calculate starting page number based on existing processed rows
		startPageNum := int(existingProcessedRows/int64(p.pageSize)) + 1

		var pageData []PageInfo
		for i := 0; i < len(allValues); i += p.pageSize {
			end := i + p.pageSize
			if end > len(allValues) {
				end = len(allValues)
			}

			pageChunk := allValues[i:end]
			pageNum := startPageNum + (i / p.pageSize)
			startKey := pageChunk[0]
			endKey := pageChunk[len(pageChunk)-1]
			actualPageSize := len(pageChunk)

			pageData = append(pageData, PageInfo{
				PageNum:  pageNum,
				StartKey: startKey,
				EndKey:   endKey,
				PageSize: actualPageSize,
			})
		}

		// Insert all page info at once
		if err := p.insertPageInfoBatch(dbName, tableName, pageData); err != nil {
			return 0, fmt.Errorf("failed to insert page info batch: %w", err)
		}
		totalPages = len(pageData)

		log.Printf("    Generated %d pages from %d rows", totalPages, len(allValues))
	}

	// Mark as completed
	if err := p.updateProgress(dbName, tableName, "completed", 0, tableRows, processedRows); err != nil {
		return 0, fmt.Errorf("failed to mark as completed: %w", err)
	}

	return totalPages, nil
}

func (p *PageInfoGenerator) generatePageInfoBatch(dbName, tableName, divideColumn string,
	lastMaxID *int64) ([]int64, int64, error) {

	var sqlTemplate string
	var args []interface{}

	if lastMaxID == nil {
		// First batch - no WHERE condition
		sqlTemplate = fmt.Sprintf(`
SELECT /*+ READ_FROM_STORAGE(TIKV) */ %s
FROM %s.%s
ORDER BY %s
LIMIT %d`, divideColumn, dbName, tableName, divideColumn, batchSize)
	} else {
		// Subsequent batches - with WHERE condition
		sqlTemplate = fmt.Sprintf(`
SELECT /*+ READ_FROM_STORAGE(TIKV) */ %s
FROM %s.%s
WHERE %s > ?
ORDER BY %s
LIMIT %d`, divideColumn, dbName, tableName, divideColumn, divideColumn, batchSize)
		args = append(args, *lastMaxID)
	}

	rows, err := p.db.Query(sqlTemplate, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute batch query: %w", err)
	}
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return nil, 0, fmt.Errorf("failed to scan value: %w", err)
		}
		values = append(values, value)
	}

	var maxEndKey int64
	if len(values) > 0 {
		maxEndKey = values[len(values)-1]
	}

	return values, maxEndKey, nil
}

func (p *PageInfoGenerator) insertPageInfoBatch(dbName, tableName string, pageData []PageInfo) error {
	if len(pageData) == 0 {
		return nil
	}

	insertSQL := fmt.Sprintf(`
INSERT INTO %s 
(site_database, site_table, page_num, start_key, end_key, page_size)
VALUES (?, ?, ?, ?, ?, ?)`, pageInfoTableName)

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, page := range pageData {
		_, err := stmt.Exec(dbName, tableName, page.PageNum, page.StartKey, page.EndKey, page.PageSize)
		if err != nil {
			return fmt.Errorf("failed to insert page info: %w", err)
		}
	}

	return tx.Commit()
}

// DatabaseTableStats represents statistics for tables in a database
type DatabaseTableStats struct {
	DatabaseName string
	Tables       []TableToProcess
	TotalCount   int
	TotalRows    int64
}

// printAndSortTablesByDatabase fetches all tables, groups them by database,
// prints statistics for each database, and returns all tables sorted by database and table name in lexicographical order
func (p *PageInfoGenerator) printAndSortTablesByDatabase() ([]TableToProcess, error) {
	// Get all tables first
	allTables, err := p.getTablesToProcess()
	if err != nil {
		return nil, fmt.Errorf("failed to get tables to process: %w", err)
	}

	if len(allTables) == 0 {
		log.Println("No tables found to process")
		return allTables, nil
	}

	// Group tables by database
	dbTableMap := make(map[string][]TableToProcess)
	for _, table := range allTables {
		dbTableMap[table.SiteDatabase] = append(dbTableMap[table.SiteDatabase], table)
	}

	// Get sorted database names
	var databaseNames []string
	for dbName := range dbTableMap {
		databaseNames = append(databaseNames, dbName)
	}
	sort.Strings(databaseNames)

	// Print statistics for each database and sort tables within each database
	var sortedTables []TableToProcess
	var totalDatabases int
	var totalTables int
	var totalRows int64

	log.Println("=" + strings.Repeat("=", 59))
	log.Println("DATABASE AND TABLE STATISTICS")
	log.Println("=" + strings.Repeat("=", 59))

	for _, dbName := range databaseNames {
		tables := dbTableMap[dbName]

		// Sort tables within database by table name (lexicographical order)
		sort.Slice(tables, func(i, j int) bool {
			return tables[i].SiteTable < tables[j].SiteTable
		})

		// Calculate database statistics
		var dbTotalRows int64
		for _, table := range tables {
			dbTotalRows += table.TableRows
		}

		// Print database statistics
		log.Printf("Database: %s", dbName)
		log.Printf("  Table count: %d", len(tables))
		log.Printf("  Total rows: %d", dbTotalRows)
		log.Printf("  Tables:")

		for i, table := range tables {
			log.Printf("    %d. %s (%d rows)", i+1, table.SiteTable, table.TableRows)
		}
		log.Println()

		// Add to final sorted list
		sortedTables = append(sortedTables, tables...)
		totalDatabases++
		totalTables += len(tables)
		totalRows += dbTotalRows
	}

	// Print overall summary
	log.Printf("OVERALL SUMMARY:")
	log.Printf("  Total databases: %d", totalDatabases)
	log.Printf("  Total tables: %d", totalTables)
	log.Printf("  Total rows: %d", totalRows)
	log.Println("=" + strings.Repeat("=", 59))
	log.Println()

	return sortedTables, nil
}
