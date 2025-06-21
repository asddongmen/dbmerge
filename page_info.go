package main

import (
	"database/sql"
	"fmt"
	"strings"
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

// PageInfoGenerator manages page information generation
type PageInfoGenerator struct {
	db       *sql.DB
	pageSize int
}

// NewPageInfoGenerator creates a new page info generator
func NewPageInfoGenerator(db *sql.DB, pageSize int) *PageInfoGenerator {
	return &PageInfoGenerator{
		db:       db,
		pageSize: pageSize,
	}
}

// Run executes the page info generation process with resume support
func (p *PageInfoGenerator) Run() error {
	startTime := time.Now()
	fmt.Println("🚀 Starting SiteMerge Page Info Generation Process")
	fmt.Printf("📏 Page size: %d\n", p.pageSize)
	fmt.Printf("📦 Batch size: %d\n", batchSize)
	fmt.Println()

	// Step 1: Create page_info table
	if err := p.createPageInfoTables(); err != nil {
		return fmt.Errorf("failed to create page info tables: %w", err)
	}

	// Step 2: Check for resume point
	resumePoint, err := p.getResumePoint()
	if err != nil {
		return fmt.Errorf("failed to get resume point: %w", err)
	}

	if resumePoint != nil {
		fmt.Printf("🔄 Found interrupted processing for table `%s`.`%s`\n",
			resumePoint.SiteDatabase, resumePoint.SiteTable)
		fmt.Printf("📍 Resuming from key %d with %d rows already processed\n",
			resumePoint.LastEndKey, resumePoint.ProcessedRows)
		fmt.Println()

		// Process the interrupted table
		if err := p.processSingleTable(*resumePoint, true); err != nil {
			fmt.Printf("❌ Failed to resume table `%s`.`%s`: %v\n",
				resumePoint.SiteDatabase, resumePoint.SiteTable, err)
			p.updateProgress(resumePoint.SiteDatabase, resumePoint.SiteTable, "failed", 0, 0, 0)
		} else {
			fmt.Println("✅ Resumed table completed")
		}
		fmt.Println()
	}

	// Step 3: Get remaining tables to process
	tables, err := p.getTablesToProcess()
	if err != nil {
		return fmt.Errorf("failed to get tables to process: %w", err)
	}

	if len(tables) == 0 {
		fmt.Println("⚠️  No tables found to process")
		return nil
	}

	// Filter out already completed tables
	remainingTables := p.filterRemainingTables(tables)
	if len(remainingTables) == 0 {
		fmt.Println("✅ All tables have been processed!")
		return nil
	}

	fmt.Printf("📋 Found %d remaining tables to process\n", len(remainingTables))
	fmt.Println()

	// Step 4: Process remaining tables
	totalPagesGenerated := 0
	processedTables := 0

	for _, table := range remainingTables {
		resumePoint := ResumePoint{
			SiteDatabase:     table.SiteDatabase,
			SiteTable:        table.SiteTable,
			ClusteredColumns: table.ClusteredColumns,
			TableRows:        table.TableRows,
			LastEndKey:       0,
			ProcessedRows:    0,
		}

		if err := p.processSingleTable(resumePoint, false); err != nil {
			fmt.Printf("❌ Failed to process table `%s`.`%s`: %v\n",
				table.SiteDatabase, table.SiteTable, err)
			p.updateProgress(table.SiteDatabase, table.SiteTable, "failed", 0, 0, 0)
			continue
		}

		processedTables++
		fmt.Println("✅ Table completed")
		fmt.Println()
	}

	// Step 5: Print summary
	elapsed := time.Since(startTime)
	fmt.Println("=" + strings.Repeat("=", 59))
	fmt.Println("📊 PROCESSING SUMMARY")
	fmt.Println("=" + strings.Repeat("=", 59))
	fmt.Printf("✅ Tables processed: %d/%d\n", processedTables, len(remainingTables))
	fmt.Printf("📄 Total pages generated: %d\n", totalPagesGenerated)
	fmt.Printf("⏱️  Total time elapsed: %.2f seconds\n", elapsed.Seconds())
	fmt.Println("🏁 Processing completed successfully!")

	return nil
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

	fmt.Printf("📋 Page info table '%s' created or already exists\n", pageInfoTableName)
	fmt.Printf("📋 Progress table '%s' created or already exists\n", progressTableName)
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

	fmt.Printf("📊 Found %d tables to process\n", len(tables))
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
			fmt.Printf("⚠️  Error checking status for %s.%s: %v\n",
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
		fmt.Printf("  🧹 Cleared %d existing page records\n", deletedRows)
	}
	return nil
}

func (p *PageInfoGenerator) processSingleTable(resumePoint ResumePoint, isResume bool) error {
	dbName := resumePoint.SiteDatabase
	tableName := resumePoint.SiteTable
	divideColumn := resumePoint.ClusteredColumns
	tableRows := resumePoint.TableRows
	resumeFromKey := resumePoint.LastEndKey
	existingProcessedRows := resumePoint.ProcessedRows

	if isResume {
		fmt.Printf("  🔄 Resuming table `%s`.`%s` from key %d (already processed %d rows)\n",
			dbName, tableName, resumeFromKey, existingProcessedRows)
	} else {
		fmt.Printf("  📊 Processing table `%s`.`%s` (%d rows)\n", dbName, tableName, tableRows)
		// Clear existing page info for this table if starting fresh
		if err := p.clearExistingPageInfo(dbName, tableName); err != nil {
			return err
		}
	}

	// Update progress status to 'processing'
	if err := p.updateProgress(dbName, tableName, "processing", resumeFromKey, tableRows, existingProcessedRows); err != nil {
		return fmt.Errorf("failed to update progress: %w", err)
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
			return fmt.Errorf("failed to generate page info batch: %w", err)
		}

		if len(batchValues) == 0 {
			break
		}

		allValues = append(allValues, batchValues...)
		batchCount++
		processedRows += int64(len(batchValues))

		fmt.Printf("    📥 Fetched batch %d: %d rows (total fetched: %d, total processed: %d)\n",
			batchCount, len(batchValues), len(allValues), processedRows)

		// Update progress periodically
		if err := p.updateProgress(dbName, tableName, "processing", maxEndKey, tableRows, processedRows); err != nil {
			return fmt.Errorf("failed to update progress: %w", err)
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
			return fmt.Errorf("failed to insert page info batch: %w", err)
		}
		totalPages = len(pageData)

		fmt.Printf("    📄 Generated %d pages from %d rows\n", totalPages, len(allValues))
	}

	// Mark as completed
	if err := p.updateProgress(dbName, tableName, "completed", 0, tableRows, processedRows); err != nil {
		return fmt.Errorf("failed to mark as completed: %w", err)
	}

	return nil
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
