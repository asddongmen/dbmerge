package main

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	sitemergeTableName   = "sitemerge.sitemerge_table_index_info"
	prepareSQLStatements = `
CREATE DATABASE IF NOT EXISTS sitemerge 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;`

	createTableSQL = `
CREATE TABLE IF NOT EXISTS sitemerge.sitemerge_table_index_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    site_database VARCHAR(255) NOT NULL,
    site_table VARCHAR(255) NOT NULL,
    clustered_index VARCHAR(10) NOT NULL,
    clustered_columns TEXT,
    com_clusted_index VARCHAR(10) NOT NULL,
    table_rows BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_database_table (site_database, site_table)
)`

	tableIndexQuery = `
select site_database, site_table, clustered_index,clustered_columns,
IF(LOCATE(',', clustered_columns) > 0, 'Yes', 'No') AS com_clusted_index, table_rows 
from 
(SELECT
    t.TABLE_SCHEMA AS site_database,
    t.TABLE_NAME AS site_table,
    -- 聚簇索引状态
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM information_schema.tidb_indexes idx
            WHERE
                idx.TABLE_SCHEMA = t.TABLE_SCHEMA
                AND idx.TABLE_NAME = t.TABLE_NAME
                AND idx.KEY_NAME = 'PRIMARY'
                AND idx.CLUSTERED = 'YES'
        ) THEN 'Yes'
        ELSE 'No'
    END AS clustered_index,
    -- 聚簇索引列名
    IFNULL(
        (SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ')
         FROM information_schema.tidb_indexes
         WHERE TABLE_SCHEMA = t.TABLE_SCHEMA
           AND TABLE_NAME = t.TABLE_NAME
           AND KEY_NAME = 'PRIMARY'
           AND CLUSTERED = 'YES'),
        'N/A'
    ) AS clustered_columns,

    t.table_rows 
FROM
    INFORMATION_SCHEMA.TABLES t
WHERE
    t.TABLE_SCHEMA = ?
ORDER BY
    t.TABLE_SCHEMA, t.TABLE_NAME) it;`
)

// TableInfo represents table index information
type TableInfo struct {
	SiteDatabase     string
	SiteTable        string
	ClusteredIndex   string
	ClusteredColumns sql.NullString
	ComClusteredIdx  string
	TableRows        sql.NullInt64
}

// TableIndexManager manages table index information collection
type TableIndexManager struct {
	db *sql.DB
}

// NewTableIndexManager creates a new table index manager
func NewTableIndexManager(db *sql.DB) *TableIndexManager {
	return &TableIndexManager{db: db}
}

// Run executes the table index information collection process
func (t *TableIndexManager) Run(targetDatabase string, clearExisting bool) error {
	fmt.Printf("🚀 Starting SiteMerge table index process for database: %s\n", targetDatabase)
	fmt.Printf("📋 Target table: %s\n", sitemergeTableName)
	fmt.Println()

	// Step 1: Initialize sitemerge database
	if err := t.executePrepareSQLs(); err != nil {
		return fmt.Errorf("failed to prepare database: %w", err)
	}

	// Step 2: Create target table
	if err := t.createTargetTable(); err != nil {
		return fmt.Errorf("failed to create target table: %w", err)
	}

	// Step 3: Clear existing data (optional)
	if clearExisting {
		if err := t.clearExistingData(targetDatabase); err != nil {
			return fmt.Errorf("failed to clear existing data: %w", err)
		}
	}

	// Step 4: Execute query and collect results
	results, err := t.executeIndexQuery(targetDatabase)
	if err != nil {
		return fmt.Errorf("failed to execute index query: %w", err)
	}

	// Step 5: Insert results
	if err := t.insertResults(results); err != nil {
		return fmt.Errorf("failed to insert results: %w", err)
	}

	fmt.Println("🏁 SiteMerge table index process completed successfully!")
	return nil
}

func (t *TableIndexManager) executePrepareSQLs() error {
	fmt.Println("🔧 Initializing sitemerge database...")

	statements := strings.Split(prepareSQLStatements, ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		if _, err := t.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute prepare statement: %w", err)
		}
	}

	fmt.Println("✅ Database preparation completed successfully")
	return nil
}

func (t *TableIndexManager) createTargetTable() error {
	if _, err := t.db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	fmt.Printf("📋 Table '%s' created or already exists\n", sitemergeTableName)
	return nil
}

func (t *TableIndexManager) clearExistingData(targetDatabase string) error {
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE site_database = ?", sitemergeTableName)
	result, err := t.db.Exec(deleteSQL, targetDatabase)
	if err != nil {
		return fmt.Errorf("failed to clear existing data: %w", err)
	}

	deletedRows, _ := result.RowsAffected()
	fmt.Printf("🧹 Cleared %d existing records for database '%s'\n", deletedRows, targetDatabase)
	return nil
}

func (t *TableIndexManager) executeIndexQuery(targetDatabase string) ([]TableInfo, error) {
	rows, err := t.db.Query(tableIndexQuery, targetDatabase)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var results []TableInfo
	for rows.Next() {
		var info TableInfo
		err := rows.Scan(
			&info.SiteDatabase,
			&info.SiteTable,
			&info.ClusteredIndex,
			&info.ClusteredColumns,
			&info.ComClusteredIdx,
			&info.TableRows,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Process results to modify ClusteredColumns for non-clustered indexes
		if info.ClusteredIndex == "No" {
			info.ClusteredColumns = sql.NullString{String: "_tidb_rowid", Valid: true}
			fmt.Printf("🔧 Modified ClusteredColumns for table %s: %s -> _tidb_rowid\n",
				info.SiteTable, info.ClusteredIndex)
		}

		results = append(results, info)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	fmt.Printf("📊 Retrieved %d records from database '%s'\n", len(results), targetDatabase)
	return results, nil
}

func (t *TableIndexManager) insertResults(results []TableInfo) error {
	if len(results) == 0 {
		fmt.Println("⚠️  No data to insert")
		return nil
	}

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s 
		(site_database, site_table, clustered_index, clustered_columns, com_clusted_index, table_rows)
		VALUES (?, ?, ?, ?, ?, ?)`, sitemergeTableName)

	tx, err := t.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, info := range results {
		_, err := stmt.Exec(
			info.SiteDatabase,
			info.SiteTable,
			info.ClusteredIndex,
			info.ClusteredColumns,
			info.ComClusteredIdx,
			info.TableRows,
		)
		if err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("✅ Successfully inserted %d records into '%s'\n", len(results), sitemergeTableName)
	return nil
}
