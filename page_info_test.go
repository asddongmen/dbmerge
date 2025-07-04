package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Disable log output during tests
	log.SetOutput(os.Stderr)
	os.Exit(m.Run())
}

func TestFilterRemainingTables(t *testing.T) {
	tests := []struct {
		name          string
		inputTables   []TableToProcess
		mockSetup     func(mock sqlmock.Sqlmock)
		expectedCount int
		expectedNames []string
	}{
		{
			name: "Tables with different statuses",
			inputTables: []TableToProcess{
				{SiteDatabase: "test", SiteTable: "table1", ClusteredColumns: "id", TableRows: 1000},
				{SiteDatabase: "test", SiteTable: "table2", ClusteredColumns: "id", TableRows: 2000},
				{SiteDatabase: "test", SiteTable: "table3", ClusteredColumns: "id", TableRows: 3000},
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				// table1: new (no record)
				mock.ExpectQuery("SELECT status, last_end_key, processed_rows").
					WithArgs("test", "table1").
					WillReturnError(sql.ErrNoRows)

				// table2: completed
				rows2 := sqlmock.NewRows([]string{"status", "last_end_key", "processed_rows"}).
					AddRow("completed", 0, 2000)
				mock.ExpectQuery("SELECT status, last_end_key, processed_rows").
					WithArgs("test", "table2").
					WillReturnRows(rows2)

				// table3: processing
				rows3 := sqlmock.NewRows([]string{"status", "last_end_key", "processed_rows"}).
					AddRow("processing", 1500, 1500)
				mock.ExpectQuery("SELECT status, last_end_key, processed_rows").
					WithArgs("test", "table3").
					WillReturnRows(rows3)
			},
			expectedCount: 2,
			expectedNames: []string{"test.table1", "test.table3"},
		},
		{
			name: "Error in progress check should include table",
			inputTables: []TableToProcess{
				{SiteDatabase: "test", SiteTable: "error_table", ClusteredColumns: "id", TableRows: 1000},
				{SiteDatabase: "test", SiteTable: "good_table", ClusteredColumns: "id", TableRows: 2000},
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				// error_table: database error
				mock.ExpectQuery("SELECT status, last_end_key, processed_rows").
					WithArgs("test", "error_table").
					WillReturnError(fmt.Errorf("database connection error"))

				// good_table: completed
				rows := sqlmock.NewRows([]string{"status", "last_end_key", "processed_rows"}).
					AddRow("completed", 0, 2000)
				mock.ExpectQuery("SELECT status, last_end_key, processed_rows").
					WithArgs("test", "good_table").
					WillReturnRows(rows)
			},
			expectedCount: 1,
			expectedNames: []string{"test.error_table"},
		},
		{
			name: "All tables completed",
			inputTables: []TableToProcess{
				{SiteDatabase: "test", SiteTable: "table1", ClusteredColumns: "id", TableRows: 1000},
				{SiteDatabase: "test", SiteTable: "table2", ClusteredColumns: "id", TableRows: 2000},
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				// table1: completed
				rows1 := sqlmock.NewRows([]string{"status", "last_end_key", "processed_rows"}).
					AddRow("completed", 0, 1000)
				mock.ExpectQuery("SELECT status, last_end_key, processed_rows").
					WithArgs("test", "table1").
					WillReturnRows(rows1)

				// table2: completed
				rows2 := sqlmock.NewRows([]string{"status", "last_end_key", "processed_rows"}).
					AddRow("completed", 0, 2000)
				mock.ExpectQuery("SELECT status, last_end_key, processed_rows").
					WithArgs("test", "table2").
					WillReturnRows(rows2)
			},
			expectedCount: 0,
			expectedNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock database
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Setup mock expectations
			tt.mockSetup(mock)

			// Create PageInfoGenerator
			generator := NewPageInfoGenerator(db, 1000, 4, "test")

			// Capture log output
			var logOutput strings.Builder
			log.SetOutput(&logOutput)
			defer log.SetOutput(os.Stderr)

			// Call the function under test
			result := generator.filterRemainingTables(tt.inputTables)

			// Verify the result
			assert.Equal(t, tt.expectedCount, len(result), "Should return correct number of remaining tables")

			actualNames := make([]string, len(result))
			for i, table := range result {
				actualNames[i] = fmt.Sprintf("%s.%s", table.SiteDatabase, table.SiteTable)
			}
			assert.ElementsMatch(t, tt.expectedNames, actualNames, "Should return correct table names")

			// Verify all mock expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestCheckTableExists(t *testing.T) {
	tests := []struct {
		name      string
		database  string
		table     string
		mockSetup func(mock sqlmock.Sqlmock)
		expected  bool
		wantErr   bool
	}{
		{
			name:     "Table exists",
			database: "test",
			table:    "bank10",
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"count"}).AddRow(1)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\)").
					WithArgs("test", "bank10").
					WillReturnRows(rows)
			},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "Table does not exist",
			database: "test",
			table:    "nonexistent",
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"count"}).AddRow(0)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\)").
					WithArgs("test", "nonexistent").
					WillReturnRows(rows)
			},
			expected: false,
			wantErr:  false,
		},
		{
			name:     "Database error",
			database: "test",
			table:    "error_table",
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT COUNT\\(\\*\\)").
					WithArgs("test", "error_table").
					WillReturnError(fmt.Errorf("database error"))
			},
			expected: false,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock database
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Setup mock expectations
			tt.mockSetup(mock)

			// Create PageInfoGenerator
			generator := NewPageInfoGenerator(db, 1000, 4, "test")

			// Call the function under test
			result, err := generator.CheckTableExists(tt.database, tt.table)

			// Verify the result
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}

			// Verify all mock expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDiagnoseTable(t *testing.T) {
	tests := []struct {
		name      string
		database  string
		table     string
		mockSetup func(mock sqlmock.Sqlmock)
		wantErr   bool
	}{
		{
			name:     "Table not in index info",
			database: "test",
			table:    "missing_table",
			mockSetup: func(mock sqlmock.Sqlmock) {
				// CheckTableExists returns false
				rows := sqlmock.NewRows([]string{"count"}).AddRow(0)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\)").
					WithArgs("test", "missing_table").
					WillReturnRows(rows)
			},
			wantErr: false,
		},
		{
			name:     "Table exists and completed",
			database: "test",
			table:    "completed_table",
			mockSetup: func(mock sqlmock.Sqlmock) {
				// CheckTableExists returns true
				existsRows := sqlmock.NewRows([]string{"count"}).AddRow(1)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\)").
					WithArgs("test", "completed_table").
					WillReturnRows(existsRows)

				// getProgressStatus returns completed
				statusRows := sqlmock.NewRows([]string{"status", "last_end_key", "processed_rows"}).
					AddRow("completed", 0, 1000)
				mock.ExpectQuery("SELECT status, last_end_key, processed_rows").
					WithArgs("test", "completed_table").
					WillReturnRows(statusRows)

				// Check page_info count
				pageRows := sqlmock.NewRows([]string{"count"}).AddRow(10)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM sitemerge.page_info").
					WithArgs("test", "completed_table").
					WillReturnRows(pageRows)
			},
			wantErr: false,
		},
		{
			name:     "Table exists but status check fails",
			database: "test",
			table:    "error_table",
			mockSetup: func(mock sqlmock.Sqlmock) {
				// CheckTableExists returns true
				existsRows := sqlmock.NewRows([]string{"count"}).AddRow(1)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\)").
					WithArgs("test", "error_table").
					WillReturnRows(existsRows)

				// getProgressStatus returns error
				mock.ExpectQuery("SELECT status, last_end_key, processed_rows").
					WithArgs("test", "error_table").
					WillReturnError(fmt.Errorf("status check error"))
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock database
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Setup mock expectations
			tt.mockSetup(mock)

			// Create PageInfoGenerator
			generator := NewPageInfoGenerator(db, 1000, 4, "test")

			// Capture log output
			var logOutput strings.Builder
			log.SetOutput(&logOutput)
			defer log.SetOutput(os.Stderr)

			// Call the function under test
			err = generator.DiagnoseTable(tt.database, tt.table)

			// Verify the result
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Verify log output contains expected diagnostic information
			logStr := logOutput.String()
			assert.Contains(t, logStr, fmt.Sprintf("🔍 Diagnosing table: %s.%s", tt.database, tt.table))

			// Verify all mock expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPrintAndSortTablesByDatabase(t *testing.T) {
	tests := []struct {
		name            string
		inputTables     []TableToProcess
		expectedOrder   []string       // Expected table names in order (database.table format)
		expectedDbStats map[string]int // Expected table count per database
	}{
		{
			name: "Sort tables with numeric suffixes correctly",
			inputTables: []TableToProcess{
				{SiteDatabase: "test", SiteTable: "bank10", ClusteredColumns: "id", TableRows: 1000},
				{SiteDatabase: "test", SiteTable: "bank1", ClusteredColumns: "id", TableRows: 500},
				{SiteDatabase: "test", SiteTable: "bank2", ClusteredColumns: "id", TableRows: 300},
				{SiteDatabase: "test", SiteTable: "bank20", ClusteredColumns: "id", TableRows: 2000},
				{SiteDatabase: "test", SiteTable: "bank3", ClusteredColumns: "id", TableRows: 400},
			},
			expectedOrder: []string{
				"test.bank1", "test.bank10", "test.bank2", "test.bank20", "test.bank3",
			},
			expectedDbStats: map[string]int{"test": 5},
		},
		{
			name: "Sort tables across multiple databases",
			inputTables: []TableToProcess{
				{SiteDatabase: "db2", SiteTable: "table_z", ClusteredColumns: "id", TableRows: 100},
				{SiteDatabase: "db1", SiteTable: "table_b", ClusteredColumns: "id", TableRows: 200},
				{SiteDatabase: "db2", SiteTable: "table_a", ClusteredColumns: "id", TableRows: 150},
				{SiteDatabase: "db1", SiteTable: "table_c", ClusteredColumns: "id", TableRows: 250},
			},
			expectedOrder: []string{
				"db1.table_b", "db1.table_c", "db2.table_a", "db2.table_z",
			},
			expectedDbStats: map[string]int{"db1": 2, "db2": 2},
		},
		{
			name: "Sort complex table names with mixed patterns",
			inputTables: []TableToProcess{
				{SiteDatabase: "prod", SiteTable: "user_10", ClusteredColumns: "id", TableRows: 1000},
				{SiteDatabase: "prod", SiteTable: "user_2", ClusteredColumns: "id", TableRows: 200},
				{SiteDatabase: "prod", SiteTable: "user_1", ClusteredColumns: "id", TableRows: 100},
				{SiteDatabase: "prod", SiteTable: "account_10", ClusteredColumns: "id", TableRows: 500},
				{SiteDatabase: "prod", SiteTable: "account_2", ClusteredColumns: "id", TableRows: 300},
			},
			expectedOrder: []string{
				"prod.account_10", "prod.account_2", "prod.user_1", "prod.user_10", "prod.user_2",
			},
			expectedDbStats: map[string]int{"prod": 5},
		},
		{
			name:            "Empty table list",
			inputTables:     []TableToProcess{},
			expectedOrder:   []string{},
			expectedDbStats: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock database
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Setup mock expectations for getTablesToProcess
			rows := sqlmock.NewRows([]string{"site_database", "site_table", "clustered_columns", "table_rows"})
			for _, table := range tt.inputTables {
				rows.AddRow(table.SiteDatabase, table.SiteTable, table.ClusteredColumns, table.TableRows)
			}
			mock.ExpectQuery("SELECT site_database, site_table, clustered_columns, table_rows").WillReturnRows(rows)

			// Create PageInfoGenerator
			generator := NewPageInfoGenerator(db, 1000, 10, "test")

			// Capture log output
			var logOutput strings.Builder
			log.SetOutput(&logOutput)
			defer log.SetOutput(os.Stderr)

			// Call the function under test
			result, err := generator.printAndSortTablesByDatabase()
			require.NoError(t, err)

			// Verify the result order
			actualOrder := make([]string, len(result))
			for i, table := range result {
				actualOrder[i] = fmt.Sprintf("%s.%s", table.SiteDatabase, table.SiteTable)
			}
			assert.Equal(t, tt.expectedOrder, actualOrder, "Tables should be sorted in correct lexicographical order")

			// Verify database statistics from log output
			logStr := logOutput.String()
			for dbName, expectedCount := range tt.expectedDbStats {
				expectedLogLine := fmt.Sprintf("Database: %s", dbName)
				assert.Contains(t, logStr, expectedLogLine, "Log should contain database name")

				expectedCountLine := fmt.Sprintf("Table count: %d", expectedCount)
				assert.Contains(t, logStr, expectedCountLine, "Log should contain correct table count")
			}

			// Verify all mock expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestLexicographicalSorting(t *testing.T) {
	// Test the core sorting logic separately
	testCases := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "numeric suffixes",
			input:    []string{"bank10", "bank1", "bank2", "bank20", "bank3"},
			expected: []string{"bank1", "bank10", "bank2", "bank20", "bank3"},
		},
		{
			name:     "mixed alphanumeric",
			input:    []string{"table_z", "table_a", "table_10", "table_2"},
			expected: []string{"table_10", "table_2", "table_a", "table_z"},
		},
		{
			name:     "simple alphabetical",
			input:    []string{"zebra", "apple", "banana"},
			expected: []string{"apple", "banana", "zebra"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Make a copy to avoid modifying the original
			result := make([]string, len(tc.input))
			copy(result, tc.input)

			// Sort using Go's standard lexicographical sorting
			sort.Strings(result)

			assert.Equal(t, tc.expected, result, "Lexicographical sorting should match expected order")
		})
	}
}

func TestDatabaseTableStatsStructure(t *testing.T) {
	// Test that our DatabaseTableStats structure works correctly
	stats := DatabaseTableStats{
		DatabaseName: "test_db",
		Tables: []TableToProcess{
			{SiteDatabase: "test_db", SiteTable: "table1", TableRows: 100},
			{SiteDatabase: "test_db", SiteTable: "table2", TableRows: 200},
		},
		TotalCount: 2,
		TotalRows:  300,
	}

	assert.Equal(t, "test_db", stats.DatabaseName)
	assert.Equal(t, 2, len(stats.Tables))
	assert.Equal(t, 2, stats.TotalCount)
	assert.Equal(t, int64(300), stats.TotalRows)
}

func TestProcessSingleTable(t *testing.T) {
	tests := []struct {
		name           string
		resumePoint    ResumePoint
		isResume       bool
		pageSize       int
		mockSetup      func(mock sqlmock.Sqlmock)
		expectedPages  int
		expectedError  bool
		expectedStatus string
	}{
		{
			name: "New table processing - single batch",
			resumePoint: ResumePoint{
				SiteDatabase:     "test_db",
				SiteTable:        "test_table",
				ClusteredColumns: "id",
				TableRows:        5000,
				LastEndKey:       0,
				ProcessedRows:    0,
			},
			isResume: false,
			pageSize: 1000,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Clear existing page info
				mock.ExpectExec("DELETE FROM sitemerge.page_info").
					WithArgs("test_db", "test_table").
					WillReturnResult(sqlmock.NewResult(0, 0))

				// Update progress to processing
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "test_table", "processing", int64(0), int64(5000), int64(0)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// First batch query - no WHERE condition (10 rows, less than batchSize 10000)
				rows := sqlmock.NewRows([]string{"id"}).
					AddRow(1).AddRow(2).AddRow(3).AddRow(4).AddRow(5).
					AddRow(6).AddRow(7).AddRow(8).AddRow(9).AddRow(10)
				mock.ExpectQuery("SELECT /\\*\\+ READ_FROM_STORAGE\\(TIKV\\) \\*/ id").
					WillReturnRows(rows)

				// Insert page info batch - transaction operations (happens before progress update)
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO sitemerge.page_info")
				mock.ExpectExec("INSERT INTO sitemerge.page_info").
					WithArgs("test_db", "test_table", 1, int64(1), int64(10), 10).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()

				// Update progress after first batch
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "test_table", "processing", int64(10), int64(5000), int64(10)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// Mark as completed
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "test_table", "completed", int64(0), int64(5000), int64(10)).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedPages:  1,
			expectedError:  false,
			expectedStatus: "completed",
		},
		{
			name: "Resume table processing",
			resumePoint: ResumePoint{
				SiteDatabase:     "test_db",
				SiteTable:        "test_table",
				ClusteredColumns: "id",
				TableRows:        10000,
				LastEndKey:       1000,
				ProcessedRows:    1000,
			},
			isResume: true,
			pageSize: 1000,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Update progress to processing (resume)
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "test_table", "processing", int64(1000), int64(10000), int64(1000)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// Batch query with WHERE condition (resume from key 1000) - 5 rows, less than batchSize
				rows := sqlmock.NewRows([]string{"id"}).
					AddRow(1001).AddRow(1002).AddRow(1003).AddRow(1004).AddRow(1005)
				mock.ExpectQuery("SELECT /\\*\\+ READ_FROM_STORAGE\\(TIKV\\) \\*/ id").
					WithArgs(int64(1000)).
					WillReturnRows(rows)

				// Insert page info batch (page 2 since we already processed 1000 rows) - transaction operations
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO sitemerge.page_info")
				mock.ExpectExec("INSERT INTO sitemerge.page_info").
					WithArgs("test_db", "test_table", 2, int64(1001), int64(1005), 5).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()

				// Update progress after batch
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "test_table", "processing", int64(1005), int64(10000), int64(1005)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// Mark as completed
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "test_table", "completed", int64(0), int64(10000), int64(1005)).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedPages:  1,
			expectedError:  false,
			expectedStatus: "completed",
		},
		{
			name: "Multiple pages generation",
			resumePoint: ResumePoint{
				SiteDatabase:     "test_db",
				SiteTable:        "test_table",
				ClusteredColumns: "id",
				TableRows:        5000,
				LastEndKey:       0,
				ProcessedRows:    0,
			},
			isResume: false,
			pageSize: 1000,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Clear existing page info
				mock.ExpectExec("DELETE FROM sitemerge.page_info").
					WithArgs("test_db", "test_table").
					WillReturnResult(sqlmock.NewResult(0, 0))

				// Update progress to processing
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "test_table", "processing", int64(0), int64(5000), int64(0)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// First batch - 2500 rows (less than batchSize 10000, so loop continues)
				rows1 := sqlmock.NewRows([]string{"id"})
				for i := 1; i <= 2500; i++ {
					rows1.AddRow(i)
				}
				mock.ExpectQuery("SELECT /\\*\\+ READ_FROM_STORAGE\\(TIKV\\) \\*/ id").
					WillReturnRows(rows1)

				// Insert page info batch - 3 pages (1000, 1000, 500) - transaction operations
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO sitemerge.page_info")
				mock.ExpectExec("INSERT INTO sitemerge.page_info").
					WithArgs("test_db", "test_table", 1, int64(1), int64(1000), 1000).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("INSERT INTO sitemerge.page_info").
					WithArgs("test_db", "test_table", 2, int64(1001), int64(2000), 1000).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("INSERT INTO sitemerge.page_info").
					WithArgs("test_db", "test_table", 3, int64(2001), int64(2500), 500).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()

				// Update progress after first batch
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "test_table", "processing", int64(2500), int64(5000), int64(2500)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// Mark as completed
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "test_table", "completed", int64(0), int64(5000), int64(2500)).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedPages:  3,
			expectedError:  false,
			expectedStatus: "completed",
		},
		{
			name: "Empty table",
			resumePoint: ResumePoint{
				SiteDatabase:     "test_db",
				SiteTable:        "empty_table",
				ClusteredColumns: "id",
				TableRows:        0,
				LastEndKey:       0,
				ProcessedRows:    0,
			},
			isResume: false,
			pageSize: 1000,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Clear existing page info
				mock.ExpectExec("DELETE FROM sitemerge.page_info").
					WithArgs("test_db", "empty_table").
					WillReturnResult(sqlmock.NewResult(0, 0))

				// Update progress to processing
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "empty_table", "processing", int64(0), int64(0), int64(0)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// First batch - empty result
				emptyRows := sqlmock.NewRows([]string{"id"})
				mock.ExpectQuery("SELECT /\\*\\+ READ_FROM_STORAGE\\(TIKV\\) \\*/ id").
					WillReturnRows(emptyRows)

				// Mark as completed (no pages to insert)
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "empty_table", "completed", int64(0), int64(0), int64(0)).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedPages:  0,
			expectedError:  false,
			expectedStatus: "completed",
		},
		{
			name: "Database error during batch query",
			resumePoint: ResumePoint{
				SiteDatabase:     "test_db",
				SiteTable:        "error_table",
				ClusteredColumns: "id",
				TableRows:        1000,
				LastEndKey:       0,
				ProcessedRows:    0,
			},
			isResume: false,
			pageSize: 1000,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Clear existing page info
				mock.ExpectExec("DELETE FROM sitemerge.page_info").
					WithArgs("test_db", "error_table").
					WillReturnResult(sqlmock.NewResult(0, 0))

				// Update progress to processing
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "error_table", "processing", int64(0), int64(1000), int64(0)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// Batch query fails
				mock.ExpectQuery("SELECT /\\*\\+ READ_FROM_STORAGE\\(TIKV\\) \\*/ id").
					WillReturnError(fmt.Errorf("database connection error"))

				// Mark as failed
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "error_table", "failed", int64(0), int64(1000), int64(0)).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedPages:  0,
			expectedError:  true,
			expectedStatus: "failed",
		},
		{
			name: "Error during page info insertion",
			resumePoint: ResumePoint{
				SiteDatabase:     "test_db",
				SiteTable:        "insert_error_table",
				ClusteredColumns: "id",
				TableRows:        1000,
				LastEndKey:       0,
				ProcessedRows:    0,
			},
			isResume: false,
			pageSize: 1000,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Clear existing page info
				mock.ExpectExec("DELETE FROM sitemerge.page_info").
					WithArgs("test_db", "insert_error_table").
					WillReturnResult(sqlmock.NewResult(0, 0))

				// Update progress to processing
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "insert_error_table", "processing", int64(0), int64(1000), int64(0)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// First batch - some data (3 rows, less than batchSize)
				rows := sqlmock.NewRows([]string{"id"}).
					AddRow(1).AddRow(2).AddRow(3)
				mock.ExpectQuery("SELECT /\\*\\+ READ_FROM_STORAGE\\(TIKV\\) \\*/ id").
					WillReturnRows(rows)

				// Update progress after first batch
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "insert_error_table", "processing", int64(3), int64(1000), int64(3)).
					WillReturnResult(sqlmock.NewResult(0, 1))

				// Insert page info batch fails
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO sitemerge.page_info").
					WillReturnError(fmt.Errorf("prepare statement error"))

				// Mark as failed
				mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
					WithArgs("test_db", "insert_error_table", "failed", int64(3), int64(1000), int64(3)).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedPages:  0,
			expectedError:  true,
			expectedStatus: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock database
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Setup mock expectations
			tt.mockSetup(mock)

			// Create PageInfoGenerator
			generator := NewPageInfoGenerator(db, tt.pageSize, 4, "test")

			// Capture log output
			var logOutput strings.Builder
			log.SetOutput(&logOutput)
			defer log.SetOutput(os.Stderr)

			// Call the function under test
			pagesCount, err := generator.processSingleTable(tt.resumePoint, tt.isResume)

			// Verify the result
			if tt.expectedError {
				assert.Error(t, err, "Should return error")
			} else {
				assert.NoError(t, err, "Should not return error")
			}

			assert.Equal(t, tt.expectedPages, pagesCount, "Should return correct number of pages")

			// Verify all mock expectations were met
			assert.NoError(t, mock.ExpectationsWereMet(), "All database expectations should be met")

			// Verify log output contains expected messages
			logContent := logOutput.String()
			if tt.isResume {
				assert.Contains(t, logContent, "Resuming table", "Should log resume message")
			} else {
				assert.Contains(t, logContent, "Processing table", "Should log processing message")
			}

			if !tt.expectedError && tt.expectedPages > 0 {
				assert.Contains(t, logContent, fmt.Sprintf("%d pages generated", tt.expectedPages), "Should log pages generated")
			}
		})
	}
}

func TestProcessSingleTableEdgeCases(t *testing.T) {
	t.Run("Progress update error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		resumePoint := ResumePoint{
			SiteDatabase:     "test_db",
			SiteTable:        "test_table",
			ClusteredColumns: "id",
			TableRows:        1000,
			LastEndKey:       0,
			ProcessedRows:    0,
		}

		// Clear existing page info (called when isResume is false)
		mock.ExpectExec("DELETE FROM sitemerge.page_info").
			WithArgs("test_db", "test_table").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// Progress update fails
		mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
			WithArgs("test_db", "test_table", "processing", int64(0), int64(1000), int64(0)).
			WillReturnError(fmt.Errorf("progress update error"))

		generator := NewPageInfoGenerator(db, 1000, 4, "test")
		pagesCount, err := generator.processSingleTable(resumePoint, false)

		assert.Error(t, err, "Should return error")
		assert.Equal(t, 0, pagesCount, "Should return 0 pages")
		assert.Contains(t, err.Error(), "failed to update progress", "Error should mention progress update")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Clear existing page info error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		resumePoint := ResumePoint{
			SiteDatabase:     "test_db",
			SiteTable:        "test_table",
			ClusteredColumns: "id",
			TableRows:        1000,
			LastEndKey:       0,
			ProcessedRows:    0,
		}

		// Clear existing page info fails
		mock.ExpectExec("DELETE FROM sitemerge.page_info").
			WithArgs("test_db", "test_table").
			WillReturnError(fmt.Errorf("delete error"))

		// Mark as failed
		mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
			WithArgs("test_db", "test_table", "failed", int64(0), int64(1000), int64(0)).
			WillReturnResult(sqlmock.NewResult(0, 1))

		generator := NewPageInfoGenerator(db, 1000, 4, "test")
		pagesCount, err := generator.processSingleTable(resumePoint, false)

		assert.Error(t, err, "Should return error")
		assert.Equal(t, 0, pagesCount, "Should return 0 pages")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Completion status update error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		resumePoint := ResumePoint{
			SiteDatabase:     "test_db",
			SiteTable:        "test_table",
			ClusteredColumns: "id",
			TableRows:        1000,
			LastEndKey:       0,
			ProcessedRows:    0,
		}

		// Clear existing page info
		mock.ExpectExec("DELETE FROM sitemerge.page_info").
			WithArgs("test_db", "test_table").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// Update progress to processing
		mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
			WithArgs("test_db", "test_table", "processing", int64(0), int64(1000), int64(0)).
			WillReturnResult(sqlmock.NewResult(0, 1))

		// Empty batch
		emptyRows := sqlmock.NewRows([]string{"id"})
		mock.ExpectQuery("SELECT /\\*\\+ READ_FROM_STORAGE\\(TIKV\\) \\*/ id").
			WillReturnRows(emptyRows)

		// Completion status update fails
		mock.ExpectExec("INSERT INTO sitemerge.page_info_progress").
			WithArgs("test_db", "test_table", "completed", int64(0), int64(1000), int64(0)).
			WillReturnError(fmt.Errorf("completion update error"))

		generator := NewPageInfoGenerator(db, 1000, 4, "test")
		pagesCount, err := generator.processSingleTable(resumePoint, false)

		assert.Error(t, err, "Should return error")
		assert.Equal(t, 0, pagesCount, "Should return 0 pages")
		assert.Contains(t, err.Error(), "failed to mark as completed", "Error should mention completion")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}
