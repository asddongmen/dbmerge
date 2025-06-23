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
			generator := NewPageInfoGenerator(db, 1000, 4)

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
			generator := NewPageInfoGenerator(db, 1000, 4)

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
			generator := NewPageInfoGenerator(db, 1000, 4)

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
			generator := NewPageInfoGenerator(db, 1000, 10)

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
