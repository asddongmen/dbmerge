package main

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableIndexManager_executePrepareSQLs(t *testing.T) {
	tests := []struct {
		name           string
		expectError    bool
		mockSetup      func(mock sqlmock.Sqlmock)
		expectedResult string
	}{
		{
			name:        "successful database creation",
			expectError: false,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE DATABASE IF NOT EXISTS sitemerge").WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name:        "database creation fails",
			expectError: true,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE DATABASE IF NOT EXISTS sitemerge").WillReturnError(fmt.Errorf("database creation failed"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			tt.mockSetup(mock)

			manager := NewTableIndexManager(db)
			err = manager.executePrepareSQLs()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTableIndexManager_createTargetTable(t *testing.T) {
	tests := []struct {
		name        string
		expectError bool
		mockSetup   func(mock sqlmock.Sqlmock)
	}{
		{
			name:        "successful table creation",
			expectError: false,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS sitemerge.sitemerge_table_index_info").WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name:        "table creation fails",
			expectError: true,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS sitemerge.sitemerge_table_index_info").WillReturnError(fmt.Errorf("table creation failed"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			tt.mockSetup(mock)

			manager := NewTableIndexManager(db)
			err = manager.createTargetTable()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTableIndexManager_clearExistingData(t *testing.T) {
	tests := []struct {
		name           string
		targetDatabase string
		expectError    bool
		mockSetup      func(mock sqlmock.Sqlmock)
		expectedRows   int64
	}{
		{
			name:           "successful clear with rows deleted",
			targetDatabase: "test_db",
			expectError:    false,
			expectedRows:   5,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM sitemerge.sitemerge_table_index_info WHERE site_database = ?").
					WithArgs("test_db").
					WillReturnResult(sqlmock.NewResult(0, 5))
			},
		},
		{
			name:           "successful clear with no rows deleted",
			targetDatabase: "empty_db",
			expectError:    false,
			expectedRows:   0,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM sitemerge.sitemerge_table_index_info WHERE site_database = ?").
					WithArgs("empty_db").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name:           "delete operation fails",
			targetDatabase: "fail_db",
			expectError:    true,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM sitemerge.sitemerge_table_index_info WHERE site_database = ?").
					WithArgs("fail_db").
					WillReturnError(fmt.Errorf("delete failed"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			tt.mockSetup(mock)

			manager := NewTableIndexManager(db)
			err = manager.clearExistingData(tt.targetDatabase)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTableIndexManager_executeIndexQuery(t *testing.T) {
	tests := []struct {
		name           string
		targetDatabase string
		expectError    bool
		mockSetup      func(mock sqlmock.Sqlmock)
		expectedTables []TableInfo
	}{
		{
			name:           "successful query with clustered index tables",
			targetDatabase: "test_db",
			expectError:    false,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"site_database", "site_table", "clustered_index", "clustered_columns", "com_clusted_index", "table_rows",
				}).
					AddRow("test_db", "users", "Yes", "id", "No", 1000).
					AddRow("test_db", "orders", "Yes", "order_id,user_id", "No", 5000)

				mock.ExpectQuery("SELECT DISTINCT").
					WithArgs("test_db").
					WillReturnRows(rows)
			},
			expectedTables: []TableInfo{
				{
					SiteDatabase:     "test_db",
					SiteTable:        "users",
					ClusteredIndex:   "Yes",
					ClusteredColumns: sql.NullString{String: "id", Valid: true},
					ComClusteredIdx:  "No",
					TableRows:        sql.NullInt64{Int64: 1000, Valid: true},
				},
				{
					SiteDatabase:     "test_db",
					SiteTable:        "orders",
					ClusteredIndex:   "Yes",
					ClusteredColumns: sql.NullString{String: "order_id,user_id", Valid: true},
					ComClusteredIdx:  "No",
					TableRows:        sql.NullInt64{Int64: 5000, Valid: true},
				},
			},
		},
		{
			name:           "successful query with non-clustered index tables",
			targetDatabase: "test_db",
			expectError:    false,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"site_database", "site_table", "clustered_index", "clustered_columns", "com_clusted_index", "table_rows",
				}).
					AddRow("test_db", "logs", "No", nil, "No", 10000)

				mock.ExpectQuery("SELECT DISTINCT").
					WithArgs("test_db").
					WillReturnRows(rows)
			},
			expectedTables: []TableInfo{
				{
					SiteDatabase:     "test_db",
					SiteTable:        "logs",
					ClusteredIndex:   "No",
					ClusteredColumns: sql.NullString{String: "_tidb_rowid", Valid: true}, // Modified by the function
					ComClusteredIdx:  "No",
					TableRows:        sql.NullInt64{Int64: 10000, Valid: true},
				},
			},
		},
		{
			name:           "empty result set",
			targetDatabase: "empty_db",
			expectError:    false,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"site_database", "site_table", "clustered_index", "clustered_columns", "com_clusted_index", "table_rows",
				})

				mock.ExpectQuery("SELECT DISTINCT").
					WithArgs("empty_db").
					WillReturnRows(rows)
			},
			expectedTables: []TableInfo{},
		},
		{
			name:           "query execution fails",
			targetDatabase: "fail_db",
			expectError:    true,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT DISTINCT").
					WithArgs("fail_db").
					WillReturnError(fmt.Errorf("query execution failed"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			tt.mockSetup(mock)

			manager := NewTableIndexManager(db)
			result, err := manager.executeIndexQuery(tt.targetDatabase)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(tt.expectedTables), len(result))

				for i, expected := range tt.expectedTables {
					assert.Equal(t, expected.SiteDatabase, result[i].SiteDatabase)
					assert.Equal(t, expected.SiteTable, result[i].SiteTable)
					assert.Equal(t, expected.ClusteredIndex, result[i].ClusteredIndex)
					assert.Equal(t, expected.ClusteredColumns, result[i].ClusteredColumns)
					assert.Equal(t, expected.ComClusteredIdx, result[i].ComClusteredIdx)
					assert.Equal(t, expected.TableRows, result[i].TableRows)
				}
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTableIndexManager_insertResults(t *testing.T) {
	tests := []struct {
		name        string
		results     []TableInfo
		expectError bool
		mockSetup   func(mock sqlmock.Sqlmock, results []TableInfo)
	}{
		{
			name: "successful insert with multiple records",
			results: []TableInfo{
				{
					SiteDatabase:     "test_db",
					SiteTable:        "users",
					ClusteredIndex:   "Yes",
					ClusteredColumns: sql.NullString{String: "id", Valid: true},
					ComClusteredIdx:  "No",
					TableRows:        sql.NullInt64{Int64: 1000, Valid: true},
				},
				{
					SiteDatabase:     "test_db",
					SiteTable:        "orders",
					ClusteredIndex:   "No",
					ClusteredColumns: sql.NullString{String: "_tidb_rowid", Valid: true},
					ComClusteredIdx:  "No",
					TableRows:        sql.NullInt64{Int64: 5000, Valid: true},
				},
			},
			expectError: false,
			mockSetup: func(mock sqlmock.Sqlmock, results []TableInfo) {
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO sitemerge.sitemerge_table_index_info")

				for _, result := range results {
					mock.ExpectExec("INSERT INTO sitemerge.sitemerge_table_index_info").
						WithArgs(result.SiteDatabase, result.SiteTable, result.ClusteredIndex,
							result.ClusteredColumns, result.ComClusteredIdx, result.TableRows).
						WillReturnResult(sqlmock.NewResult(1, 1))
				}

				mock.ExpectCommit()
			},
		},
		{
			name:        "empty results - no insert needed",
			results:     []TableInfo{},
			expectError: false,
			mockSetup: func(mock sqlmock.Sqlmock, results []TableInfo) {
				// No expectations - function should return early
			},
		},
		{
			name: "transaction begin fails",
			results: []TableInfo{
				{
					SiteDatabase:     "test_db",
					SiteTable:        "users",
					ClusteredIndex:   "Yes",
					ClusteredColumns: sql.NullString{String: "id", Valid: true},
					ComClusteredIdx:  "No",
					TableRows:        sql.NullInt64{Int64: 1000, Valid: true},
				},
			},
			expectError: true,
			mockSetup: func(mock sqlmock.Sqlmock, results []TableInfo) {
				mock.ExpectBegin().WillReturnError(fmt.Errorf("begin transaction failed"))
			},
		},
		{
			name: "insert execution fails",
			results: []TableInfo{
				{
					SiteDatabase:     "test_db",
					SiteTable:        "users",
					ClusteredIndex:   "Yes",
					ClusteredColumns: sql.NullString{String: "id", Valid: true},
					ComClusteredIdx:  "No",
					TableRows:        sql.NullInt64{Int64: 1000, Valid: true},
				},
			},
			expectError: true,
			mockSetup: func(mock sqlmock.Sqlmock, results []TableInfo) {
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO sitemerge.sitemerge_table_index_info")
				mock.ExpectExec("INSERT INTO sitemerge.sitemerge_table_index_info").
					WithArgs("test_db", "users", "Yes", sql.NullString{String: "id", Valid: true}, "No", sql.NullInt64{Int64: 1000, Valid: true}).
					WillReturnError(fmt.Errorf("insert execution failed"))
				mock.ExpectRollback()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			tt.mockSetup(mock, tt.results)

			manager := NewTableIndexManager(db)
			err = manager.insertResults(tt.results)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTableIndexManager_Run(t *testing.T) {
	tests := []struct {
		name           string
		targetDatabase string
		clearExisting  bool
		expectError    bool
		mockSetup      func(mock sqlmock.Sqlmock)
	}{
		{
			name:           "successful complete run with clear existing",
			targetDatabase: "test_db",
			clearExisting:  true,
			expectError:    false,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// executePrepareSQLs
				mock.ExpectExec("CREATE DATABASE IF NOT EXISTS sitemerge").WillReturnResult(sqlmock.NewResult(0, 0))

				// createTargetTable
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS sitemerge.sitemerge_table_index_info").WillReturnResult(sqlmock.NewResult(0, 0))

				// clearExistingData
				mock.ExpectExec("DELETE FROM sitemerge.sitemerge_table_index_info WHERE site_database = ?").
					WithArgs("test_db").
					WillReturnResult(sqlmock.NewResult(0, 2))

				// executeIndexQuery
				rows := sqlmock.NewRows([]string{
					"site_database", "site_table", "clustered_index", "clustered_columns", "com_clusted_index", "table_rows",
				}).AddRow("test_db", "users", "Yes", "id", "No", 1000)

				mock.ExpectQuery("SELECT DISTINCT").
					WithArgs("test_db").
					WillReturnRows(rows)

				// insertResults
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO sitemerge.sitemerge_table_index_info")
				mock.ExpectExec("INSERT INTO sitemerge.sitemerge_table_index_info").
					WithArgs("test_db", "users", "Yes", sql.NullString{String: "id", Valid: true}, "No", sql.NullInt64{Int64: 1000, Valid: true}).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			},
		},
		{
			name:           "successful complete run without clear existing",
			targetDatabase: "test_db",
			clearExisting:  false,
			expectError:    false,
			mockSetup: func(mock sqlmock.Sqlmock) {
				// executePrepareSQLs
				mock.ExpectExec("CREATE DATABASE IF NOT EXISTS sitemerge").WillReturnResult(sqlmock.NewResult(0, 0))

				// createTargetTable
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS sitemerge.sitemerge_table_index_info").WillReturnResult(sqlmock.NewResult(0, 0))

				// Skip clearExistingData since clearExisting is false

				// executeIndexQuery
				rows := sqlmock.NewRows([]string{
					"site_database", "site_table", "clustered_index", "clustered_columns", "com_clusted_index", "table_rows",
				}).AddRow("test_db", "users", "Yes", "id", "No", 1000)

				mock.ExpectQuery("SELECT DISTINCT").
					WithArgs("test_db").
					WillReturnRows(rows)

				// insertResults
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO sitemerge.sitemerge_table_index_info")
				mock.ExpectExec("INSERT INTO sitemerge.sitemerge_table_index_info").
					WithArgs("test_db", "users", "Yes", sql.NullString{String: "id", Valid: true}, "No", sql.NullInt64{Int64: 1000, Valid: true}).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			},
		},
		{
			name:           "failure in prepare SQLs",
			targetDatabase: "test_db",
			clearExisting:  true,
			expectError:    true,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE DATABASE IF NOT EXISTS sitemerge").WillReturnError(fmt.Errorf("database creation failed"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			tt.mockSetup(mock)

			manager := NewTableIndexManager(db)
			err = manager.Run(tt.targetDatabase, tt.clearExisting)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestNewTableIndexManager(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := NewTableIndexManager(db)

	assert.NotNil(t, manager)
	assert.Equal(t, db, manager.db)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestTableInfo_Structure(t *testing.T) {
	// Test that TableInfo structure works correctly
	info := TableInfo{
		SiteDatabase:     "test_db",
		SiteTable:        "test_table",
		ClusteredIndex:   "Yes",
		ClusteredColumns: sql.NullString{String: "id", Valid: true},
		ComClusteredIdx:  "No",
		TableRows:        sql.NullInt64{Int64: 1000, Valid: true},
	}

	assert.Equal(t, "test_db", info.SiteDatabase)
	assert.Equal(t, "test_table", info.SiteTable)
	assert.Equal(t, "Yes", info.ClusteredIndex)
	assert.Equal(t, "id", info.ClusteredColumns.String)
	assert.True(t, info.ClusteredColumns.Valid)
	assert.Equal(t, "No", info.ComClusteredIdx)
	assert.Equal(t, int64(1000), info.TableRows.Int64)
	assert.True(t, info.TableRows.Valid)
}
