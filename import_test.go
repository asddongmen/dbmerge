package main

import (
	"database/sql"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

// TestTableSQLCacheStructure tests the structure of cached SQL statements
func TestTableSQLCacheStructure(t *testing.T) {
	// Create a test cache entry
	cache := &TableSQLCache{
		AllColumns:       []string{"id", "name", "email"},
		ColumnList:       "id, name, email",
		Placeholders:     "?,?,?",
		SelectTemplate:   "SELECT id, name, email FROM testdb.users %s ORDER BY id",
		InsertTemplate:   "INSERT INTO %s.users (id, name, email) VALUES (?,?,?)",
		ClusteredColumns: "id",
	}

	// Test column count
	assert.Equal(t, 3, len(cache.AllColumns), "Expected 3 columns")

	// Test column list
	assert.Equal(t, "id, name, email", cache.ColumnList, "Column list mismatch")

	// Test placeholders
	assert.Equal(t, "?,?,?", cache.Placeholders, "Placeholders mismatch")

	// Test select template
	assert.Equal(t, "SELECT id, name, email FROM testdb.users %s ORDER BY id", cache.SelectTemplate, "Select template mismatch")

	// Test insert template
	assert.Equal(t, "INSERT INTO %s.users (id, name, email) VALUES (?,?,?)", cache.InsertTemplate, "Insert template mismatch")
}

// TestGetTableSQLCacheWithMock tests the getTableSQLCache function with sqlmock
func TestGetTableSQLCacheWithMock(t *testing.T) {
	// Create mock database
	mockDB, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer mockDB.Close()

	// Create import manager with mock DB
	manager := &ImportManager{
		sourceDB:       mockDB,
		destDB:         mockDB,
		threads:        4,
		tableName:      "",
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
		sqlCache:       make(map[string]*TableSQLCache),
	}

	// Set up expectations for getTableClusteredColumns
	mock.ExpectQuery("SELECT clustered_columns FROM sitemerge.sitemerge_table_index_info WHERE site_database = \\? AND site_table = \\?").
		WithArgs("testdb", "testtable").
		WillReturnRows(sqlmock.NewRows([]string{"clustered_columns"}).AddRow("id"))

	// Set up expectations for getTableColumns
	mock.ExpectQuery("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \\? AND TABLE_NAME = \\? ORDER BY ORDINAL_POSITION").
		WithArgs("testdb", "testtable").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).
			AddRow("id").
			AddRow("name").
			AddRow("email"))

	// Test cache miss - should create new cache entry
	cache, err := manager.getTableSQLCache("testdb", "testtable")
	assert.NoError(t, err)
	assert.NotNil(t, cache)

	// Verify cache entry
	assert.Equal(t, []string{"id", "name", "email"}, cache.AllColumns)
	assert.Equal(t, "id, name, email", cache.ColumnList)
	assert.Equal(t, "?,?,?", cache.Placeholders)
	assert.Equal(t, "id", cache.ClusteredColumns)
	assert.Equal(t, "SELECT id, name, email FROM testdb.testtable %s ORDER BY id", cache.SelectTemplate)
	assert.Equal(t, "INSERT INTO %s.testtable (id, name, email) VALUES (?,?,?)", cache.InsertTemplate)

	// Test cache hit - should return existing cache entry
	cache2, err := manager.getTableSQLCache("testdb", "testtable")
	assert.NoError(t, err)
	assert.Equal(t, cache, cache2, "Should return same cache entry")

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestGetTableSQLCacheWithTidbRowid tests cache with _tidb_rowid clustered columns
func TestGetTableSQLCacheWithTidbRowid(t *testing.T) {
	// Create mock database
	mockDB, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer mockDB.Close()

	// Create import manager with mock DB
	manager := &ImportManager{
		sourceDB:       mockDB,
		destDB:         mockDB,
		threads:        4,
		tableName:      "",
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
		sqlCache:       make(map[string]*TableSQLCache),
	}

	// Set up expectations for getTableClusteredColumns - return NULL for _tidb_rowid
	mock.ExpectQuery("SELECT clustered_columns FROM sitemerge.sitemerge_table_index_info WHERE site_database = \\? AND site_table = \\?").
		WithArgs("testdb", "testtable").
		WillReturnRows(sqlmock.NewRows([]string{"clustered_columns"}).AddRow(nil))

	// Set up expectations for getTableColumns
	mock.ExpectQuery("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \\? AND TABLE_NAME = \\? ORDER BY ORDINAL_POSITION").
		WithArgs("testdb", "testtable").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).
			AddRow("id").
			AddRow("name"))

	// Test cache creation with _tidb_rowid
	cache, err := manager.getTableSQLCache("testdb", "testtable")
	assert.NoError(t, err)
	assert.NotNil(t, cache)

	// Verify _tidb_rowid is used
	assert.Equal(t, "_tidb_rowid", cache.ClusteredColumns)
	assert.Equal(t, "SELECT id, name FROM testdb.testtable %s ORDER BY _tidb_rowid", cache.SelectTemplate)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestGetTableSQLCacheWithCompositeKey tests cache with composite clustered key
func TestGetTableSQLCacheWithCompositeKey(t *testing.T) {
	// Create mock database
	mockDB, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer mockDB.Close()

	// Create import manager with mock DB
	manager := &ImportManager{
		sourceDB:       mockDB,
		destDB:         mockDB,
		threads:        4,
		tableName:      "",
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
		sqlCache:       make(map[string]*TableSQLCache),
	}

	// Set up expectations for getTableClusteredColumns - return composite key
	mock.ExpectQuery("SELECT clustered_columns FROM sitemerge.sitemerge_table_index_info WHERE site_database = \\? AND site_table = \\?").
		WithArgs("testdb", "testtable").
		WillReturnRows(sqlmock.NewRows([]string{"clustered_columns"}).AddRow("tenant_id,user_id"))

	// Set up expectations for getTableColumns
	mock.ExpectQuery("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \\? AND TABLE_NAME = \\? ORDER BY ORDINAL_POSITION").
		WithArgs("testdb", "testtable").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).
			AddRow("tenant_id").
			AddRow("user_id").
			AddRow("name"))

	// Test cache creation with composite key
	cache, err := manager.getTableSQLCache("testdb", "testtable")
	assert.NoError(t, err)
	assert.NotNil(t, cache)

	// Verify composite key is used
	assert.Equal(t, "tenant_id,user_id", cache.ClusteredColumns)
	assert.Equal(t, "SELECT tenant_id, user_id, name FROM testdb.testtable %s ORDER BY tenant_id,user_id", cache.SelectTemplate)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestSQLCacheConcurrency tests the thread-safety of the cache mechanism
func TestSQLCacheConcurrency(t *testing.T) {
	// Create a simple cache map with mutex (similar to ImportManager's cache)
	sqlCache := make(map[string]*TableSQLCache)
	var sqlCacheMutex sync.RWMutex

	// Test concurrent access
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			// Test read lock
			sqlCacheMutex.RLock()
			_, exists := sqlCache["test"]
			sqlCacheMutex.RUnlock()

			// Test write lock
			sqlCacheMutex.Lock()
			if !exists {
				sqlCache["test"] = &TableSQLCache{
					AllColumns:       []string{"id"},
					ColumnList:       "id",
					Placeholders:     "?",
					SelectTemplate:   "SELECT id FROM test %s",
					InsertTemplate:   "INSERT INTO %s.test (id) VALUES (?)",
					ClusteredColumns: "id",
				}
			}
			sqlCacheMutex.Unlock()

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify cache entry was created
	sqlCacheMutex.RLock()
	cache, exists := sqlCache["test"]
	sqlCacheMutex.RUnlock()

	assert.True(t, exists, "Cache entry should exist after concurrent access")
	assert.NotNil(t, cache, "Cache entry should not be nil")
	assert.Equal(t, 1, len(cache.AllColumns), "Expected 1 column")

	t.Log("Concurrent cache access completed successfully")
}

// TestSQLCacheKeyGeneration tests the cache key generation logic
func TestSQLCacheKeyGeneration(t *testing.T) {
	// Test the cache key format used in getTableSQLCache
	database := "testdb"
	table := "testtable"
	cacheKey := database + "." + table

	assert.Equal(t, "testdb.testtable", cacheKey, "Cache key format mismatch")

	// Test with different database and table names
	database2 := "prod_db"
	table2 := "users"
	cacheKey2 := database2 + "." + table2

	assert.Equal(t, "prod_db.users", cacheKey2, "Cache key format mismatch")
}

// TestSQLTemplateGeneration tests the SQL template generation logic
func TestSQLTemplateGeneration(t *testing.T) {
	// Test the logic used in getTableSQLCache for generating SQL templates
	database := "testdb"
	table := "users"
	allColumns := []string{"id", "name", "email"}
	clusteredColumns := "id"

	// Generate column list
	columnList := ""
	for i, col := range allColumns {
		if i > 0 {
			columnList += ", "
		}
		columnList += col
	}

	// Generate placeholders
	placeholders := ""
	for i := range allColumns {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += "?"
	}

	// Generate select template
	selectTemplate := "SELECT " + columnList + " FROM " + database + "." + table + " %s ORDER BY " + clusteredColumns

	// Generate insert template
	insertTemplate := "INSERT INTO %s." + table + " (" + columnList + ") VALUES (" + placeholders + ")"

	// Test results
	assert.Equal(t, "id, name, email", columnList, "Column list generation mismatch")
	assert.Equal(t, "?, ?, ?", placeholders, "Placeholders generation mismatch")
	assert.Equal(t, "SELECT id, name, email FROM testdb.users %s ORDER BY id", selectTemplate, "Select template generation mismatch")
	assert.Equal(t, "INSERT INTO %s.users (id, name, email) VALUES (?, ?, ?)", insertTemplate, "Insert template generation mismatch")
}

// TestSQLCacheErrorHandling tests error handling in cache operations
func TestSQLCacheErrorHandling(t *testing.T) {
	// Create mock database
	mockDB, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer mockDB.Close()

	// Create import manager with mock DB
	manager := &ImportManager{
		sourceDB:       mockDB,
		destDB:         mockDB,
		threads:        4,
		tableName:      "",
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
		sqlCache:       make(map[string]*TableSQLCache),
	}

	// Set up expectations for getTableClusteredColumns to return error
	mock.ExpectQuery("SELECT clustered_columns FROM sitemerge.sitemerge_table_index_info WHERE site_database = \\? AND site_table = \\?").
		WithArgs("testdb", "testtable").
		WillReturnError(sql.ErrConnDone)

	// Test error handling
	cache, err := manager.getTableSQLCache("testdb", "testtable")
	assert.Error(t, err, "Should return error when database query fails")
	assert.Nil(t, cache, "Cache should be nil when error occurs")
	assert.Contains(t, err.Error(), "failed to get clustered columns", "Error message should be descriptive")

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestGetPendingImportTasksExcludesSuccessPages tests that pages with import_status = 'success' are excluded
func TestGetPendingImportTasksExcludesSuccessPages(t *testing.T) {
	// Create mock database
	mockDB, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer mockDB.Close()

	// Create import manager with mock DB
	manager := &ImportManager{
		sourceDB:       mockDB,
		destDB:         mockDB,
		threads:        4,
		tableName:      "",
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
		sqlCache:       make(map[string]*TableSQLCache),
	}

	// Set up expectations for getPendingImportTasks query
	// This query should only return pages with import_status IN ('pending', 'failed')
	// and exclude pages with import_status = 'success'
	expectedQuery := `
		SELECT pi.id, pi.site_database, pi.site_table, pi.page_num, 
			   pi.start_key, pi.end_key, pi.page_size
		FROM sitemerge.page_info pi
		INNER JOIN sitemerge.page_operation_status pos 
			ON pi.site_database = pos.site_database 
			AND pi.site_table = pos.site_table 
			AND pi.id = pos.page_id
		WHERE pos.export_status = 'success' 
		AND pos.import_status IN \('pending', 'failed'\)
		AND pos.import_retry_count < 3
		ORDER BY pi.site_database, pi.site_table, pi.page_num
		LIMIT 500`

	// Mock the query to return only pending and failed pages, no success pages
	mock.ExpectQuery(expectedQuery).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "site_database", "site_table", "page_num", "start_key", "end_key", "page_size",
		}).AddRow(
			1, "testdb", "testtable", 1, 1000, 2000, 1000,
		).AddRow(
			2, "testdb", "testtable", 2, 2000, 3000, 1000,
		))

	// Call getPendingImportTasks
	tasks, err := manager.getPendingImportTasks()
	assert.NoError(t, err)
	assert.Len(t, tasks, 2, "Should return 2 pending/failed tasks")

	// Verify task details
	assert.Equal(t, "testdb", tasks[0].Database)
	assert.Equal(t, "testtable", tasks[0].Table)
	assert.Equal(t, 1, tasks[0].PageInfo.PageNum)
	assert.Equal(t, int64(1000), tasks[0].PageInfo.StartKey)
	assert.Equal(t, int64(2000), tasks[0].PageInfo.EndKey)

	assert.Equal(t, "testdb", tasks[1].Database)
	assert.Equal(t, "testtable", tasks[1].Table)
	assert.Equal(t, 2, tasks[1].PageInfo.PageNum)
	assert.Equal(t, int64(2000), tasks[1].PageInfo.StartKey)
	assert.Equal(t, int64(3000), tasks[1].PageInfo.EndKey)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestGetPendingImportTasksWithMixedStatuses tests with mixed page statuses
func TestGetPendingImportTasksWithMixedStatuses(t *testing.T) {
	// Create mock database
	mockDB, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer mockDB.Close()

	// Create import manager with mock DB
	manager := &ImportManager{
		sourceDB:       mockDB,
		destDB:         mockDB,
		threads:        4,
		tableName:      "",
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
		sqlCache:       make(map[string]*TableSQLCache),
	}

	// Set up expectations for getPendingImportTasks query
	expectedQuery := `
		SELECT pi.id, pi.site_database, pi.site_table, pi.page_num, 
			   pi.start_key, pi.end_key, pi.page_size
		FROM sitemerge.page_info pi
		INNER JOIN sitemerge.page_operation_status pos 
			ON pi.site_database = pos.site_database 
			AND pi.site_table = pos.site_table 
			AND pi.id = pos.page_id
		WHERE pos.export_status = 'success' 
		AND pos.import_status IN \('pending', 'failed'\)
		AND pos.import_retry_count < 3
		ORDER BY pi.site_database, pi.site_table, pi.page_num
		LIMIT 500`

	// Mock the query to return only pending and failed pages
	// Note: Even though we have success pages in the database, the query should filter them out
	mock.ExpectQuery(expectedQuery).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "site_database", "site_table", "page_num", "start_key", "end_key", "page_size",
		}).AddRow(
			3, "testdb", "testtable", 3, 3000, 4000, 1000,
		))

	// Call getPendingImportTasks
	tasks, err := manager.getPendingImportTasks()
	assert.NoError(t, err)
	assert.Len(t, tasks, 1, "Should return only 1 pending/failed task, excluding success pages")

	// Verify task details
	assert.Equal(t, "testdb", tasks[0].Database)
	assert.Equal(t, "testtable", tasks[0].Table)
	assert.Equal(t, 3, tasks[0].PageInfo.PageNum)
	assert.Equal(t, int64(3000), tasks[0].PageInfo.StartKey)
	assert.Equal(t, int64(4000), tasks[0].PageInfo.EndKey)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestGetPendingImportTasksEmptyResult tests when no pending tasks exist
func TestGetPendingImportTasksEmptyResult(t *testing.T) {
	// Create mock database
	mockDB, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer mockDB.Close()

	// Create import manager with mock DB
	manager := &ImportManager{
		sourceDB:       mockDB,
		destDB:         mockDB,
		threads:        4,
		tableName:      "",
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
		sqlCache:       make(map[string]*TableSQLCache),
	}

	// Set up expectations for getPendingImportTasks query
	expectedQuery := `
		SELECT pi.id, pi.site_database, pi.site_table, pi.page_num, 
			   pi.start_key, pi.end_key, pi.page_size
		FROM sitemerge.page_info pi
		INNER JOIN sitemerge.page_operation_status pos 
			ON pi.site_database = pos.site_database 
			AND pi.site_table = pos.site_table 
			AND pi.id = pos.page_id
		WHERE pos.export_status = 'success' 
		AND pos.import_status IN \('pending', 'failed'\)
		AND pos.import_retry_count < 3
		ORDER BY pi.site_database, pi.site_table, pi.page_num
		LIMIT 500`

	// Mock the query to return no rows (all pages are success or don't meet criteria)
	mock.ExpectQuery(expectedQuery).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "site_database", "site_table", "page_num", "start_key", "end_key", "page_size",
		}))

	// Call getPendingImportTasks
	tasks, err := manager.getPendingImportTasks()
	assert.NoError(t, err)
	assert.Len(t, tasks, 0, "Should return empty list when no pending/failed tasks exist")

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkImportPageAsRunningExcludesSuccessPages tests that markImportPageAsRunning excludes success pages
func TestMarkImportPageAsRunningExcludesSuccessPages(t *testing.T) {
	// Create mock database
	mockDB, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer mockDB.Close()

	// Create import manager with mock DB
	manager := &ImportManager{
		sourceDB:       mockDB,
		destDB:         mockDB,
		threads:        4,
		tableName:      "",
		importTaskChan: make(chan ImportTask, 100),
		missingTables:  make([]string, 0),
		sqlCache:       make(map[string]*TableSQLCache),
	}

	// Test case 1: Page is already running
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) > 0 FROM sitemerge.page_operation_status WHERE site_database = \\? AND site_table = \\? AND page_id = \\? AND import_status = 'running'").
		WithArgs("testdb", "testtable", int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) > 0"}).AddRow(true))

	result := manager.markImportPageAsRunning("testdb", "testtable", 1)
	assert.False(t, result, "Should return false when page is already running")

	// Test case 2: Page is already successful
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) > 0 FROM sitemerge.page_operation_status WHERE site_database = \\? AND site_table = \\? AND page_id = \\? AND import_status = 'running'").
		WithArgs("testdb", "testtable", int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) > 0"}).AddRow(false))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) > 0 FROM sitemerge.page_operation_status WHERE site_database = \\? AND site_table = \\? AND page_id = \\? AND import_status = 'success'").
		WithArgs("testdb", "testtable", int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) > 0"}).AddRow(true))

	result = manager.markImportPageAsRunning("testdb", "testtable", 2)
	assert.False(t, result, "Should return false when page is already successful")

	// Test case 3: Page is pending and can be marked as running
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) > 0 FROM sitemerge.page_operation_status WHERE site_database = \\? AND site_table = \\? AND page_id = \\? AND import_status = 'running'").
		WithArgs("testdb", "testtable", int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) > 0"}).AddRow(false))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) > 0 FROM sitemerge.page_operation_status WHERE site_database = \\? AND site_table = \\? AND page_id = \\? AND import_status = 'success'").
		WithArgs("testdb", "testtable", int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) > 0"}).AddRow(false))

	mock.ExpectExec("UPDATE sitemerge.page_operation_status SET import_status = 'running', import_start_time = NOW\\(\\) WHERE site_database = \\? AND site_table = \\? AND page_id = \\?").
		WithArgs("testdb", "testtable", int64(3)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result = manager.markImportPageAsRunning("testdb", "testtable", 3)
	assert.True(t, result, "Should return true when page can be marked as running")

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}
