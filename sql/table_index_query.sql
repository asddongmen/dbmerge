SELECT DISTINCT
    t.TABLE_SCHEMA as site_database,
    t.TABLE_NAME as site_table,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS s2 
            WHERE s2.TABLE_SCHEMA = t.TABLE_SCHEMA 
            AND s2.TABLE_NAME = t.TABLE_NAME 
            AND s2.INDEX_NAME = 'PRIMARY'
        ) THEN 'Yes'
        ELSE 'No'
    END as clustered_index,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS s2 
            WHERE s2.TABLE_SCHEMA = t.TABLE_SCHEMA 
            AND s2.TABLE_NAME = t.TABLE_NAME 
            AND s2.INDEX_NAME = 'PRIMARY'
        ) THEN (
            SELECT GROUP_CONCAT(s3.COLUMN_NAME ORDER BY s3.SEQ_IN_INDEX)
            FROM INFORMATION_SCHEMA.STATISTICS s3
            WHERE s3.TABLE_SCHEMA = t.TABLE_SCHEMA 
            AND s3.TABLE_NAME = t.TABLE_NAME 
            AND s3.INDEX_NAME = 'PRIMARY'
        )
        ELSE NULL
    END as clustered_columns,
    'No' as com_clusted_index,
    t.TABLE_ROWS
FROM 
    INFORMATION_SCHEMA.TABLES t
WHERE 
    t.TABLE_SCHEMA = %s
    AND t.TABLE_TYPE = 'BASE TABLE'
ORDER BY 
    t.TABLE_NAME; 