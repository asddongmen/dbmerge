
### 任务说明
创建一个 Go 程序 `page_info.go`，用于从 TiDB 数据库中提取表的分页信息并存储到 `page_info` 表中。该程序应支持自定义 TiDB 连接参数，并通过分页查询获取所有表的分片信息。


### 功能要求

#### 1. 数据库连接配置
- 接受用户输入的 TiDB host、port、database、username 和 password, page_size 默认 500
- 建立数据库连接并创建 `page_info` 表（如果不存在）。

#### 2. `page_info` 表结构
```sql
CREATE TABLE IF NOT EXISTS page_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    site_database VARCHAR(255),
    site_table VARCHAR(255),
    page_num INT,
    start_key BIGINT,
    end_key BIGINT,
    page_size INT,
    UNIQUE KEY site_table_page_num(site_database, site_table, page_num)
);
```

#### 3. 分页逻辑实现
- **数据源**：遍历 `sitemerge.sitemerge_table_index_info` 表获取所有库、表、索引信息。以下是`sitemerge.sitemerge_table_index_info` 的一个查询结果：
```sql
+-----+---------------+------------+-----------------+-------------------+-------------------+------------+---------------------+
| id  | site_database | site_table | clustered_index | clustered_columns | com_clusted_index | table_rows | created_at          |
+-----+---------------+------------+-----------------+-------------------+-------------------+------------+---------------------+
| 1   | test          | bank0      | Yes             | id                | No                | 207849     | 2025-06-20 10:45:14 |
| 2   | test          | bank1      | Yes             | id                | No                | 0          | 2025-06-20 10:45:14 |
| 3   | test          | bank10     | Yes             | id                | No                | 0          | 2025-06-20 10:45:14 |
| 4   | test          | bank11     | Yes             | id                | No                | 0          | 2025-06-20 10:45:14 |
| 5   | test          | bank12     | Yes             | id                | No                | 0          | 2025-06-20 10:45:14 |
| 6   | test          | bank13     | Yes             | id                | No                | 0          | 2025-06-20 10:45:14 |
| 7   | test          | bank14     | Yes             | id                | No                | 0          | 2025-06-20 10:45:14 |
| 8   | test          | bank15     | Yes             | id                | No                | 0          | 2025-06-20 10:45:14 |
| 9   | test          | bank16     | Yes             | id                | No                | 0          | 2025-06-20 10:45:14 |
| 10  | test          | bank17     | Yes             | id                | No                | 0          | 2025-06-20 10:45:14 |
```
- **分页算法**：按 `_tidb_rowid` 字段进行分页，每页 500 行。
- **查询优化**：使用 TiDB 提示 `/*+ READ_FROM_STORAGE(TIKV) */` 强制从 TiKV 读取数据。
- **分批处理**：每次查询 10,000 行数据，避免内存溢出。


### 核心实现逻辑

#### 1. 初始查询（获取第一页）
下面 sql 中的 divide_column 是上述表中的 clustered_columns 字段，db_name 是上述表中的 site_database 字段，table_name 是上述表中的 site_table 字段。

```sql
SELECT
    FLOOR((t.row_num - 1) / {page_size}) + 1 AS page_num,
    MIN(t.divide_column) AS start_key,
    MAX(t.divide_column) AS end_key,
    COUNT(*) AS page_size
FROM (
    SELECT 
        divide_column, 
        ROW_NUMBER() OVER (ORDER BY divide_column) AS row_num
    FROM (
        SELECT /*+ READ_FROM_STORAGE(TIKV) */ _tidb_rowid AS divide_column
        FROM `{db_name}`.`{table_name}`
        ORDER BY _tidb_rowid
        LIMIT 10000
    ) s
) t
GROUP BY page_num
ORDER BY page_num;
```

#### 2. 循环查询（获取后续页）
```sql
SELECT
    FLOOR((t.row_num - 1) / {page_size}) + 1 AS page_num,
    MIN(t.divide_column) AS start_key,
    MAX(t.divide_column) AS end_key,
    COUNT(*) AS page_size
FROM (
    SELECT 
        divide_column, 
        ROW_NUMBER() OVER (ORDER BY divide_column) AS row_num
    FROM (
        SELECT /*+ READ_FROM_STORAGE(TIKV) */ _tidb_rowid AS divide_column
        FROM `{db_name}`.`{table_name}`
        WHERE _tidb_rowid > {last_max_id}
        ORDER BY _tidb_rowid
        LIMIT 10000
    ) s
) t
GROUP BY page_num
ORDER BY page_num;
```

#### 3. 数据插入
将每次查询结果批量插入到 `page_info` 表中，并记录当前批次的最大 `end_key` 用于下一次查询。


### 脚本执行流程
1. 接收用户输入的数据库连接参数。
2. 创建 `page_info` 表。
3. 查询 `sitemerge.sitemerge_table_index_info` 获取所有表信息。
4. 对每个表执行分页查询，逐步获取并存储分页信息。
5. 打印执行进度和统计信息。


### 技术要求
- 使用 `pymysql` 连接 TiDB。
- 实现异常处理和日志记录。
- 支持命令行参数输入（建议使用 `argparse` 库）。
- 优化大数据量处理性能。


### 输出示例
```plaintext
connect to TiDB database...
create page_info table...
found 10 tables to process...
processing table users...
  - generated 20 pages
  - generated 40 pages
  ...
processing completed! total 1200 pages generated, cost 10 seconds.
```
你还可以想想怎么输出更美观的日志！所有的注释和日志都要用英文！

