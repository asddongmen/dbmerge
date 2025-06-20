# SiteMerge - TiDB Table Index Information Manager

This Python program connects to TiDB, creates a table to store index information, and populates it with data from a target database.

## Features

- Connect to TiDB using provided credentials
- Create `sitemerge_table_index_info` table if it doesn't exist
- Query table index information from INFORMATION_SCHEMA
- Insert query results into the target table
- Configurable table name via magic constant

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

```bash
python sitemerge.py --host <host> --port <port> --user <username> --password <password> --database <database_name>
```

### Arguments

- `--host`: TiDB host address
- `--port`: TiDB port number  
- `--user`: TiDB username
- `--password`: TiDB password
- `--database`: Target database name to analyze
- `--keep-existing`: (Optional) Keep existing data, do not clear before inserting

### Example

```bash
python sitemerge.py --host 127.0.0.1 --port 4000 --user root --password mypassword --database test
```

## Configuration

The target table name can be modified by changing the `SITEMERGE_TABLE_NAME` constant in the Python file:

```python
SITEMERGE_TABLE_NAME = "sitemerge_table_index_info"
```

## Output Table Schema

The program creates a table with the following structure:

```sql
CREATE TABLE sitemerge_table_index_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    siteDatabase VARCHAR(255) NOT NULL,
    sitetable VARCHAR(255) NOT NULL,
    ClusteredIndex VARCHAR(10) NOT NULL,
    ClusteredColumns TEXT,
    Com_clustedInd VARCHAR(10) NOT NULL,
    TABLE_ROWS BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_database_table (siteDatabase, sitetable)
);
```

## SQL Query

The program uses a predefined SQL query stored in `sql/table_index_query.sql` to extract table index information from the TiDB INFORMATION_SCHEMA. 