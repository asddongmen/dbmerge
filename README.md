# SiteMerge - TiDB Table Management Tool

SiteMerge is a powerful command-line tool written in Go that provides comprehensive table management capabilities for TiDB databases. It offers two main functionalities: collecting table index information from target databases and generating pagination information for efficient data processing.

## Features

### 🔍 Table Index Analysis (`table-index` command)
- Analyzes target database tables and collects index information
- Creates a `sitemerge` database for storing metadata
- Identifies clustered indexes and primary keys
- Collects table row counts and structure information
- Supports clearing existing data or keeping historical records

### 📄 Page Information Generation (`page-info` command) 
- Generates pagination information based on table index data
- Supports resume capability for interrupted processing
- Batched processing for large datasets (10K rows per batch)
- Configurable page sizes for optimal performance
- Progress tracking with status updates
- Automatic handling of clustered and non-clustered indexes

## Installation

### Prerequisites
- Go 1.21 or higher
- Access to a TiDB database
- Required Go dependencies (automatically managed)

### Build from Source
```bash
# Clone or create the project directory
mkdir sitemerge && cd sitemerge

# Copy the source files (main.go, table_index.go, page_info.go, go.mod)

# Build the executable
go build -o sitemerge .
```

### Install Dependencies
```bash
go mod tidy
```

## Usage

### Global Flags
All commands require the following connection parameters:

```bash
--host string       TiDB host address (required)
--port int          TiDB port number (default: 4000)
--user string       TiDB username (required)  
--password string   TiDB password (required)
--database string   Target database name (required)
```

### Command 1: Table Index Information Collection

Analyzes tables in the target database and stores index information:

```bash
./sitemerge table-index --host 127.0.0.1 --port 4000 --user root --password mypass --database mydb
```

#### Options:
- `--keep-existing`: Keep existing data (do not clear before inserting)

#### What it does:
1. Creates the `sitemerge` database if it doesn't exist
2. Creates the `sitemerge.sitemerge_table_index_info` table
3. Analyzes all tables in the target database
4. Collects information about:
   - Table names and databases
   - Clustered index status
   - Primary key columns
   - Table row counts
5. Stores the metadata in the sitemerge database

### Command 2: Page Information Generation

Generates pagination information for efficient data processing:

```bash
./sitemerge page-info --host 127.0.0.1 --port 4000 --user root --password mypass --database mydb
```

#### Options:
- `--page-size int`: Number of rows per page (default: 500)

#### What it does:
1. Reads table information from `sitemerge.sitemerge_table_index_info`
2. Creates `sitemerge.page_info` and `sitemerge.page_info_progress` tables
3. For each table:
   - Fetches data in batches (10K rows per batch)
   - Generates page boundaries based on clustered columns
   - Creates page records with start/end keys
   - Tracks progress for resume capability
4. Supports automatic resume if interrupted

## Database Schema

### sitemerge.sitemerge_table_index_info
Stores table index information:
```sql
CREATE TABLE sitemerge.sitemerge_table_index_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    site_database VARCHAR(255) NOT NULL,
    site_table VARCHAR(255) NOT NULL,
    clustered_index VARCHAR(10) NOT NULL,
    clustered_columns TEXT,
    com_clusted_index VARCHAR(10) NOT NULL,
    table_rows BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_database_table (site_database, site_table)
);
```

### sitemerge.page_info
Stores pagination information:
```sql
CREATE TABLE sitemerge.page_info (
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

### sitemerge.page_info_progress
Tracks processing progress:
```sql
CREATE TABLE sitemerge.page_info_progress (
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
);
```

## Examples

### Complete Workflow
```bash
# Step 1: Collect table index information
./sitemerge table-index \
  --host 127.0.0.1 \
  --port 4000 \
  --user root \
  --password mypassword \
  --database production_db

# Step 2: Generate page information
./sitemerge page-info \
  --host 127.0.0.1 \
  --port 4000 \
  --user root \
  --password mypassword \
  --database production_db \
  --page-size 1000
```

### Resume Interrupted Processing
If the page-info command is interrupted, simply run it again - it will automatically resume from where it left off:

```bash
./sitemerge page-info --host 127.0.0.1 --port 4000 --user root --password mypass --database mydb
# Output: 🔄 Found interrupted processing for table `mydb`.`large_table`
# Output: 📍 Resuming from key 150000 with 75000 rows already processed
```

## Performance Considerations

- **Batch Size**: Fixed at 10,000 rows per batch for optimal performance
- **Page Size**: Configurable (default: 500 rows per page)
- **Memory Usage**: Batched processing minimizes memory footprint
- **Resume Capability**: Prevents data loss on interruption
- **TiKV Hints**: Uses storage hints for optimal query performance

## Error Handling

The tool provides comprehensive error handling:
- Database connection failures
- SQL execution errors
- Transaction rollbacks on failure
- Progress tracking for resume capability
- Detailed error messages with context

## Migration from Python Scripts

This Go implementation replaces the original Python scripts:
- `generate_table_index_info.py` → `sitemerge table-index`
- `generate_page_info.py` → `sitemerge page-info`

### Key Improvements:
- **Performance**: Native Go performance vs Python
- **Single Binary**: No dependency management issues
- **Better Error Handling**: Comprehensive error reporting
- **Improved CLI**: Modern CLI interface with Cobra
- **Enhanced Resume**: More robust resume capability

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 