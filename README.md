# SiteMerge - TiDB Table Management Tool

SiteMerge is a powerful command-line tool written in Go that provides comprehensive table management capabilities for TiDB databases. It offers four main functionalities: collecting table index information from target databases, generating pagination information for efficient data processing, exporting data from TiDB databases, and importing data to TiDB databases.

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

### 📤 Data Export (`export` command)
- Exports data from TiDB database using page-based processing
- Reads page_info for efficient sharding and parallel processing
- Maintains export status in the sitemerge.export_import_summary table
- Supports resume capability for interrupted exports
- Configurable thread count for parallel processing
- Option to export specific tables or all tables

### 📥 Data Import (`import` command)
- Imports data to TiDB database using page-based processing
- Reads page_info for efficient sharding and parallel processing
- Maintains import status in the sitemerge.export_import_summary table
- Supports resume capability for interrupted imports
- Configurable thread count for parallel processing
- Option to import specific tables or all tables
- Connects to both source and destination databases

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
- `--threads int`: Number of worker threads (1-512, default: 1)

#### What it does:
1. Reads table information from `sitemerge.sitemerge_table_index_info`
2. Creates `sitemerge.page_info` and `sitemerge.page_info_progress` tables
3. For each table:
   - Fetches data in batches (10K rows per batch)
   - Generates page boundaries based on clustered columns
   - Creates page records with start/end keys
   - Tracks progress for resume capability
4. Supports automatic resume if interrupted

### Command 3: Data Export

Exports data from TiDB database using page-based processing:

```bash
./sitemerge export --host 127.0.0.1 --port 4000 --user root --password mypass --database mydb
```

#### Options:
- `--threads int`: Number of worker threads (1-512, default: 8)
- `--table-name string`: Specific table name to process (default: all tables)

#### What it does:
1. Reads page information from `sitemerge.page_info`
2. Creates `sitemerge.export_import_summary` table for tracking export status
3. For each table (or specific table if specified):
   - Processes data in parallel using multiple threads
   - Exports data based on page boundaries
   - Maintains export progress and status
   - Supports resume capability for interrupted exports
4. Provides detailed progress reporting

### Command 4: Data Import

Imports data to TiDB database using page-based processing:

```bash
./sitemerge import --host 127.0.0.1 --port 4000 --user root --password mypass --database sourcedb \
  --dst-host 127.0.0.1 --dst-port 4000 --dst-user root --dst-password mypass --dst-database destdb
```

#### Options:
- `--threads int`: Number of worker threads (1-512, default: 8)
- `--table-name string`: Specific table name to process (default: all tables)
- `--dst-host string`: Destination TiDB host address (required)
- `--dst-port int`: Destination TiDB port number (default: 4000)
- `--dst-user string`: Destination TiDB username (required)
- `--dst-password string`: Destination TiDB password (required)
- `--dst-database string`: Destination database name (required)

#### What it does:
1. Connects to both source and destination databases
2. Reads page information from `sitemerge.page_info`
3. Creates `sitemerge.export_import_summary` table for tracking import status
4. For each table (or specific table if specified):
   - Processes data in parallel using multiple threads
   - Imports data based on page boundaries
   - Maintains import progress and status
   - Supports resume capability for interrupted imports
5. Provides detailed progress reporting

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

### sitemerge.export_import_summary
Tracks export and import operations:
```sql
CREATE TABLE sitemerge.export_import_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    site_database VARCHAR(255),
    site_table VARCHAR(255),
    operation_type ENUM('export', 'import') NOT NULL,
    status ENUM('processing', 'completed', 'failed') DEFAULT 'processing',
    total_pages INT DEFAULT 0,
    processed_pages INT DEFAULT 0,
    total_rows BIGINT DEFAULT 0,
    processed_rows BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY site_table_operation(site_database, site_table, operation_type)
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
  --page-size 1000 \
  --threads 4

# Step 3: Export data from source database
./sitemerge export \
  --host 127.0.0.1 \
  --port 4000 \
  --user root \
  --password mypassword \
  --database production_db \
  --threads 8

# Step 4: Import data to destination database
./sitemerge import \
  --host 127.0.0.1 \
  --port 4000 \
  --user root \
  --password mypassword \
  --database production_db \
  --dst-host 127.0.0.1 \
  --dst-port 4000 \
  --dst-user root \
  --dst-password mypassword \
  --dst-database staging_db \
  --threads 8
```

### Export/Import Specific Tables
```bash
# Export only specific table
./sitemerge export \
  --host 127.0.0.1 \
  --port 4000 \
  --user root \
  --password mypass \
  --database mydb \
  --table-name users \
  --threads 16

# Import only specific table
./sitemerge import \
  --host 127.0.0.1 \
  --port 4000 \
  --user root \
  --password mypass \
  --database sourcedb \
  --dst-host 127.0.0.1 \
  --dst-port 4000 \
  --dst-user root \
  --dst-password mypass \
  --dst-database destdb \
  --table-name users \
  --threads 16
```

### Resume Interrupted Processing
If any command is interrupted, simply run it again - it will automatically resume from where it left off:

```bash
# Resume interrupted page-info generation
./sitemerge page-info --host 127.0.0.1 --port 4000 --user root --password mypass --database mydb
# Output: 🔄 Found interrupted processing for table `mydb`.`large_table`
# Output: 📍 Resuming from key 150000 with 75000 rows already processed

# Resume interrupted export
./sitemerge export --host 127.0.0.1 --port 4000 --user root --password mypass --database mydb
# Output: 🔄 Found interrupted export for table `mydb`.`users`
# Output: 📍 Resuming from page 45 with 22500 rows already exported

# Resume interrupted import
./sitemerge import --host 127.0.0.1 --port 4000 --user root --password mypass --database sourcedb \
  --dst-host 127.0.0.1 --dst-port 4000 --dst-user root --dst-password mypass --dst-database destdb
# Output: 🔄 Found interrupted import for table `sourcedb`.`users`
# Output: 📍 Resuming from page 45 with 22500 rows already imported
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
- `export_data.py` → `sitemerge export`
- `import_data.py` → `sitemerge import`

### Key Improvements:
- **Performance**: Native Go performance vs Python
- **Single Binary**: No dependency management issues
- **Better Error Handling**: Comprehensive error reporting
- **Improved CLI**: Modern CLI interface with Cobra
- **Enhanced Resume**: More robust resume capability
- **Parallel Processing**: Multi-threaded export/import operations
- **Unified Interface**: Single tool for all operations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 