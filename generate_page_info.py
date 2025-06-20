#!/usr/bin/env python3
"""
SiteMerge - TiDB Page Information Generator

This program connects to TiDB, creates a page_info table to store pagination information,
and populates it with data from tables listed in sitemerge_table_index_info.
"""

import pymysql
import argparse
import sys
import time
from typing import Optional, List, Tuple

# Magic value for the page info table name
PAGE_INFO_TABLE_NAME = "sitemerge.page_info"
SITEMERGE_TABLE_NAME = "sitemerge.sitemerge_table_index_info"
PROGRESS_TABLE_NAME = "sitemerge.page_info_progress"

# Create page_info table DDL
CREATE_PAGE_INFO_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PAGE_INFO_TABLE_NAME} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    site_database VARCHAR(255),
    site_table VARCHAR(255),
    page_num INT,
    start_key BIGINT,
    end_key BIGINT,
    page_size INT,
    UNIQUE KEY site_table_page_num(site_database, site_table, page_num)
)
"""

# Create progress tracking table DDL
CREATE_PROGRESS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE_NAME} (
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
)
"""

# Query to get table information from sitemerge_table_index_info
GET_TABLES_SQL = f"""
SELECT site_database, site_table, clustered_columns, table_rows
FROM {SITEMERGE_TABLE_NAME}
ORDER BY site_database, site_table
"""

class TiDBConnection:
    """TiDB connection manager"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection: Optional[pymysql.Connection] = None
    
    def connect(self) -> None:
        """Establish connection to TiDB"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                autocommit=False
            )
            print(f"✅ Successfully connected to TiDB at {self.host}:{self.port}")
        except Exception as e:
            print(f"❌ Error connecting to TiDB: {e}")
            sys.exit(1)
    
    def close(self) -> None:
        """Close TiDB connection"""
        if self.connection:
            self.connection.close()
            print("🔌 Connection closed")

class PageInfoGenerator:
    """Main manager for page information generation"""
    
    def __init__(self, db_connection: TiDBConnection, page_size: int = 500):
        self.db_connection = db_connection
        self.page_size = page_size
        self.batch_size = 10000  # Process 10K rows per batch
    
    def create_page_info_table(self) -> None:
        """Create the page_info table if it doesn't exist"""
        try:
            with self.db_connection.connection.cursor() as cursor:
                cursor.execute(CREATE_PAGE_INFO_TABLE_SQL)
                cursor.execute(CREATE_PROGRESS_TABLE_SQL)
                self.db_connection.connection.commit()
                print(f"📋 Page info table '{PAGE_INFO_TABLE_NAME}' created or already exists")
                print(f"📋 Progress table '{PROGRESS_TABLE_NAME}' created or already exists")
        except Exception as e:
            print(f"❌ Error creating tables: {e}")
            sys.exit(1)
    
    def get_tables_to_process(self) -> List[Tuple]:
        """Get list of tables to process from sitemerge_table_index_info"""
        try:
            with self.db_connection.connection.cursor() as cursor:
                cursor.execute(GET_TABLES_SQL)
                results = cursor.fetchall()
                print(f"📊 Found {len(results)} tables to process")
                return results
        except Exception as e:
            print(f"❌ Error getting tables to process: {e}")
            sys.exit(1)
    
    def clear_existing_page_info(self, db_name: str, table_name: str) -> None:
        """Clear existing page info for the specified table"""
        try:
            with self.db_connection.connection.cursor() as cursor:
                delete_sql = f"DELETE FROM {PAGE_INFO_TABLE_NAME} WHERE site_database = %s AND site_table = %s"
                cursor.execute(delete_sql, (db_name, table_name))
                deleted_rows = cursor.rowcount
                self.db_connection.connection.commit()
                if deleted_rows > 0:
                    print(f"  🧹 Cleared {deleted_rows} existing page records")
        except Exception as e:
            print(f"❌ Error clearing existing page info: {e}")
            sys.exit(1)

    def get_progress_status(self, db_name: str, table_name: str) -> Tuple[str, int, int]:
        """Get progress status for a table. Returns (status, last_end_key, processed_rows)"""
        try:
            with self.db_connection.connection.cursor() as cursor:
                query_sql = f"""
                SELECT status, last_end_key, processed_rows 
                FROM {PROGRESS_TABLE_NAME} 
                WHERE site_database = %s AND site_table = %s
                """
                cursor.execute(query_sql, (db_name, table_name))
                result = cursor.fetchone()
                
                if result:
                    return result[0], result[1], result[2]
                else:
                    return 'new', 0, 0
        except Exception as e:
            print(f"❌ Error getting progress status: {e}")
            return 'new', 0, 0

    def update_progress(self, db_name: str, table_name: str, status: str, 
                       last_end_key: int = 0, total_rows: int = 0, processed_rows: int = 0) -> None:
        """Update progress for a table"""
        try:
            with self.db_connection.connection.cursor() as cursor:
                upsert_sql = f"""
                INSERT INTO {PROGRESS_TABLE_NAME} 
                (site_database, site_table, status, last_end_key, total_rows, processed_rows)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                status = VALUES(status),
                last_end_key = VALUES(last_end_key),
                total_rows = VALUES(total_rows),
                processed_rows = VALUES(processed_rows),
                updated_at = CURRENT_TIMESTAMP
                """
                cursor.execute(upsert_sql, (db_name, table_name, status, last_end_key, total_rows, processed_rows))
                self.db_connection.connection.commit()
        except Exception as e:
            print(f"❌ Error updating progress: {e}")

    def get_resume_point(self) -> Optional[Tuple[str, str, str, int, int, int]]:
        """Get the resume point from last interrupted processing"""
        try:
            with self.db_connection.connection.cursor() as cursor:
                # Look for tables that are in 'processing' status
                query_sql = f"""
                SELECT p.site_database, p.site_table, s.clustered_columns, 
                       s.table_rows, p.last_end_key, p.processed_rows
                FROM {PROGRESS_TABLE_NAME} p
                JOIN {SITEMERGE_TABLE_NAME} s ON p.site_database = s.site_database AND p.site_table = s.site_table
                WHERE p.status = 'processing'
                ORDER BY p.updated_at ASC
                LIMIT 1
                """
                cursor.execute(query_sql)
                result = cursor.fetchone()
                
                if result:
                    return result
                else:
                    return None
        except Exception as e:
            print(f"❌ Error getting resume point: {e}")
            return None
    
    def generate_page_info_batch(self, db_name: str, table_name: str, divide_column: str, 
                                last_max_id: Optional[int] = None) -> Tuple[List[int], int]:
        """Generate raw data for a batch, return list of divide_column values and max_end_key"""
        
        # Determine which SQL template to use
        if last_max_id is None:
            # First batch - no WHERE condition
            sql_template = f"""
            SELECT /*+ READ_FROM_STORAGE(TIKV) */ {divide_column}
            FROM `{db_name}`.`{table_name}`
            ORDER BY {divide_column}
            LIMIT {self.batch_size}
            """
        else:
            # Subsequent batches - with WHERE condition
            sql_template = f"""
            SELECT /*+ READ_FROM_STORAGE(TIKV) */ {divide_column}
            FROM `{db_name}`.`{table_name}`
            WHERE {divide_column} > {last_max_id}
            ORDER BY {divide_column}
            LIMIT {self.batch_size}
            """
        
        try:
            with self.db_connection.connection.cursor() as cursor:
                cursor.execute(sql_template)
                results = cursor.fetchall()
                
                # Extract values from tuples
                values = [row[0] for row in results] if results else []
                max_end_key = values[-1] if values else 0
                
                return values, max_end_key
                
        except Exception as e:
            print(f"❌ Error generating page info batch: {e}")
            return [], 0
    
    def insert_page_info_batch(self, db_name: str, table_name: str, page_data: List[Tuple]) -> None:
        """Insert a batch of page information into the page_info table"""
        if not page_data:
            return
        
        insert_sql = f"""
        INSERT INTO {PAGE_INFO_TABLE_NAME} 
        (site_database, site_table, page_num, start_key, end_key, page_size)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        # Prepare data for insertion (add db_name and table_name to each row)
        insert_data = []
        for row in page_data:
            page_num, start_key, end_key, page_size = row
            insert_data.append((db_name, table_name, page_num, start_key, end_key, page_size))
        
        try:
            with self.db_connection.connection.cursor() as cursor:
                cursor.executemany(insert_sql, insert_data)
                self.db_connection.connection.commit()
        except Exception as e:
            print(f"❌ Error inserting page info batch: {e}")
            self.db_connection.connection.rollback()
            raise
    
    def process_single_table(self, db_name: str, table_name: str, divide_column: str, table_rows: int, 
                           resume_from_key: int = 0, existing_processed_rows: int = 0) -> int:
        """Process a single table to generate page information with resume support"""
        
        if resume_from_key > 0:
            print(f"  🔄 Resuming table `{db_name}`.`{table_name}` from key {resume_from_key} (already processed {existing_processed_rows:,} rows)")
        else:
            print(f"  📊 Processing table `{db_name}`.`{table_name}` ({table_rows:,} rows)")
            # Clear existing page info for this table if starting fresh
            self.clear_existing_page_info(db_name, table_name)
        
        # Update progress status to 'processing'
        self.update_progress(db_name, table_name, 'processing', resume_from_key, table_rows, existing_processed_rows)
        
        total_pages = 0
        last_max_id = resume_from_key if resume_from_key > 0 else None
        batch_count = 0
        all_values = []  # Store all values across batches
        processed_rows = existing_processed_rows
        
        # First, collect all data in batches
        while True:
            # Generate raw data for current batch
            batch_values, max_end_key = self.generate_page_info_batch(
                db_name, table_name, divide_column, last_max_id
            )
            
            if not batch_values:
                break
            
            all_values.extend(batch_values)
            batch_count += 1
            processed_rows += len(batch_values)
            
            print(f"    📥 Fetched batch {batch_count}: {len(batch_values)} rows (total fetched: {len(all_values)}, total processed: {processed_rows:,})")
            
            # Update progress periodically
            self.update_progress(db_name, table_name, 'processing', max_end_key, table_rows, processed_rows)
            
            # Check if we've processed all data
            if len(batch_values) < self.batch_size:
                break
            
            # Update last_max_id for next iteration
            last_max_id = max_end_key
        
        # Now process all values to generate pages
        if all_values:
            # Calculate starting page number based on existing processed rows
            start_page_num = (existing_processed_rows // self.page_size) + 1
            
            page_data = []
            for i in range(0, len(all_values), self.page_size):
                page_chunk = all_values[i:i + self.page_size]
                page_num = start_page_num + (i // self.page_size)
                start_key = page_chunk[0]
                end_key = page_chunk[-1]
                actual_page_size = len(page_chunk)
                
                page_data.append((page_num, start_key, end_key, actual_page_size))
            
            # Insert all page info at once
            self.insert_page_info_batch(db_name, table_name, page_data)
            total_pages = len(page_data)
            
            print(f"    📄 Generated {total_pages} pages from {len(all_values)} rows")
        
        # Mark as completed
        self.update_progress(db_name, table_name, 'completed', 0, table_rows, processed_rows)
        
        return total_pages
    
    def run(self) -> None:
        """Main execution flow with resume support"""
        start_time = time.time()
        print("🚀 Starting SiteMerge Page Info Generation Process")
        print(f"📏 Page size: {self.page_size}")
        print(f"📦 Batch size: {self.batch_size:,}")
        print()
        
        # Step 1: Create page_info table
        self.create_page_info_table()
        
        # Step 2: Check for resume point
        resume_point = self.get_resume_point()
        if resume_point:
            db_name, table_name, divide_column, table_rows, last_end_key, processed_rows = resume_point
            print(f"🔄 Found interrupted processing for table `{db_name}`.`{table_name}`")
            print(f"📍 Resuming from key {last_end_key} with {processed_rows:,} rows already processed")
            print()
            
            # Process the interrupted table
            try:
                pages_generated = self.process_single_table(
                    db_name, table_name, divide_column, table_rows, last_end_key, processed_rows
                )
                print(f"  ✅ Resumed table completed: {pages_generated} pages generated")
                print()
                
            except Exception as e:
                print(f"  ❌ Failed to resume table `{db_name}`.`{table_name}`: {e}")
                # Mark as failed
                self.update_progress(db_name, table_name, 'failed')
                print()
        
        # Step 3: Get remaining tables to process
        tables = self.get_tables_to_process()
        
        if not tables:
            print("⚠️  No tables found to process")
            return
        
        # Filter out already completed tables
        remaining_tables = []
        for db_name, table_name, divide_column, table_rows in tables:
            status, _, _ = self.get_progress_status(db_name, table_name)
            if status not in ['completed']:
                remaining_tables.append((db_name, table_name, divide_column, table_rows))
        
        if not remaining_tables:
            print("✅ All tables have been processed!")
            return
        
        print(f"📋 Found {len(remaining_tables)} remaining tables to process")
        print()
        
        # Step 4: Process remaining tables
        total_pages_generated = 0
        processed_tables = 0
        
        for db_name, table_name, divide_column, table_rows in remaining_tables:
            try:
                pages_generated = self.process_single_table(
                    db_name, table_name, divide_column, table_rows
                )
                total_pages_generated += pages_generated
                processed_tables += 1
                print(f"  ✅ Completed: {pages_generated} pages generated")
                print()
                
            except Exception as e:
                print(f"  ❌ Failed to process table `{db_name}`.`{table_name}`: {e}")
                # Mark as failed
                self.update_progress(db_name, table_name, 'failed')
                print()
                continue
        
        # Step 5: Print summary
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print("="*60)
        print("📊 PROCESSING SUMMARY")
        print("="*60)
        print(f"✅ Tables processed: {processed_tables}/{len(remaining_tables)}")
        print(f"📄 Total pages generated: {total_pages_generated:,}")
        print(f"⏱️  Total time elapsed: {elapsed_time:.2f} seconds")
        print(f"🏁 Processing completed successfully!")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="SiteMerge - TiDB Page Information Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python generate_page_info.py --host 127.0.0.1 --port 4000 --user root --password mypass --database test
  python generate_page_info.py --host 127.0.0.1 --port 4000 --user root --password mypass --database test --page-size 1000
        """
    )
    
    parser.add_argument('--host', required=True, help='TiDB host address')
    parser.add_argument('--port', type=int, required=True, help='TiDB port number')
    parser.add_argument('--user', required=True, help='TiDB username')
    parser.add_argument('--password', required=True, help='TiDB password')
    parser.add_argument('--database', required=True, help='TiDB database name to connect to')
    parser.add_argument('--page-size', type=int, default=500, 
                       help='Number of rows per page (default: 500)')
    
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Create database connection
    db_conn = TiDBConnection(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database
    )
    
    try:
        # Connect to database  
        db_conn.connect()
        
        # Create and run page info generator
        generator = PageInfoGenerator(db_conn, args.page_size)
        generator.run()
        
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
    finally:
        db_conn.close()

if __name__ == "__main__":
    main() 