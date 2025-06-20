#!/usr/bin/env python3
"""
SiteMerge - TiDB Table Index Information Manager

This program connects to TiDB, creates a table to store index information,
and populates it with data from the target database.
"""

import pymysql
import argparse
import sys
import os
from typing import Optional

# Magic value for the target table name (configurable)
SITEMERGE_TABLE_NAME = "sitemerge.sitemerge_table_index_info"

# SQL file paths
SQL_QUERY_FILE = "sql/table_index_query.sql"
PREPARE_SQL_FILE = "sql/prepare.sql"

# Create table DDL
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SITEMERGE_TABLE_NAME} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    site_database VARCHAR(255) NOT NULL,
    site_table VARCHAR(255) NOT NULL,
    clustered_index VARCHAR(10) NOT NULL,
    clustered_columns TEXT,
    com_clusted_index VARCHAR(10) NOT NULL,
    table_rows BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_database_table (site_database, site_table)
)
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
            print(f"Successfully connected to TiDB at {self.host}:{self.port}")
        except Exception as e:
            print(f"Error connecting to TiDB: {e}")
            sys.exit(1)
    
    def close(self) -> None:
        """Close TiDB connection"""
        if self.connection:
            self.connection.close()
            print("Connection closed")

class SiteMergeManager:
    """Main manager for site merge operations"""
    
    def __init__(self, db_connection: TiDBConnection):
        self.db_connection = db_connection
    
    def create_target_table(self) -> None:
        """Create the sitemerge_table_index_info table if it doesn't exist"""
        try:
            with self.db_connection.connection.cursor() as cursor:
                cursor.execute(CREATE_TABLE_SQL)
                self.db_connection.connection.commit()
                print(f"Table '{SITEMERGE_TABLE_NAME}' created or already exists")
        except Exception as e:
            print(f"Error creating table: {e}")
            sys.exit(1)
    
    def load_sql_query(self) -> str:
        """Load the predefined SQL query from file"""
        try:
            with open(SQL_QUERY_FILE, 'r', encoding='utf-8') as file:
                return file.read().strip()
        except FileNotFoundError:
            print(f"Error: SQL file '{SQL_QUERY_FILE}' not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error reading SQL file: {e}")
            sys.exit(1)
    
    def execute_prepare_sql(self) -> None:
        """Execute the prepare.sql script to initialize sitemerge database"""
        try:
            with open(PREPARE_SQL_FILE, 'r', encoding='utf-8') as file:
                prepare_sql = file.read().strip()
            
            # Split SQL statements by semicolon and execute each one
            statements = [stmt.strip() for stmt in prepare_sql.split(';') if stmt.strip()]
            
            with self.db_connection.connection.cursor() as cursor:
                for statement in statements:
                    if statement:
                        cursor.execute(statement)
                        result = cursor.fetchall()
                        if result:
                            print(f"Prepare SQL result: {result}")
                
                self.db_connection.connection.commit()
                print("Database preparation completed successfully")
                
        except FileNotFoundError:
            print(f"Warning: Prepare SQL file '{PREPARE_SQL_FILE}' not found, skipping database preparation")
        except Exception as e:
            print(f"Error executing prepare SQL: {e}")
            sys.exit(1)
    
    def execute_index_query(self, target_database: str) -> list:
        """Execute the index information query"""
        sql_query = self.load_sql_query()
        
        try:
            with self.db_connection.connection.cursor() as cursor:
                cursor.execute(sql_query, (target_database,))
                results = cursor.fetchall()
                print(f"Retrieved {len(results)} records from database '{target_database}'")
                return results
        except Exception as e:
            print(f"Error executing query: {e}")
            sys.exit(1)
    
    def insert_results(self, results: list) -> None:
        """Insert query results into the target table"""
        if not results:
            print("No data to insert")
            return
        
        # Process results to modify ClusteredColumns for non-clustered indexes
        processed_results = []
        for result in results:
            # Convert tuple to list for modification
            result_list = list(result)
            
            # Check if ClusteredIndex is 'No' (index 2 in the tuple)
            if len(result_list) >= 4 and result_list[2] == 'No':
                # Set ClusteredColumns to '_tidb_rowid' (index 3 in the tuple)
                result_list[3] = '_tidb_rowid'
                print(f"Modified ClusteredColumns for table {result_list[1]}: {result_list[2]} -> _tidb_rowid")
            
            processed_results.append(tuple(result_list))
        
        insert_sql = f"""
        INSERT INTO {SITEMERGE_TABLE_NAME} 
        (site_database, site_table, clustered_index, clustered_columns, com_clusted_index, table_rows)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self.db_connection.connection.cursor() as cursor:
                cursor.executemany(insert_sql, processed_results)
                self.db_connection.connection.commit()
                print(f"Successfully inserted {len(processed_results)} records into '{SITEMERGE_TABLE_NAME}'")
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.db_connection.connection.rollback()
            sys.exit(1)
    
    def clear_existing_data(self, target_database: str) -> None:
        """Clear existing data for the target database"""
        try:
            with self.db_connection.connection.cursor() as cursor:
                delete_sql = f"DELETE FROM {SITEMERGE_TABLE_NAME} WHERE site_database = %s"
                cursor.execute(delete_sql, (target_database,))
                deleted_rows = cursor.rowcount
                self.db_connection.connection.commit()
                print(f"Cleared {deleted_rows} existing records for database '{target_database}'")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            sys.exit(1)
    
    def run(self, target_database: str, clear_existing: bool = True) -> None:
        """Main execution flow"""
        print(f"Starting SiteMerge process for database: {target_database}")
        print(f"Target table: {SITEMERGE_TABLE_NAME}")
        
        # Step 0: Initialize sitemerge database
        self.execute_prepare_sql()
        
        # Step 1: Create target table
        self.create_target_table()
        
        # Step 2: Clear existing data (optional)
        if clear_existing:
            self.clear_existing_data(target_database)
        
        # Step 3: Execute query
        results = self.execute_index_query(target_database)
        
        # Step 4: Insert results
        self.insert_results(results)
        
        print("SiteMerge process completed successfully")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="SiteMerge - TiDB Table Index Information Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python sitemerge.py --host 127.0.0.1 --port 4000 --user root --password mypass --database test
        """
    )
    
    parser.add_argument('--host', required=True, help='TiDB host address')
    parser.add_argument('--port', type=int, required=True, help='TiDB port number')
    parser.add_argument('--user', required=True, help='TiDB username')
    parser.add_argument('--password', required=True, help='TiDB password')
    parser.add_argument('--database', required=True, help='Target database name to analyze')
    parser.add_argument('--keep-existing', action='store_true', 
                       help='Keep existing data (do not clear before inserting)')
    
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
        
        # Create and run site merge manager
        manager = SiteMergeManager(db_conn)
        manager.run(args.database, clear_existing=not args.keep_existing)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        db_conn.close()

if __name__ == "__main__":
    main() 