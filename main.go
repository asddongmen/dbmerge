package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

// DBConfig holds database connection configuration
type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// GetDSN returns the MySQL DSN string
func (c *DBConfig) GetDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		c.User, c.Password, c.Host, c.Port, c.Database)
}

var (
	dbConfig              DBConfig
	dstDbConfig           DBConfig
	pageSize              int
	threads               int
	tableName             string
	skipCheckExportStatus bool
)

var rootCmd = &cobra.Command{
	Use:   "sitemerge",
	Short: "SiteMerge - TiDB Table Management Tool",
	Long: `SiteMerge is a CLI tool for managing TiDB table information and generating page info.
It provides three main functionalities:
1. Collecting table index information from target databases
2. Generating page information for data processing
3. Exporting and importing data between TiDB databases`,
}

var tableIndexCmd = &cobra.Command{
	Use:   "table-index",
	Short: "Generate table index information",
	Long: `Analyze target database tables and collect index information.
This command creates a sitemerge database and populates it with table metadata
including clustered index information and table row counts.`,
	Run: runTableIndex,
}

var pageInfoCmd = &cobra.Command{
	Use:   "page-info",
	Short: "Generate page information for tables",
	Long: `Generate pagination information for tables based on their index information.
This command reads from the sitemerge_table_index_info table and creates
page_info records for efficient data processing with resume capability.`,
	Run: runPageInfo,
}

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export data from TiDB database",
	Long: `Export data from TiDB database using page-based processing.
This command reads page_info for sharding and maintains export status
in the sitemerge.export_import_summary table with resume capability.`,
	Run: runExport,
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data to TiDB database",
	Long: `Import data to TiDB database using page-based processing.
This command reads page_info for sharding and maintains import status
in the sitemerge.export_import_summary table with resume capability.`,
	Run: runImport,
}

var truncateTableCmd = &cobra.Command{
	Use:   "truncate-table",
	Short: "Truncate all tables in target database",
	Long: `Truncate all tables in the target database.
This command will remove all data from all tables in the specified database.
WARNING: This operation is irreversible and will delete all data.`,
	Run: runTruncateTable,
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&dbConfig.Host, "host", "", "TiDB host address (required)")
	rootCmd.PersistentFlags().IntVar(&dbConfig.Port, "port", 4000, "TiDB port number")
	rootCmd.PersistentFlags().StringVar(&dbConfig.User, "user", "", "TiDB username (required)")
	rootCmd.PersistentFlags().StringVar(&dbConfig.Password, "password", "", "TiDB password (required)")
	rootCmd.PersistentFlags().StringVar(&dbConfig.Database, "database", "", "Target database name (required)")

	// Mark required flags
	rootCmd.MarkPersistentFlagRequired("host")
	rootCmd.MarkPersistentFlagRequired("user")
	rootCmd.MarkPersistentFlagRequired("password")
	rootCmd.MarkPersistentFlagRequired("database")

	// Subcommand specific flags
	tableIndexCmd.Flags().Bool("keep-existing", false, "Keep existing data (do not clear before inserting)")
	pageInfoCmd.Flags().IntVar(&pageSize, "page-size", 500, "Number of rows per page")
	pageInfoCmd.Flags().IntVar(&threads, "threads", 1, "Number of worker threads (1-512, default: 1)")

	exportCmd.Flags().IntVar(&threads, "threads", 8, "Number of worker threads (1-512, default: 8)")
	exportCmd.Flags().StringVar(&tableName, "table-name", "", "Specific table name to process (default: all tables)")

	// Import command flags
	importCmd.Flags().StringVar(&dstDbConfig.Host, "dst-host", "", "Destination TiDB host address (required)")
	importCmd.Flags().IntVar(&dstDbConfig.Port, "dst-port", 4000, "Destination TiDB port number")
	importCmd.Flags().StringVar(&dstDbConfig.User, "dst-user", "", "Destination TiDB username (required)")
	importCmd.Flags().StringVar(&dstDbConfig.Password, "dst-password", "", "Destination TiDB password (required)")
	importCmd.Flags().StringVar(&dstDbConfig.Database, "dst-database", "", "Destination database name (required)")
	importCmd.Flags().IntVar(&threads, "threads", 8, "Number of worker threads (1-512, default: 8)")
	importCmd.Flags().StringVar(&tableName, "table-name", "", "Specific table name to process (default: all tables)")
	importCmd.Flags().BoolVar(&skipCheckExportStatus, "skip-check-export-status", false, "Skip checking export status")

	// Mark required flags for import command
	importCmd.MarkFlagRequired("dst-host")
	importCmd.MarkFlagRequired("dst-user")
	importCmd.MarkFlagRequired("dst-password")
	importCmd.MarkFlagRequired("dst-database")

	// truncate-table command flags
	truncateTableCmd.Flags().Bool("force", false, "Skip confirmation prompt")

	// Add subcommands
	rootCmd.AddCommand(tableIndexCmd)
	rootCmd.AddCommand(pageInfoCmd)
	rootCmd.AddCommand(exportCmd)
	rootCmd.AddCommand(importCmd)
	rootCmd.AddCommand(truncateTableCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func connectDB(maxConnections int) (*sql.DB, error) {
	db, err := sql.Open("mysql", dbConfig.GetDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConnections)
	db.SetMaxIdleConns(maxConnections / 2)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Printf("✅ Successfully connected to TiDB at %s:%d (Max connections: %d)\n",
		dbConfig.Host, dbConfig.Port, maxConnections)
	return db, nil
}

func connectDestDB(maxConnections int) (*sql.DB, error) {
	db, err := sql.Open("mysql", dstDbConfig.GetDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open destination database connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConnections)
	db.SetMaxIdleConns(maxConnections / 2)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping destination database: %w", err)
	}

	log.Printf("✅ Successfully connected to destination TiDB at %s:%d (Max connections: %d)\n",
		dstDbConfig.Host, dstDbConfig.Port, maxConnections)
	return db, nil
}

func runTableIndex(cmd *cobra.Command, args []string) {
	keepExisting, _ := cmd.Flags().GetBool("keep-existing")

	db, err := connectDB(10) // Use fixed connections for table-index command
	if err != nil {
		log.Printf("❌ %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	manager := NewTableIndexManager(db)
	if err := manager.Run(dbConfig.Database, !keepExisting); err != nil {
		log.Printf("❌ %v\n", err)
		os.Exit(1)
	}
}

func runPageInfo(cmd *cobra.Command, args []string) {
	// Validate threads parameter
	if threads < 1 || threads > 512 {
		log.Printf("❌ Thread count must be between 1 and 512, got: %d\n", threads)
		os.Exit(1)
	}

	// Connect to database with connection pool sized for threads
	maxConnections := threads * 2 // Allow 2 connections per thread
	db, err := connectDB(maxConnections)
	if err != nil {
		log.Printf("❌ %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	generator := NewPageInfoGenerator(db, pageSize, threads, dbConfig.Database)
	if err := generator.Run(); err != nil {
		log.Printf("❌ %v\n", err)
		os.Exit(1)
	}
}

func runExport(cmd *cobra.Command, args []string) {
	// Validate threads parameter
	if threads < 1 || threads > 512 {
		log.Printf("❌ Thread count must be between 1 and 512, got: %d\n", threads)
		os.Exit(1)
	}

	// Connect to source database
	maxConnections := threads * 3 // Allow more connections for export/import operations
	sourceDB, err := connectDB(maxConnections)
	if err != nil {
		log.Printf("❌ Failed to connect to source database: %v\n", err)
		os.Exit(1)
	}
	defer sourceDB.Close()

	// Create and run export manager
	manager := NewExportManager(sourceDB, threads, tableName)
	if err := manager.Run(); err != nil {
		log.Printf("❌ %v\n", err)
		os.Exit(1)
	}
}

func runImport(cmd *cobra.Command, args []string) {
	// Validate threads parameter
	if threads < 1 || threads > 512 {
		log.Printf("❌ Thread count must be between 1 and 512, got: %d\n", threads)
		os.Exit(1)
	}

	// Connect to source database
	maxConnections := threads * 3 // Allow more connections for export/import operations
	sourceDB, err := connectDB(maxConnections)
	if err != nil {
		log.Printf("❌ Failed to connect to source database: %v\n", err)
		os.Exit(1)
	}
	defer sourceDB.Close()

	// Connect to destination database
	log.Printf("🔗 Connecting to destination database...\n")
	destDB, err := connectDestDB(maxConnections)
	if err != nil {
		log.Printf("❌ Failed to connect to destination database: %v\n", err)
		os.Exit(1)
	}
	defer destDB.Close()

	// Create and run import manager
	manager := NewImportManager(sourceDB, destDB, threads, tableName, skipCheckExportStatus)
	if err := manager.Run(); err != nil {
		log.Printf("❌ %v\n", err)
		os.Exit(1)
	}
}

func runTruncateTable(cmd *cobra.Command, args []string) {
	force, _ := cmd.Flags().GetBool("force")

	// Connect to database
	db, err := connectDB(10)
	if err != nil {
		log.Printf("❌ %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := truncateAllTables(db, dbConfig.Database, force); err != nil {
		log.Printf("❌ %v\n", err)
		os.Exit(1)
	}
}

// truncateAllTables truncates all tables in the specified database
func truncateAllTables(db *sql.DB, database string, force bool) error {
	// Get all tables in the database
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = ? 
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := db.Query(query, database)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate tables: %w", err)
	}

	if len(tables) == 0 {
		log.Printf("✅ No tables found in database '%s'\n", database)
		return nil
	}

	log.Printf("📋 Found %d tables in database '%s':\n", len(tables), database)
	for _, table := range tables {
		log.Printf("   - %s\n", table)
	}

	// Ask for confirmation if not forced
	if !force {
		log.Printf("\n⚠️  WARNING: This operation will permanently delete all data from %d tables!\n", len(tables))
		log.Printf("Are you sure you want to proceed? (y/N): ")

		reader := bufio.NewReader(os.Stdin)
		response, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read user input: %w", err)
		}

		response = strings.TrimSpace(strings.ToLower(response))
		if response != "y" && response != "yes" {
			log.Printf("❌ Operation cancelled by user\n")
			return nil
		}
	}

	// Disable foreign key checks to avoid constraint issues
	log.Printf("🔧 Disabling foreign key checks...\n")
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %w", err)
	}

	// Truncate each table
	successCount := 0
	for _, table := range tables {
		log.Printf("🗑️  Truncating table: %s...", table)

		truncateQuery := fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", database, table)
		if _, err := db.Exec(truncateQuery); err != nil {
			log.Printf(" ❌ Failed: %v\n", err)
			continue
		}

		log.Printf(" ✅ Done\n")
		successCount++
	}

	// Re-enable foreign key checks
	log.Printf("🔧 Re-enabling foreign key checks...\n")
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		return fmt.Errorf("failed to re-enable foreign key checks: %w", err)
	}

	log.Printf("✅ Successfully truncated %d/%d tables in database '%s'\n", successCount, len(tables), database)

	if successCount != len(tables) {
		return fmt.Errorf("failed to truncate %d tables", len(tables)-successCount)
	}

	return nil
}
