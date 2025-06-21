package main

import (
	"database/sql"
	"fmt"
	"os"

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
	dbConfig DBConfig
	pageSize int
	threads  int
)

var rootCmd = &cobra.Command{
	Use:   "sitemerge",
	Short: "SiteMerge - TiDB Table Management Tool",
	Long: `SiteMerge is a CLI tool for managing TiDB table information and generating page info.
It provides two main functionalities:
1. Collecting table index information from target databases
2. Generating page information for data processing`,
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

	// Add subcommands
	rootCmd.AddCommand(tableIndexCmd)
	rootCmd.AddCommand(pageInfoCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
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

	fmt.Printf("✅ Successfully connected to TiDB at %s:%d (Max connections: %d)\n",
		dbConfig.Host, dbConfig.Port, maxConnections)
	return db, nil
}

func runTableIndex(cmd *cobra.Command, args []string) {
	keepExisting, _ := cmd.Flags().GetBool("keep-existing")

	db, err := connectDB(10) // Use fixed connections for table-index command
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	manager := NewTableIndexManager(db)
	if err := manager.Run(dbConfig.Database, !keepExisting); err != nil {
		fmt.Fprintf(os.Stderr, "❌ %v\n", err)
		os.Exit(1)
	}
}

func runPageInfo(cmd *cobra.Command, args []string) {
	// Validate threads parameter
	if threads < 1 || threads > 512 {
		fmt.Fprintf(os.Stderr, "❌ Thread count must be between 1 and 512, got: %d\n", threads)
		os.Exit(1)
	}

	// Connect to database with connection pool sized for threads
	maxConnections := threads * 2 // Allow 2 connections per thread
	db, err := connectDB(maxConnections)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	generator := NewPageInfoGenerator(db, pageSize, threads)
	if err := generator.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "❌ %v\n", err)
		os.Exit(1)
	}
}
