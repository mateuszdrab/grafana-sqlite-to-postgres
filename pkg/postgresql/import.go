package postgresql

import (
	"bufio"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	// Postgres driver
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

// DB allows for interface methods.
// It just holds a connection pointer.
type DB struct {
	conn *sql.DB
	log  *logrus.Logger
}

// New returns a Postgres database connection.
func New(connString string, logger *logrus.Logger) (db DB, err error) {
	db.log = logger
	db.conn, err = sql.Open("postgres", connString)
	if err != nil {
		return
	}
	_, err = db.conn.Exec("SELECT 1")
	return
}

// ClearTableRows deletes all rows from all tables in the public schema, except migration_log.
// This ensures a clean slate before inserting records from the dump.
// Returns the number of tables cleared and total rows affected.
func (db *DB) ClearTableRows() (int, int64, error) {
	// Query for all table names in the public schema
	query := `
		SELECT tablename 
		FROM pg_tables 
		WHERE schemaname = 'public'
		ORDER BY tablename
	`

	rows, err := db.conn.Query(query)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query table names: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return 0, 0, fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	if err = rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("error iterating table names: %v", err)
	}

	if len(tables) == 0 {
		return 0, 0, nil
	}

	db.log.Debugf("Found %d tables in public schema", len(tables))

	// Disable foreign key checks temporarily
	if _, err := db.conn.Exec("SET CONSTRAINTS ALL DEFERRED"); err != nil {
		db.log.Warnf("Could not defer constraints: %v", err)
	}

	tablesCleared := 0
	var totalRowsAffected int64

	for _, table := range tables {
		// Skip migration_log table
		if table == "migration_log" {
			db.log.Debugf("Skipping migration_log table")
			continue
		}

		deleteQuery := fmt.Sprintf("DELETE FROM \"%s\"", table)
		result, err := db.conn.Exec(deleteQuery)
		if err != nil {
			db.log.Warnf("Could not delete from table %s: %v", table, err)
			continue
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			db.log.Debugf("Deleted %d rows from %s", rowsAffected, table)
			tablesCleared++
			totalRowsAffected += rowsAffected
		}
	}

	// Re-enable foreign key checks
	if _, err := db.conn.Exec("SET CONSTRAINTS ALL IMMEDIATE"); err != nil {
		db.log.Warnf("Could not restore constraints: %v", err)
	}

	return tablesCleared, totalRowsAffected, nil
}

// ImportDump imports a SQL dump file.
func (db *DB) ImportDump(dumpFile string, progressInterval time.Duration) error {

	promptToContinue := func() bool {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("You seem to have encountered some errors. Would you still like to continue? [Y/n]: ")
		text, _ := reader.ReadString('\n')
		switch response := strings.ToLower(text); response {
		case "n\n":
			return false
		default:
			return true
		}
	}

	// Auto-detect boolean columns in PostgreSQL that need temporary integer conversion
	// because SQLite stores booleans as 0/1 integers.
	boolCols, err := db.detectBooleanColumns()
	if err != nil {
		return fmt.Errorf("failed to detect boolean columns: %w", err)
	}
	db.log.Debugf("Detected %d boolean columns requiring integer conversion", len(boolCols))
	for _, col := range boolCols {
		if col.defaultValue != "" {
			db.log.Debugf("  boolean column: %s.%s (default: %s)", col.table, col.column, col.defaultValue)
		} else {
			db.log.Debugf("  boolean column: %s.%s (no default)", col.table, col.column)
		}
	}

	if errorEncountered := db.prepareTables(boolCols); errorEncountered == true {
		if promptToContinue() != true {
			return fmt.Errorf("%s", "Stopping migration at user's request.")
		}
	}

	file, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}

	sqlStmts := strings.Split(string(file), ";\n")
	totalStmts := 0
	for _, stmt := range sqlStmts {
		if strings.TrimSpace(stmt) != "" {
			totalStmts++
		}
	}

	start := time.Now()
	lastProgress := start
	processedStmts := 0
	progressEnabled := progressInterval > 0

	for _, stmt := range sqlStmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		processedStmts++

		if _, err := db.conn.Exec(stmt); err != nil {
			// We can safely ignore "duplicate key value violates unique constraint" errors.
			if strings.Contains(err.Error(), "duplicate key") {
				continue
			} else if strings.Contains(err.Error(), "is of type bytea but expression is of type text") {
				// TODO(wbh1): This is absolutely horrible and I am ashamed of this code. Should figure out column types ahead of time.
				db.log.Debugf("Failed to import because of type issue (%v). Trying to fix...\n", err.Error())
				stmt = strings.Replace(
					strings.Replace(stmt, `,convert_from('\x`, ",decode('", 1),
					"'utf-8'", "'hex'", 1)
				if _, err := db.conn.Exec(stmt); err != nil {
					return fmt.Errorf("%v %v", err.Error(), stmt)
				}
			} else {
				return fmt.Errorf("%v %v", err.Error(), stmt)
			}
		}

		if progressEnabled && time.Since(lastProgress) >= progressInterval {
			elapsed := time.Since(start).Round(time.Second)
			progress := 0.0
			if totalStmts > 0 {
				progress = (float64(processedStmts) / float64(totalStmts)) * 100
			}
			db.log.Infof("⏳ Import progress: %d/%d statements (%.1f%%), elapsed %s", processedStmts, totalStmts, progress, elapsed)
			lastProgress = time.Now()
		}
	}

	db.log.Debugf("Import execution complete: %d/%d statements in %s", processedStmts, totalStmts, time.Since(start).Round(time.Second))

	// Fix boolean columns that we converted before.
	if errorEncountered := db.decodeBooleanColumns(boolCols); errorEncountered == true {
		if promptToContinue() != true {
			return fmt.Errorf("%s", "Stopping migration at user's request.")
		}
	}

	// Fix sequences for new items.
	if err := db.fixSequences(); err != nil {
		return err
	}

	return nil

}


// boolColumn holds a PostgreSQL boolean column's location and default value.
type boolColumn struct {
	table        string // double-quoted identifier, e.g. "alert"
	column       string // double-quoted identifier, e.g. "silenced"
	defaultValue string // "true", "false", or "" when there is no default
}

// detectBooleanColumns queries PostgreSQL information_schema for all boolean columns
// in the public schema. These columns require temporary integer conversion during
// SQLite data import because SQLite stores booleans as 0 and 1.
func (db *DB) detectBooleanColumns() ([]boolColumn, error) {
	rows, err := db.conn.Query(`
		SELECT table_name, column_name, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND data_type = 'boolean'
		ORDER BY table_name, column_name
	`)
	if err != nil {
		return nil, fmt.Errorf("querying boolean columns: %w", err)
	}
	defer rows.Close()

	var cols []boolColumn
	for rows.Next() {
		var tableName, columnName string
		var columnDefault sql.NullString
		if err := rows.Scan(&tableName, &columnName, &columnDefault); err != nil {
			return nil, fmt.Errorf("scanning boolean column: %w", err)
		}
		col := boolColumn{
			table:  `"` + tableName + `"`,
			column: `"` + columnName + `"`,
		}
		if columnDefault.Valid {
			col.defaultValue = normalizeBoolDefault(columnDefault.String)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// normalizeBoolDefault converts a PostgreSQL column_default expression for a boolean
// column (e.g. "false" or "'false'::boolean") into a plain "true" or "false" literal.
func normalizeBoolDefault(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if idx := strings.Index(s, "::"); idx != -1 {
		s = strings.TrimSpace(s[:idx])
	}
	return strings.Trim(s, "'")
}

// prepareTables temporarily converts boolean columns to integer so that the
// SQLite dump's 0/1 values can be inserted without type errors.
func (db *DB) prepareTables(cols []boolColumn) (errorEncountered bool) {
	for _, col := range cols {
		if col.defaultValue != "" {
			stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", col.table, col.column)
			db.log.Debugln("Executing: ", stmt)
			if _, err := db.conn.Exec(stmt); err != nil {
				if strings.Contains(err.Error(), "does not exist") {
					db.log.Debugf("%s %v %v", "Column/table doesn't exist. This is usually fine to ignore, but here's the info:", err.Error(), stmt)
				} else {
					db.log.Warnf("%v %v", err.Error(), stmt)
					errorEncountered = true
				}
			}
		}

		stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE integer USING %s::integer", col.table, col.column, col.column)
		db.log.Debugln("Executing: ", stmt)
		if _, err := db.conn.Exec(stmt); err != nil {
			if strings.Contains(err.Error(), "does not exist") {
				db.log.Debugf("%s %v %v", "Column/table doesn't exist. This is usually fine to ignore, but here's the info:", err.Error(), stmt)
			} else {
				db.log.Warnf("%v %v", err.Error(), stmt)
				errorEncountered = true
			}
		}
	}
	return
}

// decodeBooleanColumns converts the temporarily-integer columns back to boolean
// and restores any defaults that were dropped in prepareTables.
func (db *DB) decodeBooleanColumns(cols []boolColumn) bool {
	var errorEncountered bool

	for _, col := range cols {
		stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE boolean USING CASE WHEN %s = 0 THEN FALSE WHEN %s = 1 THEN TRUE ELSE NULL END", col.table, col.column, col.column, col.column)
		db.log.Debugln("Executing: ", stmt)
		if _, err := db.conn.Exec(stmt); err != nil {
			if strings.Contains(err.Error(), "does not exist") {
				db.log.Debugf("%s %v %v", "Column/table doesn't exist. This is usually fine to ignore, but here's the info:", err.Error(), stmt)
			} else {
				db.log.Warnf("%v %v", err.Error(), stmt)
				errorEncountered = true
			}
		}

		if col.defaultValue != "" {
			stmt = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", col.table, col.column, col.defaultValue)
			db.log.Debugln("Executing: ", stmt)
			if _, err := db.conn.Exec(stmt); err != nil {
				if strings.Contains(err.Error(), "does not exist") {
					db.log.Debugf("%s %v %v", "Column/table doesn't exist. This is usually fine to ignore, but here's the info:", err.Error(), stmt)
				} else {
					db.log.Warnf("%v %v", err.Error(), stmt)
					errorEncountered = true
				}
			}
		}
	}

	return errorEncountered
}

// Make sure that sequences are fine on the tables
func (db *DB) fixSequences() error {

	// Query from https://wiki.postgresql.org/wiki/Fixing_Sequences
	stmt := `SELECT 'SELECT SETVAL(' ||
	quote_literal(quote_ident(PGT.schemaname) || '.' || quote_ident(S.relname)) ||
	', COALESCE(MAX(' ||quote_ident(C.attname)|| '), 1) ) FROM ' ||
	quote_ident(PGT.schemaname)|| '.'||quote_ident(T.relname)|| ';' stmt
FROM pg_class AS S,
pg_depend AS D,
pg_class AS T,
pg_attribute AS C,
pg_tables AS PGT
WHERE S.relkind = 'S'
AND S.oid = D.objid
AND D.refobjid = T.oid
AND D.refobjid = C.attrelid
AND D.refobjsubid = C.attnum
AND T.relname = PGT.tablename
ORDER BY S.relname;`

	db.log.Debugln("Running query to generate statements to reset all sequences.")
	rows, err := db.conn.Query(stmt)
	if err != nil {
		return fmt.Errorf("%v %v", err.Error(), stmt)
	}
	defer rows.Close()

	db.log.Debugln("Running generated queries to reset all sequences.")
	for rows.Next() {
		var stmt string
		if err := rows.Scan(&stmt); err != nil {
			return fmt.Errorf("%v %v", "Failed to retrieve sequence reset statement", err)
		}

		// Execute the generate statement
		if _, err := db.conn.Exec(stmt); err != nil {
			return fmt.Errorf("%v %v", err.Error(), stmt)
		}
	}

	return nil

}
