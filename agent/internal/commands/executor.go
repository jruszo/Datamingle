package commands

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	stdlibRuntime "runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/jruszo/datamingle/agent/internal/client"
	"github.com/jruszo/datamingle/agent/internal/tools"
)

type Executor struct {
	now          func() time.Time
	toolCacheDir string
	runCommand   func(ctx context.Context, name string, args ...string) ([]byte, error)
	openDB       func(assignment client.Assignment, database string) (*sql.DB, error)
}

type Result struct {
	Message string
	Payload map[string]any
}

type queryData struct {
	Columns     []string
	ColumnTypes []string
	Rows        [][]any
}

type alterTableStatement struct {
	SQL         string
	Database    string
	Table       string
	AlterClause string
}

const commandOutputTailLimit = 64 * 1024

const (
	mysqlTopologyWarningServerUUID              = "server_uuid_unavailable"
	mysqlTopologyWarningReadOnly                = "read_only_unavailable"
	mysqlTopologyWarningSuperReadOnly           = "super_read_only_unavailable"
	mysqlTopologyWarningReplicaStatus           = "replica_status_unavailable"
	mysqlTopologyWarningGroupReplicationMembers = "group_replication_members_unavailable"
)

var alterTablePattern = regexp.MustCompile(`(?is)^\s*alter\s+table\s+((?:` + "`[^`]+`" + `|[A-Za-z0-9_$]+)(?:\.(?:` + "`[^`]+`" + `|[A-Za-z0-9_$]+))?)\s+(.+?)\s*;?\s*$`)
var archiveIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9_$-]+$`)
var archiveStatisticPattern = regexp.MustCompile(`(?im)^(SELECT|INSERT|DELETE)\s+(\d+)\s*$`)

func NewExecutor() *Executor {
	return NewExecutorWithToolCache("")
}

func NewExecutorWithToolCache(toolCacheDir string) *Executor {
	executor := &Executor{now: time.Now, toolCacheDir: toolCacheDir, openDB: openDatabase}
	executor.runCommand = runCommandStreamingTail
	return executor
}

func runCommandStreamingTail(ctx context.Context, name string, args ...string) ([]byte, error) {
	output := newBoundedTailWriter(commandOutputTailLimit)
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = output
	cmd.Stderr = output
	err := cmd.Run()
	return output.Bytes(), err
}

type boundedTailWriter struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func newBoundedTailWriter(limit int) *boundedTailWriter {
	return &boundedTailWriter{limit: limit}
}

func (w *boundedTailWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.limit <= 0 {
		return len(p), nil
	}
	if len(p) >= w.limit {
		w.buf = append(w.buf[:0], p[len(p)-w.limit:]...)
		return len(p), nil
	}
	w.buf = append(w.buf, p...)
	if len(w.buf) > w.limit {
		copy(w.buf, w.buf[len(w.buf)-w.limit:])
		w.buf = w.buf[:w.limit]
	}
	return len(p), nil
}

func (w *boundedTailWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buf...)
}

var _ io.Writer = (*boundedTailWriter)(nil)

func (e *Executor) Execute(ctx context.Context, command client.AgentCommand, cfg client.AgentConfig) (Result, error) {
	assignment, ok := assignmentForCommand(command, cfg)
	if !ok {
		return Result{}, fmt.Errorf("instance %d is not assigned to this agent", command.InstanceID)
	}
	if assignment.DBType != "mysql" && assignment.DBType != "pgsql" {
		return Result{}, fmt.Errorf("unsupported database type %q", assignment.DBType)
	}

	started := e.now()
	switch command.CommandType {
	case "connection.test":
		return e.connectionTest(ctx, assignment, started)
	case "inventory.collect":
		return e.inventoryCollect(ctx, assignment, started)
	case "query.execute":
		return e.queryExecute(ctx, assignment, command, started)
	case "schema.change":
		return e.schemaChange(ctx, assignment, cfg, command, started)
	case "workflow.check":
		return e.workflowCheck(command, started)
	case "workflow.execute":
		return e.workflowExecute(ctx, assignment, cfg, command, started)
	case "export.check":
		return e.exportCheck(ctx, assignment, command, started)
	case "export.execute":
		return e.exportExecute(ctx, assignment, command, started)
	case "archive.execute":
		return e.archiveExecute(ctx, assignment, cfg, command, started)
	default:
		return Result{}, fmt.Errorf("unsupported command type %q", command.CommandType)
	}
}

func (e *Executor) archiveExecute(ctx context.Context, assignment client.Assignment, cfg client.AgentConfig, command client.AgentCommand, started time.Time) (Result, error) {
	if assignment.DBType == "pgsql" {
		return e.postgresArchiveExecute(ctx, assignment, command, started)
	}
	toolPath, err := e.toolArtifactPath("pt-archiver", cfg)
	if err != nil {
		return Result{}, err
	}
	database := stringValueOrDefault(command.Payload, "db_name", assignment.Database)
	table := stringValue(command.Payload, "table_name")
	where := strings.TrimSpace(stringValue(command.Payload, "where"))
	mode := stringValueOrDefault(command.Payload, "mode", "purge")
	if !archiveIdentifierPattern.MatchString(database) || !archiveIdentifierPattern.MatchString(table) {
		return Result{}, fmt.Errorf("archive database and table names contain unsupported characters")
	}
	if where == "" {
		return Result{}, fmt.Errorf("archive condition is required")
	}
	sourceCredentials, cleanupSource, err := onlineSchemaCredentialFile(assignment)
	if err != nil {
		return Result{}, err
	}
	defer cleanupSource()
	sourceDSN := archiveDSN(assignment, sourceCredentials, database, table)
	if assignment.Charset != "" {
		sourceDSN += ",A=" + assignment.Charset
	}
	args := []string{
		fmt.Sprintf("--source=%s", sourceDSN),
		fmt.Sprintf("--where=%s", where),
		"--progress=5000",
		"--statistics",
		"--charset=utf8mb4",
		"--no-check-charset",
		"--limit=10000",
		"--txn-size=1000",
		fmt.Sprintf("--sleep=%d", intValue(command.Payload, "sleep", 1)),
		"--no-version-check",
	}

	var cleanupDestination func()
	switch mode {
	case "purge":
		args = append(args, "--purge")
	case "dest":
		destination, ok := assignmentForInstanceID(int64(intValue(command.Payload, "dest_instance_id", 0)), cfg)
		if !ok {
			return Result{}, fmt.Errorf("archive destination is not assigned to this agent")
		}
		destDatabase := stringValue(command.Payload, "dest_db_name")
		destTable := stringValue(command.Payload, "dest_table_name")
		if !archiveIdentifierPattern.MatchString(destDatabase) || !archiveIdentifierPattern.MatchString(destTable) {
			return Result{}, fmt.Errorf("archive destination names contain unsupported characters")
		}
		destinationCredentials, cleanup, err := onlineSchemaCredentialFile(destination)
		if err != nil {
			return Result{}, err
		}
		cleanupDestination = cleanup
		defer cleanupDestination()
		destDSN := archiveDSN(destination, destinationCredentials, destDatabase, destTable)
		args = append(args, "--dest="+destDSN)
	case "file":
		archiveDir := filepath.Join(filepath.Dir(e.toolCacheDir), "archives")
		if err := os.MkdirAll(archiveDir, 0o750); err != nil {
			return Result{}, err
		}
		args = append(args, fmt.Sprintf("--file=%s", filepath.Join(archiveDir, fmt.Sprintf("archive-%d.txt", command.ID))))
	default:
		return Result{}, fmt.Errorf("unsupported archive mode %q", mode)
	}
	if boolValue(command.Payload, "no_delete", false) {
		args = append(args, "--no-delete")
	}

	output, runErr := e.runCommand(ctx, toolPath, args...)
	statistics := strings.TrimSpace(string(output))
	if runErr != nil {
		return Result{}, fmt.Errorf("pt-archiver failed: %w: %s", runErr, statistics)
	}
	counts := map[string]int{"SELECT": 0, "INSERT": 0, "DELETE": 0}
	for _, match := range archiveStatisticPattern.FindAllStringSubmatch(statistics, -1) {
		if value, parseErr := strconv.Atoi(match[2]); parseErr == nil {
			counts[strings.ToUpper(match[1])] = value
		}
	}
	if !boolValue(command.Payload, "no_delete", false) {
		if mode == "dest" && counts["INSERT"] != counts["DELETE"] {
			return Result{}, fmt.Errorf("pt-archiver insert/delete counts do not match: %d != %d", counts["INSERT"], counts["DELETE"])
		}
		if mode != "dest" && counts["SELECT"] != counts["DELETE"] {
			return Result{}, fmt.Errorf("pt-archiver select/delete counts do not match: %d != %d", counts["SELECT"], counts["DELETE"])
		}
	}

	return Result{Message: "archive executed", Payload: map[string]any{
		"statement":         fmt.Sprintf("pt-archiver %s %s.%s", mode, database, table),
		"select_cnt":        counts["SELECT"],
		"insert_cnt":        counts["INSERT"],
		"delete_cnt":        counts["DELETE"],
		"statistics":        statistics,
		"success":           true,
		"execution_seconds": e.now().Sub(started).Seconds(),
		"command_id":        command.ID,
	}}, nil
}

func (e *Executor) postgresArchiveExecute(ctx context.Context, assignment client.Assignment, command client.AgentCommand, started time.Time) (Result, error) {
	database := stringValueOrDefault(command.Payload, "db_name", assignment.Database)
	table := stringValue(command.Payload, "table_name")
	where := strings.TrimSpace(stringValue(command.Payload, "where"))
	mode := stringValueOrDefault(command.Payload, "mode", "purge")
	quotedTable, identifierOK := quotePostgresIdentifier(table)
	if !archiveIdentifierPattern.MatchString(database) || !identifierOK {
		return Result{}, fmt.Errorf("archive database and table names contain unsupported characters")
	}
	if where == "" {
		return Result{}, fmt.Errorf("archive condition is required")
	}
	if err := validatePostgresArchiveCondition(where); err != nil {
		return Result{}, err
	}
	if mode != "purge" {
		return Result{}, fmt.Errorf("PostgreSQL archiving currently supports purge mode")
	}
	db, err := e.openDB(assignment, database)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if err := ensureWritable(ctx, assignment.DBType, db); err != nil {
		return Result{}, err
	}
	batchSize := 1000
	selected, deleted := 0, 0
	for {
		var count int
		if boolValue(command.Payload, "no_delete", false) {
			if err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", quotedTable, where)).Scan(&count); err != nil {
				return Result{}, fmt.Errorf("PostgreSQL archive count failed: %w", err)
			}
			selected += count
			break
		}
		statement := fmt.Sprintf("WITH candidates AS (SELECT ctid FROM %s WHERE %s LIMIT %d FOR UPDATE SKIP LOCKED), deleted AS (DELETE FROM %s target USING candidates WHERE target.ctid = candidates.ctid RETURNING 1) SELECT count(*) FROM deleted", quotedTable, where, batchSize, quotedTable)
		if err = db.QueryRowContext(ctx, statement).Scan(&count); err != nil {
			return Result{}, fmt.Errorf("PostgreSQL archive batch failed: %w", err)
		}
		selected += count
		deleted += count
		if count < batchSize {
			break
		}
		if sleep := intValue(command.Payload, "sleep", 1); sleep > 0 {
			select {
			case <-ctx.Done():
				return Result{}, ctx.Err()
			case <-time.After(time.Duration(sleep) * time.Second):
			}
		}
	}
	statement := fmt.Sprintf("PostgreSQL batched archive purge %s.%s", database, table)
	return Result{Message: "archive executed", Payload: map[string]any{"statement": statement, "select_cnt": selected, "insert_cnt": 0, "delete_cnt": deleted, "statistics": fmt.Sprintf("SELECT %d\nDELETE %d", selected, deleted), "success": true, "execution_seconds": e.now().Sub(started).Seconds(), "command_id": command.ID}}, nil
}

func quotePostgresIdentifier(identifier string) (string, bool) {
	parts := strings.Split(identifier, ".")
	if len(parts) > 2 {
		return "", false
	}
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		if !archiveIdentifierPattern.MatchString(part) {
			return "", false
		}
		quoted = append(quoted, `"`+strings.ReplaceAll(part, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, "."), true
}

func validatePostgresArchiveCondition(condition string) error {
	if strings.ContainsRune(condition, '\x00') || strings.Contains(condition, ";") || strings.Contains(condition, "--") || strings.Contains(condition, "/*") || strings.Contains(condition, "*/") {
		return fmt.Errorf("archive condition contains unsupported SQL delimiters or comment markers")
	}
	return nil
}

func (e *Executor) inventoryCollect(ctx context.Context, assignment client.Assignment, started time.Time) (Result, error) {
	db, err := openDatabase(assignment, assignment.Database)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()

	var hostname, version string
	query := "SELECT @@hostname, VERSION()"
	if assignment.DBType == "pgsql" {
		query = "SELECT COALESCE(inet_server_addr()::text, current_setting('listen_addresses')), version()"
	}
	if err := db.QueryRowContext(ctx, query).Scan(&hostname, &version); err != nil {
		return Result{}, err
	}
	payload := map[string]any{"hostname": hostname, "version": version, "execution_seconds": e.now().Sub(started).Seconds()}
	if assignment.DBType == "mysql" {
		payload["mysql_topology"] = collectMySQLTopology(ctx, db)
	} else {
		var recovery bool
		if err := db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&recovery); err == nil {
			payload["postgresql_topology"] = map[string]any{"in_recovery": recovery, "role": map[bool]string{true: "replica", false: "primary"}[recovery]}
		}
	}
	return Result{
		Message: "inventory collected",
		Payload: payload,
	}, nil
}

func collectMySQLTopology(ctx context.Context, db *sql.DB) map[string]any {
	warnings := []string{}
	serverUUID, err := queryMySQLTopologyString(ctx, db, "SELECT @@server_uuid")
	if err != nil {
		warnings = appendMySQLTopologyWarning(warnings, mysqlTopologyWarningServerUUID, err)
	}
	readOnly, err := queryMySQLTopologyBool(ctx, db, "SELECT @@global.read_only")
	if err != nil {
		warnings = appendMySQLTopologyWarning(warnings, mysqlTopologyWarningReadOnly, err)
	}
	superReadOnly, err := queryMySQLTopologyBool(ctx, db, "SELECT @@global.super_read_only")
	if err != nil {
		warnings = appendMySQLTopologyWarning(warnings, mysqlTopologyWarningSuperReadOnly, err)
	}
	replicaStatus, err := queryFirstRowMap(ctx, db, "SHOW REPLICA STATUS")
	if err != nil {
		log.Printf("mysql topology warning %s via SHOW REPLICA STATUS: %v", mysqlTopologyWarningReplicaStatus, err)
		replicaStatus, err = queryFirstRowMap(ctx, db, "SHOW SLAVE STATUS")
		if err != nil {
			warnings = appendMySQLTopologyWarning(warnings, mysqlTopologyWarningReplicaStatus, err)
		}
	}
	groupMembers, err := queryRowsMap(
		ctx,
		db,
		"SELECT MEMBER_HOST, MEMBER_PORT, MEMBER_ROLE, MEMBER_STATE FROM performance_schema.replication_group_members",
	)
	if err != nil {
		if !isMissingMySQLTableError(err) {
			warnings = appendMySQLTopologyWarning(warnings, mysqlTopologyWarningGroupReplicationMembers, err)
		}
	}
	payload := buildMySQLTopologyPayload(serverUUID, readOnly, superReadOnly, replicaStatus, groupMembers)
	if len(warnings) > 0 {
		payload["warnings"] = warnings
	}
	return payload
}

func appendMySQLTopologyWarning(warnings []string, code string, err error) []string {
	if err != nil {
		log.Printf("mysql topology warning %s: %v", code, err)
	}
	return append(warnings, code)
}

func queryMySQLTopologyString(ctx context.Context, db *sql.DB, query string) (string, error) {
	var value any
	if err := db.QueryRowContext(ctx, query).Scan(&value); err != nil {
		return "", err
	}
	return topologyStringFromAny(value), nil
}

func queryMySQLTopologyBool(ctx context.Context, db *sql.DB, query string) (*bool, error) {
	var value any
	if err := db.QueryRowContext(ctx, query).Scan(&value); err != nil {
		return nil, err
	}
	parsed, ok := topologyBoolFromAny(value)
	if !ok {
		return nil, fmt.Errorf("unexpected boolean value %q", topologyStringFromAny(value))
	}
	return &parsed, nil
}

func isMissingMySQLTableError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1109 || mysqlErr.Number == 1146
}

func buildMySQLTopologyPayload(serverUUID string, readOnly *bool, superReadOnly *bool, replicaStatus map[string]string, groupMembers []map[string]string) map[string]any {
	payload := map[string]any{}
	if strings.TrimSpace(serverUUID) != "" {
		payload["server_uuid"] = strings.TrimSpace(serverUUID)
	}
	if readOnly != nil {
		payload["read_only"] = *readOnly
	}
	if superReadOnly != nil {
		payload["super_read_only"] = *superReadOnly
	}
	sourceHost := firstMapString(replicaStatus, "Source_Host", "Master_Host")
	sourcePort := intFromString(firstMapString(replicaStatus, "Source_Port", "Master_Port"))
	if sourceHost != "" {
		if sourcePort == 0 {
			sourcePort = 3306
		}
		payload["source_host"] = sourceHost
		payload["source_port"] = sourcePort
	}
	if len(replicaStatus) > 0 {
		payload["replica_status"] = replicaStatus
	}
	if len(groupMembers) > 0 {
		members := make([]map[string]string, 0, len(groupMembers))
		for _, raw := range groupMembers {
			members = append(members, map[string]string{
				"member_host":  firstMapString(raw, "MEMBER_HOST", "member_host"),
				"member_port":  firstMapString(raw, "MEMBER_PORT", "member_port"),
				"member_role":  firstMapString(raw, "MEMBER_ROLE", "member_role"),
				"member_state": firstMapString(raw, "MEMBER_STATE", "member_state"),
			})
		}
		payload["group_replication_members"] = members
	}
	return payload
}

func queryFirstRowMap(ctx context.Context, db *sql.DB, query string) (map[string]string, error) {
	rows, err := queryRowsMap(ctx, db, query)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return map[string]string{}, nil
	}
	return rows[0], nil
}

func queryRowsMap(ctx context.Context, db *sql.DB, query string) ([]map[string]string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	results := []map[string]string{}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := map[string]string{}
		for i, column := range columns {
			if values[i].Valid {
				row[column] = values[i].String
			}
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func firstMapString(values map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func topologyIntFromAny(value any) int {
	switch typed := value.(type) {
	case []byte:
		return intFromString(string(typed))
	case string:
		return intFromString(typed)
	}
	return intFromString(fmt.Sprint(value))
}

func topologyBoolFromAny(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case int:
		return typed != 0, true
	case int64:
		return typed != 0, true
	case float64:
		return typed != 0, true
	case []byte:
		return topologyBoolFromString(string(typed))
	case string:
		return topologyBoolFromString(typed)
	default:
		return false, false
	}
}

func topologyBoolFromString(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "on", "true", "yes":
		return true, true
	case "0", "off", "false", "no":
		return false, true
	default:
		return false, false
	}
}

func topologyStringFromAny(value any) string {
	switch typed := value.(type) {
	case []byte:
		return strings.TrimSpace(string(typed))
	case string:
		return strings.TrimSpace(typed)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func intFromString(value string) int {
	var result int
	if _, err := fmt.Sscanf(strings.TrimSpace(value), "%d", &result); err != nil {
		return 0
	}
	return result
}

func assignmentForCommand(command client.AgentCommand, cfg client.AgentConfig) (client.Assignment, bool) {
	return assignmentForInstanceID(command.InstanceID, cfg)
}

func assignmentForInstanceID(instanceID int64, cfg client.AgentConfig) (client.Assignment, bool) {
	for _, assignment := range cfg.Assignments {
		if assignment.InstanceID == instanceID {
			return assignment, true
		}
	}
	return client.Assignment{}, false
}

func (e *Executor) connectionTest(ctx context.Context, assignment client.Assignment, started time.Time) (Result, error) {
	db, err := openDatabase(assignment, assignment.Database)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return Result{}, err
	}
	return Result{
		Message: "connection test succeeded",
		Payload: map[string]any{
			"execution_seconds": e.now().Sub(started).Seconds(),
		},
	}, nil
}

func (e *Executor) queryExecute(ctx context.Context, assignment client.Assignment, command client.AgentCommand, started time.Time) (Result, error) {
	sqlText := stringValue(command.Payload, "sql")
	if sqlText == "" {
		return Result{}, fmt.Errorf("query command payload is missing sql")
	}
	if !isReadOnlySQL(sqlText) {
		return Result{}, fmt.Errorf("query.execute only allows read-only SQL")
	}
	limit := intValue(command.Payload, "limit", 1000)
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}

	data, err := queryRows(
		ctx,
		assignment,
		stringValueOrDefault(command.Payload, "db_name", assignment.Database),
		sqlText,
		limit,
	)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Message: "query executed",
		Payload: map[string]any{
			"full_sql":              sqlText,
			"column_list":           data.Columns,
			"columns":               data.Columns,
			"column_type":           data.ColumnTypes,
			"rows":                  data.Rows,
			"row_count":             len(data.Rows),
			"affected_rows":         len(data.Rows),
			"execution_seconds":     e.now().Sub(started).Seconds(),
			"seconds_behind_master": "",
		},
	}, nil
}

func (e *Executor) schemaChange(ctx context.Context, assignment client.Assignment, cfg client.AgentConfig, command client.AgentCommand, started time.Time) (Result, error) {
	sqlText := stringValue(command.Payload, "sql")
	if sqlText == "" {
		return Result{}, fmt.Errorf("schema change command payload is missing sql")
	}
	executor := stringValueOrDefault(command.Payload, "executor", "direct")
	if executor != "direct" {
		if assignment.DBType != "mysql" {
			return Result{}, fmt.Errorf("online schema executors are only supported for MySQL")
		}
		statements, ok := singleSQLStatement(sqlText)
		if !ok {
			return Result{}, fmt.Errorf("schema.change only allows one DDL statement")
		}
		return e.executeOnlineSchemaStatements(
			ctx,
			assignment,
			cfg,
			command,
			started,
			executor,
			stringValueOrDefault(command.Payload, "db_name", assignment.Database),
			[]string{statements},
		)
	}
	if !isSafeDDL(sqlText) {
		return Result{}, fmt.Errorf("schema.change only allows approved single-statement DDL")
	}
	db, err := openDatabase(assignment, stringValueOrDefault(command.Payload, "db_name", assignment.Database))
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if err := ensureWritable(ctx, assignment.DBType, db); err != nil {
		return Result{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, sqlText)
	if err != nil {
		return Result{}, err
	}
	affectedRows, _ := result.RowsAffected()
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	finished := e.now()
	return Result{
		Message: "schema change executed",
		Payload: map[string]any{
			"affected_rows":     affectedRows,
			"execution_seconds": finished.Sub(started).Seconds(),
			"audit": map[string]any{
				"assignment_id": assignment.ID,
				"instance_id":   assignment.InstanceID,
				"executor":      executor,
				"sql":           sqlText,
				"started_at":    started.UTC().Format(time.RFC3339Nano),
				"finished_at":   finished.UTC().Format(time.RFC3339Nano),
				"result":        "committed",
			},
		},
	}, nil
}

func (e *Executor) workflowCheck(command client.AgentCommand, started time.Time) (Result, error) {
	sqlText := stringValue(command.Payload, "sql")
	if sqlText == "" {
		return Result{}, fmt.Errorf("workflow check payload is missing sql")
	}

	statements, ok := splitSQLStatements(sqlText)
	if !ok {
		rows := []map[string]any{
			reviewRow(1, "SQL review", 2, "Audit failed", "SQL must contain at least one complete statement.", sqlText, 0, 0, command.ID),
		}
		return Result{
			Message: "workflow check completed",
			Payload: reviewPayload(sqlText, 0, rows, started, e.now()),
		}, nil
	}

	syntaxType := 0
	rows := make([]map[string]any, 0, len(statements))
	for i, statement := range statements {
		statementSyntaxType := classifyWorkflowSyntax(statement)
		if statementSyntaxType == 0 {
			rows = append(rows, reviewRow(
				i+1,
				"SQL review",
				2,
				"Audit failed",
				"Only DDL and DML workflow statements are supported.",
				statement,
				0,
				0,
				command.ID,
			))
			continue
		}
		if statementSyntaxType == 1 {
			syntaxType = 1
		} else if syntaxType == 0 {
			syntaxType = 2
		}
		rows = append(rows, reviewRow(
			i+1,
			"SQL review",
			0,
			"Audit completed",
			"",
			statement,
			0,
			0,
			command.ID,
		))
	}

	return Result{
		Message: "workflow check completed",
		Payload: reviewPayload(sqlText, syntaxType, rows, started, e.now()),
	}, nil
}

func (e *Executor) workflowExecute(ctx context.Context, assignment client.Assignment, cfg client.AgentConfig, command client.AgentCommand, started time.Time) (Result, error) {
	sqlText := stringValue(command.Payload, "sql")
	if sqlText == "" {
		return Result{}, fmt.Errorf("workflow execute payload is missing sql")
	}
	executor := stringValueOrDefault(command.Payload, "executor", "direct")
	statements, ok := splitSQLStatements(sqlText)
	if !ok {
		return Result{}, fmt.Errorf("workflow SQL must contain at least one complete statement")
	}
	for _, statement := range statements {
		if classifyWorkflowSyntax(statement) == 0 {
			return Result{}, fmt.Errorf("workflow.execute only allows DDL and DML statements")
		}
	}
	if executor != "direct" {
		return e.executeOnlineSchemaStatements(
			ctx,
			assignment,
			cfg,
			command,
			started,
			executor,
			stringValueOrDefault(command.Payload, "db_name", assignment.Database),
			statements,
		)
	}

	db, err := openDatabase(assignment, stringValueOrDefault(command.Payload, "db_name", assignment.Database))
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if err := ensureWritable(ctx, assignment.DBType, db); err != nil {
		return Result{}, err
	}

	rows := make([]map[string]any, 0, len(statements))
	var totalAffectedRows int64
	for i, statement := range statements {
		statementStarted := e.now()
		result, err := db.ExecContext(ctx, statement)
		if err != nil {
			return Result{}, fmt.Errorf("statement %d failed: %w", i+1, err)
		}
		affectedRows, _ := result.RowsAffected()
		totalAffectedRows += affectedRows
		rows = append(rows, reviewRow(
			i+1,
			"Agent execution",
			0,
			"Execute Successfully",
			"",
			statement,
			affectedRows,
			e.now().Sub(statementStarted).Seconds(),
			command.ID,
		))
	}

	finished := e.now()
	return Result{
		Message: "workflow executed",
		Payload: map[string]any{
			"full_sql":             sqlText,
			"execute_rows":         rows,
			"review_rows":          rows,
			"affected_rows":        totalAffectedRows,
			"actual_affected_rows": totalAffectedRows,
			"warning_count":        0,
			"error_count":          0,
			"status":               "Execute Successfully",
			"execution_seconds":    finished.Sub(started).Seconds(),
			"command_id":           command.ID,
		},
	}, nil
}

func (e *Executor) exportCheck(ctx context.Context, assignment client.Assignment, command client.AgentCommand, started time.Time) (Result, error) {
	sqlText := stringValue(command.Payload, "sql")
	if sqlText == "" {
		return Result{}, fmt.Errorf("export check payload is missing sql")
	}
	statement, ok := singleSQLStatement(sqlText)
	if !ok || !isExportSQL(sqlText) {
		rows := []map[string]any{
			reviewRow(1, "Export review", 2, "Audit failed", "Only single-statement SELECT export SQL is supported.", sqlText, 0, 0, command.ID),
		}
		return Result{
			Message: "export check completed",
			Payload: reviewPayload(sqlText, 3, rows, started, e.now()),
		}, nil
	}
	maxRows := intValue(command.Payload, "max_export_rows", 10000)
	if maxRows <= 0 {
		maxRows = 10000
	}

	db, err := openDatabase(assignment, stringValueOrDefault(command.Payload, "db_name", assignment.Database))
	if err != nil {
		return Result{}, err
	}
	defer db.Close()

	var rowCount int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS datamingle_export_check", statement)
	if err := db.QueryRowContext(ctx, countSQL).Scan(&rowCount); err != nil {
		rows := []map[string]any{
			reviewRow(1, "Export review", 2, "Audit failed", err.Error(), statement, 0, 0, command.ID),
		}
		return Result{
			Message: "export check completed",
			Payload: reviewPayload(sqlText, 3, rows, started, e.now()),
		}, nil
	}

	errLevel := 0
	stageStatus := "Audit completed"
	errMessage := ""
	if rowCount > int64(maxRows) {
		errLevel = 2
		stageStatus = "Audit failed"
		errMessage = fmt.Sprintf("Export row count %d exceeds configured maximum %d.", rowCount, maxRows)
	}
	rows := []map[string]any{
		reviewRow(1, "Export review", errLevel, stageStatus, errMessage, statement, rowCount, 0, command.ID),
	}
	payload := reviewPayload(sqlText, 3, rows, started, e.now())
	payload["affected_rows"] = rowCount
	return Result{
		Message: "export check completed",
		Payload: payload,
	}, nil
}

func (e *Executor) exportExecute(ctx context.Context, assignment client.Assignment, command client.AgentCommand, started time.Time) (Result, error) {
	sqlText := stringValue(command.Payload, "sql")
	if sqlText == "" {
		return Result{}, fmt.Errorf("export execute payload is missing sql")
	}
	if !isExportSQL(sqlText) {
		return Result{}, fmt.Errorf("export.execute only allows single-statement SELECT SQL")
	}
	maxRows := intValue(command.Payload, "max_export_rows", 10000)
	if maxRows <= 0 {
		maxRows = 10000
	}

	data, err := queryRows(
		ctx,
		assignment,
		stringValueOrDefault(command.Payload, "db_name", assignment.Database),
		sqlText,
		maxRows+1,
	)
	if err != nil {
		return Result{}, err
	}
	if len(data.Rows) > maxRows {
		return Result{}, fmt.Errorf("export row count exceeds configured maximum %d", maxRows)
	}

	return Result{
		Message: "export executed",
		Payload: map[string]any{
			"full_sql":          sqlText,
			"column_list":       data.Columns,
			"columns":           data.Columns,
			"column_type":       data.ColumnTypes,
			"rows":              data.Rows,
			"row_count":         len(data.Rows),
			"affected_rows":     len(data.Rows),
			"export_format":     stringValue(command.Payload, "export_format"),
			"execution_seconds": e.now().Sub(started).Seconds(),
			"command_id":        command.ID,
		},
	}, nil
}

func (e *Executor) executeOnlineSchemaStatements(
	ctx context.Context,
	assignment client.Assignment,
	cfg client.AgentConfig,
	command client.AgentCommand,
	started time.Time,
	executorID string,
	defaultDatabase string,
	statements []string,
) (Result, error) {
	toolPath, err := e.onlineSchemaToolPath(executorID, cfg)
	if err != nil {
		return Result{}, err
	}

	parsedStatements := make([]alterTableStatement, 0, len(statements))
	for _, statement := range statements {
		if classifyWorkflowSyntax(statement) != 1 {
			return Result{}, fmt.Errorf("%s execution only supports DDL statements", executorID)
		}
		parsed, err := parseAlterTable(defaultDatabase, statement)
		if err != nil {
			return Result{}, err
		}
		parsedStatements = append(parsedStatements, parsed)
	}

	credentialFile, cleanupCredentials, err := onlineSchemaCredentialFile(assignment)
	if err != nil {
		return Result{}, err
	}
	defer cleanupCredentials()

	commandArgs := make([][]string, 0, len(parsedStatements))
	for _, parsed := range parsedStatements {
		args, err := onlineSchemaCommandArgs(executorID, toolPath, assignment, parsed, credentialFile)
		if err != nil {
			return Result{}, err
		}
		commandArgs = append(commandArgs, args)
	}

	rows := make([]map[string]any, 0, len(statements))
	for i, parsed := range parsedStatements {
		statementStarted := e.now()
		args := commandArgs[i]
		output, err := e.runCommand(ctx, args[0], args[1:]...)
		if err != nil {
			message := strings.TrimSpace(string(output))
			if message == "" {
				message = err.Error()
			}
			return Result{}, fmt.Errorf("%s execution failed for %s.%s: %s", executorID, parsed.Database, parsed.Table, message)
		}
		row := reviewRow(
			i+1,
			"Agent online schema execution",
			0,
			"Execute Successfully",
			"",
			parsed.SQL,
			0,
			e.now().Sub(statementStarted).Seconds(),
			command.ID,
		)
		row["executor"] = executorID
		row["table_name"] = parsed.Table
		rows = append(rows, row)
	}

	finished := e.now()
	if command.CommandType == "schema.change" {
		return Result{
			Message: "schema change executed",
			Payload: map[string]any{
				"affected_rows":     0,
				"execution_seconds": finished.Sub(started).Seconds(),
				"audit": map[string]any{
					"assignment_id": assignment.ID,
					"instance_id":   assignment.InstanceID,
					"executor":      executorID,
					"sql":           stringValue(command.Payload, "sql"),
					"started_at":    started.UTC().Format(time.RFC3339Nano),
					"finished_at":   finished.UTC().Format(time.RFC3339Nano),
					"result":        "committed",
					"command_id":    command.ID,
					"rows":          rows,
				},
			},
		}, nil
	}
	return Result{
		Message: "workflow executed",
		Payload: map[string]any{
			"full_sql":             stringValue(command.Payload, "sql"),
			"execute_rows":         rows,
			"review_rows":          rows,
			"affected_rows":        0,
			"actual_affected_rows": 0,
			"warning_count":        0,
			"error_count":          0,
			"status":               "Execute Successfully",
			"execution_seconds":    finished.Sub(started).Seconds(),
			"command_id":           command.ID,
		},
	}, nil
}

func (e *Executor) onlineSchemaToolPath(executorID string, cfg client.AgentConfig) (string, error) {
	toolName := onlineSchemaToolName(executorID)
	if toolName == "" {
		return "", fmt.Errorf("unsupported online schema executor %q", executorID)
	}
	return e.toolArtifactPath(toolName, cfg)
}

func (e *Executor) toolArtifactPath(toolName string, cfg client.AgentConfig) (string, error) {
	if strings.TrimSpace(e.toolCacheDir) == "" {
		return "", fmt.Errorf("%s artifact is not available: tool cache directory is not configured", toolName)
	}
	for _, artifact := range cfg.ToolArtifacts {
		if artifact.ToolName != toolName {
			continue
		}
		if artifact.Platform != stdlibRuntime.GOOS || artifact.Architecture != stdlibRuntime.GOARCH {
			continue
		}
		path := tools.ArtifactPath(e.toolCacheDir, tools.Artifact{
			ToolName:     artifact.ToolName,
			Version:      artifact.Version,
			Platform:     artifact.Platform,
			Architecture: artifact.Architecture,
			DownloadURL:  artifact.DownloadURL,
			SHA256:       artifact.SHA256,
			SizeBytes:    artifact.SizeBytes,
		})
		info, err := os.Stat(path)
		if err == nil && !info.IsDir() {
			return path, nil
		}
	}
	return "", fmt.Errorf("%s artifact is not available for %s/%s", toolName, stdlibRuntime.GOOS, stdlibRuntime.GOARCH)
}

func onlineSchemaToolName(executorID string) string {
	switch executorID {
	case "gh-ost":
		return "gh-ost"
	case "pt-osc":
		return "pt-online-schema-change"
	default:
		return ""
	}
}

func parseAlterTable(defaultDatabase string, sqlText string) (alterTableStatement, error) {
	match := alterTablePattern.FindStringSubmatch(strings.TrimSpace(sqlText))
	if match == nil {
		return alterTableStatement{}, fmt.Errorf("online schema execution only supports ALTER TABLE statements")
	}
	objectName := match[1]
	alterClause := strings.TrimSpace(match[2])
	database := strings.TrimSpace(defaultDatabase)
	table := objectName
	if parts := splitQualifiedIdentifier(objectName); len(parts) == 2 {
		database = parts[0]
		table = parts[1]
	}
	database = normalizeIdentifier(database)
	table = normalizeIdentifier(table)
	if database == "" || table == "" {
		return alterTableStatement{}, fmt.Errorf("ALTER TABLE statement must include a database and table")
	}
	return alterTableStatement{
		SQL:         strings.TrimSpace(sqlText),
		Database:    database,
		Table:       table,
		AlterClause: alterClause,
	}, nil
}

func splitQualifiedIdentifier(value string) []string {
	inBacktick := false
	for index, r := range value {
		switch r {
		case '`':
			inBacktick = !inBacktick
		case '.':
			if !inBacktick {
				return []string{value[:index], value[index+1:]}
			}
		}
	}
	return []string{value}
}

func normalizeIdentifier(value string) string {
	return strings.Trim(strings.TrimSpace(value), "`")
}

func onlineSchemaCommandArgs(executorID string, toolPath string, assignment client.Assignment, statement alterTableStatement, credentialFile string) ([]string, error) {
	base := []string{
		toolPath,
		fmt.Sprintf("--host=%s", assignment.Host),
		fmt.Sprintf("--port=%d", assignment.Port),
		fmt.Sprintf("--user=%s", assignment.Username),
	}
	if assignment.Charset != "" {
		base = append(base, fmt.Sprintf("--charset=%s", assignment.Charset))
	}
	switch executorID {
	case "gh-ost":
		if credentialFile != "" {
			base = append(base, fmt.Sprintf("--conf=%s", credentialFile))
		}
		return append(base,
			fmt.Sprintf("--database=%s", statement.Database),
			fmt.Sprintf("--table=%s", statement.Table),
			fmt.Sprintf("--alter=%s", statement.AlterClause),
			"--allow-on-master",
			"--assume-rbr",
			"--exact-rowcount",
			"--initially-drop-ghost-table",
			"--initially-drop-old-table",
			"--execute",
		), nil
	case "pt-osc":
		dsn := fmt.Sprintf("D=%s,t=%s", statement.Database, statement.Table)
		if credentialFile != "" {
			dsn = fmt.Sprintf("F=%s,%s", credentialFile, dsn)
		}
		return append(base,
			fmt.Sprintf("--alter=%s", statement.AlterClause),
			"--alter-foreign-keys-method=auto",
			"--recursion-method=none",
			"--execute",
			dsn,
		), nil
	default:
		return nil, fmt.Errorf("unsupported online schema executor %q", executorID)
	}
}

func onlineSchemaCredentialFile(assignment client.Assignment) (string, func(), error) {
	if assignment.Password == "" {
		return "", func() {}, nil
	}
	file, err := os.CreateTemp("", "datamingle-online-schema-*.cnf")
	if err != nil {
		return "", nil, err
	}
	path := file.Name()
	cleanup := func() {
		_ = os.Remove(path)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		cleanup()
		return "", nil, err
	}
	_, err = fmt.Fprintf(
		file,
		"[client]\nuser=%s\npassword=%s\nhost=%s\nport=%d\n",
		assignment.Username,
		assignment.Password,
		assignment.Host,
		assignment.Port,
	)
	if assignment.Charset != "" && err == nil {
		_, err = fmt.Fprintf(file, "default-character-set=%s\n", assignment.Charset)
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		cleanup()
		return "", nil, err
	}
	return path, cleanup, nil
}

func archiveDSN(assignment client.Assignment, credentialFile, database, table string) string {
	parts := make([]string, 0, 7)
	if credentialFile != "" {
		parts = append(parts, "F="+credentialFile)
	} else {
		parts = append(parts,
			"h="+assignment.Host,
			fmt.Sprintf("P=%d", assignment.Port),
			"u="+assignment.Username,
		)
	}
	parts = append(parts, "D="+database, "t="+table)
	return strings.Join(parts, ",")
}

func queryRows(ctx context.Context, assignment client.Assignment, database string, sqlText string, limit int) (queryData, error) {
	db, err := openDatabase(assignment, database)
	if err != nil {
		return queryData{}, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return queryData{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return queryData{}, err
	}
	columnTypes := columnTypeNames(rows)
	values := make([]any, len(columns))
	valuePointers := make([]any, len(columns))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	resultRows := make([][]any, 0, min(max(limit, 0), 100))
	for rows.Next() {
		if limit > 0 && len(resultRows) >= limit {
			break
		}
		for i := range values {
			values[i] = nil
		}
		if err := rows.Scan(valuePointers...); err != nil {
			return queryData{}, err
		}
		row := make([]any, len(columns))
		for i := range columns {
			row[i] = normalizeSQLValue(values[i])
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return queryData{}, err
	}
	return queryData{Columns: columns, ColumnTypes: columnTypes, Rows: resultRows}, nil
}

func columnTypeNames(rows *sql.Rows) []string {
	types, err := rows.ColumnTypes()
	if err != nil {
		return []string{}
	}
	names := make([]string, len(types))
	for i, columnType := range types {
		names[i] = columnType.DatabaseTypeName()
	}
	return names
}

func openMySQL(assignment client.Assignment, database string) (*sql.DB, error) {
	cfg := mysql.NewConfig()
	cfg.User = assignment.Username
	cfg.Passwd = assignment.Password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(assignment.Host, fmt.Sprintf("%d", assignment.Port))
	cfg.DBName = database
	cfg.ParseTime = true
	cfg.Timeout = 10 * time.Second
	cfg.ReadTimeout = 30 * time.Second
	cfg.WriteTimeout = 30 * time.Second
	cfg.MultiStatements = false
	if assignment.Charset != "" {
		cfg.Params = map[string]string{"charset": assignment.Charset}
	}
	if assignment.SSL.Enabled {
		cfg.TLSConfig = "preferred"
		if assignment.SSL.Verify {
			cfg.TLSConfig = "true"
		}
	}
	return sql.Open("mysql", cfg.FormatDSN())
}

func openDatabase(assignment client.Assignment, database string) (*sql.DB, error) {
	if assignment.DBType == "mysql" {
		return openMySQL(assignment, database)
	}
	if assignment.DBType != "pgsql" {
		return nil, fmt.Errorf("unsupported database type %q", assignment.DBType)
	}
	if database == "" {
		database = "postgres"
	}
	u := &url.URL{Scheme: "postgres", User: url.UserPassword(assignment.Username, assignment.Password), Host: net.JoinHostPort(assignment.Host, strconv.Itoa(assignment.Port)), Path: "/" + database}
	q := u.Query()
	q.Set("connect_timeout", "10")
	q.Set("sslmode", "disable")
	if assignment.SSL.Enabled {
		q.Set("sslmode", map[bool]string{true: "verify-full", false: "require"}[assignment.SSL.Verify])
	}
	u.RawQuery = q.Encode()
	return sql.Open("postgres", u.String())
}

func ensureWritable(ctx context.Context, dbType string, db *sql.DB) error {
	if dbType == "mysql" {
		return ensureWritableMySQL(ctx, db)
	}
	var readOnly, recovery bool
	if err := db.QueryRowContext(ctx, "SELECT current_setting('transaction_read_only')::boolean, pg_is_in_recovery()").Scan(&readOnly, &recovery); err != nil {
		return fmt.Errorf("unable to verify PostgreSQL writable state: %w", err)
	}
	if readOnly || recovery {
		return fmt.Errorf("PostgreSQL target is read-only or in recovery")
	}
	return nil
}

func ensureWritableMySQL(ctx context.Context, db *sql.DB) error {
	var readOnly int
	if err := db.QueryRowContext(ctx, "SELECT @@global.read_only").Scan(&readOnly); err != nil {
		return fmt.Errorf("unable to verify MySQL read_only state: %w", err)
	}
	if readOnly != 0 {
		return fmt.Errorf("MySQL read_only is enabled")
	}
	var superReadOnly int
	if err := db.QueryRowContext(ctx, "SELECT @@global.super_read_only").Scan(&superReadOnly); err == nil && superReadOnly != 0 {
		return fmt.Errorf("MySQL super_read_only is enabled")
	}
	return nil
}

func isReadOnlySQL(sqlText string) bool {
	normalized, ok := normalizeSingleSQLStatement(sqlText)
	if !ok {
		return false
	}
	if hasSQLPrefix(normalized, "show grants") {
		return false
	}
	if operation, mutating, ok := classifyPostgresCTE(normalized); ok {
		return operation == "select" && !mutating
	}
	return hasSQLPrefix(normalized, "select") ||
		hasSQLPrefix(normalized, "explain") ||
		hasSQLPrefix(normalized, "describe") ||
		hasSQLPrefix(normalized, "desc") ||
		hasSQLPrefix(normalized, "show")
}

func isExportSQL(sqlText string) bool {
	normalized, ok := normalizeSingleSQLStatement(sqlText)
	if !ok {
		return false
	}
	if operation, mutating, ok := classifyPostgresCTE(normalized); ok {
		return operation == "select" && !mutating
	}
	return hasSQLPrefix(normalized, "select")
}

func isSafeDDL(sqlText string) bool {
	normalized, ok := normalizeSingleSQLStatement(sqlText)
	if !ok {
		return false
	}
	return hasSQLPrefix(normalized, "create table") ||
		hasSQLPrefix(normalized, "alter table") ||
		hasSQLPrefix(normalized, "create index") ||
		hasSQLPrefix(normalized, "drop index")
}

func classifyWorkflowSyntax(sqlText string) int {
	normalized := normalizeSQLStatement(stripSQLComments(sqlText))
	if operation, mutating, ok := classifyPostgresCTE(normalized); ok {
		if mutating || operation != "select" {
			return 2
		}
		return 0
	}
	switch {
	case normalized == "":
		return 0
	case hasSQLPrefix(normalized, "drop database"),
		hasSQLPrefix(normalized, "create database"),
		hasSQLPrefix(normalized, "alter database"):
		return 0
	case hasSQLPrefix(normalized, "alter"),
		hasSQLPrefix(normalized, "create"),
		hasSQLPrefix(normalized, "drop"),
		hasSQLPrefix(normalized, "rename"),
		hasSQLPrefix(normalized, "truncate"):
		return 1
	case hasSQLPrefix(normalized, "call"),
		hasSQLPrefix(normalized, "delete"),
		hasSQLPrefix(normalized, "insert"),
		hasSQLPrefix(normalized, "replace"),
		hasSQLPrefix(normalized, "update"):
		return 2
	default:
		return 0
	}
}

func classifyPostgresCTE(sqlText string) (operation string, mutating bool, ok bool) {
	if !hasSQLPrefix(sqlText, "with") && !hasSQLPrefix(sqlText, "with recursive") {
		return "", false, false
	}
	depth := 0
	completedBody := false
	for i := 0; i < len(sqlText); {
		if delimiter, found := postgresDollarQuoteDelimiter(sqlText, i); found {
			closing := strings.Index(sqlText[i+len(delimiter):], delimiter)
			if closing < 0 {
				return "", false, true
			}
			i += len(delimiter) + closing + len(delimiter)
			continue
		}
		if strings.HasPrefix(sqlText[i:], "--") {
			if newline := strings.IndexByte(sqlText[i+2:], '\n'); newline >= 0 {
				i += newline + 3
				continue
			}
			break
		}
		if strings.HasPrefix(sqlText[i:], "/*") {
			if closing := strings.Index(sqlText[i+2:], "*/"); closing >= 0 {
				i += closing + 4
				continue
			}
			break
		}
		switch sqlText[i] {
		case '\'', '"':
			quote := sqlText[i]
			i++
			for i < len(sqlText) {
				if sqlText[i] == quote {
					if i+1 < len(sqlText) && sqlText[i+1] == quote {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
		case '(':
			depth++
			i++
		case ')':
			if depth == 1 {
				completedBody = true
			}
			if depth > 0 {
				depth--
			}
			i++
		default:
			if !unicode.IsLetter(rune(sqlText[i])) && sqlText[i] != '_' {
				i++
				continue
			}
			start := i
			for i < len(sqlText) && (unicode.IsLetter(rune(sqlText[i])) || unicode.IsDigit(rune(sqlText[i])) || sqlText[i] == '_') {
				i++
			}
			token := strings.ToLower(sqlText[start:i])
			isMutation := token == "insert" || token == "update" || token == "delete"
			if depth > 0 && isMutation {
				mutating = true
			}
			if depth == 0 && completedBody && (token == "select" || isMutation) {
				return token, mutating, true
			}
		}
	}
	return "", mutating, true
}

func postgresDollarQuoteDelimiter(sqlText string, start int) (string, bool) {
	if start >= len(sqlText) || sqlText[start] != '$' {
		return "", false
	}
	for i := start + 1; i < len(sqlText); i++ {
		if sqlText[i] == '$' {
			return sqlText[start : i+1], true
		}
		if !unicode.IsLetter(rune(sqlText[i])) && !unicode.IsDigit(rune(sqlText[i])) && sqlText[i] != '_' {
			return "", false
		}
	}
	return "", false
}

func normalizeSingleSQLStatement(sqlText string) (string, bool) {
	statement, ok := singleSQLStatement(sqlText)
	if !ok {
		return "", false
	}
	return normalizeSQLStatement(statement), true
}

func singleSQLStatement(sqlText string) (string, bool) {
	statements, ok := splitSQLStatements(sqlText)
	if !ok || len(statements) != 1 {
		return "", false
	}
	return statements[0], true
}

func normalizeSQLStatement(sqlText string) string {
	trimmed := strings.TrimSpace(sqlText)
	trimmed = strings.TrimRightFunc(trimmed, func(r rune) bool {
		return unicode.IsSpace(r) || r == ';'
	})
	trimmed = strings.TrimSpace(trimmed)
	return strings.ToLower(strings.Join(strings.Fields(trimmed), " "))
}

func hasSQLPrefix(sqlText, prefix string) bool {
	return sqlText == prefix || strings.HasPrefix(sqlText, prefix+" ")
}

func splitSQLStatements(sqlText string) ([]string, bool) {
	stripped := stripSQLComments(sqlText)
	var statements []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	inBacktick := false

	for i := 0; i < len(stripped); i++ {
		ch := stripped[i]
		if inSingle {
			current.WriteByte(ch)
			if ch == '\\' && i+1 < len(stripped) {
				i++
				current.WriteByte(stripped[i])
				continue
			}
			if ch == '\'' {
				if i+1 < len(stripped) && stripped[i+1] == '\'' {
					i++
					current.WriteByte(stripped[i])
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			current.WriteByte(ch)
			if ch == '\\' && i+1 < len(stripped) {
				i++
				current.WriteByte(stripped[i])
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			current.WriteByte(ch)
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		if delimiter, found := postgresDollarQuoteDelimiter(stripped, i); found {
			closing := strings.Index(stripped[i+len(delimiter):], delimiter)
			if closing < 0 {
				return nil, false
			}
			end := i + len(delimiter) + closing + len(delimiter)
			current.WriteString(stripped[i:end])
			i = end - 1
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
			current.WriteByte(ch)
		case '"':
			inDouble = true
			current.WriteByte(ch)
		case '`':
			inBacktick = true
			current.WriteByte(ch)
		case ';':
			statement := strings.TrimSpace(current.String())
			if statement != "" {
				statements = append(statements, statement)
			}
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if inSingle || inDouble || inBacktick {
		return nil, false
	}
	statement := strings.TrimSpace(current.String())
	if statement != "" {
		statements = append(statements, statement)
	}
	if len(statements) == 0 {
		return nil, false
	}
	return statements, true
}

func stripSQLComments(sqlText string) string {
	var out strings.Builder
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		if inSingle {
			out.WriteByte(ch)
			if ch == '\\' && i+1 < len(sqlText) {
				i++
				out.WriteByte(sqlText[i])
				continue
			}
			if ch == '\'' {
				if i+1 < len(sqlText) && sqlText[i+1] == '\'' {
					i++
					out.WriteByte(sqlText[i])
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			out.WriteByte(ch)
			if ch == '\\' && i+1 < len(sqlText) {
				i++
				out.WriteByte(sqlText[i])
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			out.WriteByte(ch)
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		if delimiter, found := postgresDollarQuoteDelimiter(sqlText, i); found {
			closing := strings.Index(sqlText[i+len(delimiter):], delimiter)
			if closing < 0 {
				out.WriteString(sqlText[i:])
				break
			}
			end := i + len(delimiter) + closing + len(delimiter)
			out.WriteString(sqlText[i:end])
			i = end - 1
			continue
		}

		switch {
		case ch == '\'':
			inSingle = true
			out.WriteByte(ch)
		case ch == '"':
			inDouble = true
			out.WriteByte(ch)
		case ch == '`':
			inBacktick = true
			out.WriteByte(ch)
		case ch == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-' && (i+2 == len(sqlText) || isSQLCommentSpace(sqlText[i+2])):
			i += 2
			for i < len(sqlText) && sqlText[i] != '\n' {
				i++
			}
			if i < len(sqlText) {
				out.WriteByte('\n')
			}
		case ch == '#':
			for i < len(sqlText) && sqlText[i] != '\n' {
				i++
			}
			if i < len(sqlText) {
				out.WriteByte('\n')
			}
		case ch == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*':
			i += 2
			for i+1 < len(sqlText) && !(sqlText[i] == '*' && sqlText[i+1] == '/') {
				if sqlText[i] == '\n' {
					out.WriteByte('\n')
				} else {
					out.WriteByte(' ')
				}
				i++
			}
			if i+1 < len(sqlText) {
				i++
			}
		default:
			out.WriteByte(ch)
		}
	}
	return out.String()
}

func isSQLCommentSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n'
}

func containsSemicolonOutsideLiterals(sqlText string) bool {
	statements, ok := splitSQLStatements(sqlText)
	return ok && len(statements) > 1
}

func reviewPayload(fullSQL string, syntaxType int, rows []map[string]any, started time.Time, finished time.Time) map[string]any {
	warningCount := 0
	errorCount := 0
	var affectedRows int64
	for _, row := range rows {
		errLevel := intFromAny(row["errlevel"])
		if errLevel == 1 {
			warningCount++
		}
		if errLevel >= 2 {
			errorCount++
		}
		affectedRows += int64(intFromAny(row["affected_rows"]))
	}
	return map[string]any{
		"full_sql":          fullSQL,
		"checked":           errorCount == 0,
		"warning_count":     warningCount,
		"error_count":       errorCount,
		"is_critical":       false,
		"syntax_type":       syntaxType,
		"review_rows":       rows,
		"rows":              rows,
		"column_list":       []string{},
		"status":            "Audit completed",
		"affected_rows":     affectedRows,
		"execution_seconds": finished.Sub(started).Seconds(),
	}
}

func reviewRow(id int, stage string, errlevel int, stageStatus string, errorMessage string, sqlText string, affectedRows int64, executeSeconds float64, commandID int64) map[string]any {
	return map[string]any{
		"id":                   id,
		"stage":                stage,
		"errlevel":             errlevel,
		"stagestatus":          stageStatus,
		"errormessage":         errorMessage,
		"sql":                  sqlText,
		"affected_rows":        affectedRows,
		"sequence":             fmt.Sprintf("%d_0_0", id),
		"backup_dbname":        "",
		"execute_time":         executeSeconds,
		"sqlsha1":              sqlSHA1(sqlText),
		"backup_time":          "",
		"actual_affected_rows": affectedRows,
		"agent_command_id":     commandID,
	}
}

func sqlSHA1(sqlText string) string {
	sum := sha1.Sum([]byte(sqlText))
	return hex.EncodeToString(sum[:])
}

func stringValue(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func stringValueOrDefault(payload map[string]any, key, fallback string) string {
	if value := stringValue(payload, key); value != "" {
		return value
	}
	return fallback
}

func intValue(payload map[string]any, key string, fallback int) int {
	switch value := payload[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return fallback
	}
}

func boolValue(payload map[string]any, key string, fallback bool) bool {
	switch value := payload[key].(type) {
	case bool:
		return value
	case int:
		if value == 0 || value == 1 {
			return value == 1
		}
	case int64:
		if value == 0 || value == 1 {
			return value == 1
		}
	case float64:
		if value == 0 || value == 1 {
			return value == 1
		}
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func normalizeSQLValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return typed
	}
}
