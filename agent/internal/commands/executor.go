package commands

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode"

	"github.com/go-sql-driver/mysql"

	"github.com/jruszo/datamingle/agent/internal/client"
)

type Executor struct {
	now func() time.Time
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

func NewExecutor() *Executor {
	return &Executor{now: time.Now}
}

func (e *Executor) Execute(ctx context.Context, command client.AgentCommand, cfg client.AgentConfig) (Result, error) {
	assignment, ok := assignmentForCommand(command, cfg)
	if !ok {
		return Result{}, fmt.Errorf("instance %d is not assigned to this agent", command.InstanceID)
	}
	if assignment.DBType != "mysql" {
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
		return e.schemaChange(ctx, assignment, command, started)
	case "workflow.check":
		return e.workflowCheck(command, started)
	case "workflow.execute":
		return e.workflowExecute(ctx, assignment, command, started)
	case "export.check":
		return e.exportCheck(ctx, assignment, command, started)
	case "export.execute":
		return e.exportExecute(ctx, assignment, command, started)
	default:
		return Result{}, fmt.Errorf("unsupported command type %q", command.CommandType)
	}
}

func (e *Executor) inventoryCollect(ctx context.Context, assignment client.Assignment, started time.Time) (Result, error) {
	db, err := openMySQL(assignment, assignment.Database)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()

	var hostname string
	var version string
	if err := db.QueryRowContext(ctx, "SELECT @@hostname, VERSION()").Scan(&hostname, &version); err != nil {
		return Result{}, err
	}
	return Result{
		Message: "inventory collected",
		Payload: map[string]any{
			"hostname":          hostname,
			"version":           version,
			"execution_seconds": e.now().Sub(started).Seconds(),
		},
	}, nil
}

func assignmentForCommand(command client.AgentCommand, cfg client.AgentConfig) (client.Assignment, bool) {
	for _, assignment := range cfg.Assignments {
		if assignment.InstanceID == command.InstanceID {
			return assignment, true
		}
	}
	return client.Assignment{}, false
}

func (e *Executor) connectionTest(ctx context.Context, assignment client.Assignment, started time.Time) (Result, error) {
	db, err := openMySQL(assignment, assignment.Database)
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

func (e *Executor) schemaChange(ctx context.Context, assignment client.Assignment, command client.AgentCommand, started time.Time) (Result, error) {
	sqlText := stringValue(command.Payload, "sql")
	if sqlText == "" {
		return Result{}, fmt.Errorf("schema change command payload is missing sql")
	}
	executor := stringValueOrDefault(command.Payload, "executor", "direct")
	if executor != "direct" {
		return Result{}, fmt.Errorf("%s execution requires external online schema tooling, which is not installed yet", executor)
	}
	if !isSafeDDL(sqlText) {
		return Result{}, fmt.Errorf("schema.change only allows approved single-statement DDL")
	}
	db, err := openMySQL(assignment, stringValueOrDefault(command.Payload, "db_name", assignment.Database))
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if err := ensureWritableMySQL(ctx, db); err != nil {
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

func (e *Executor) workflowExecute(ctx context.Context, assignment client.Assignment, command client.AgentCommand, started time.Time) (Result, error) {
	sqlText := stringValue(command.Payload, "sql")
	if sqlText == "" {
		return Result{}, fmt.Errorf("workflow execute payload is missing sql")
	}
	executor := stringValueOrDefault(command.Payload, "executor", "direct")
	if executor != "direct" {
		return Result{}, fmt.Errorf("%s execution requires external online schema tooling, which is not installed yet", executor)
	}
	statements, ok := splitSQLStatements(sqlText)
	if !ok {
		return Result{}, fmt.Errorf("workflow SQL must contain at least one complete statement")
	}
	for _, statement := range statements {
		if classifyWorkflowSyntax(statement) == 0 {
			return Result{}, fmt.Errorf("workflow.execute only allows DDL and DML statements")
		}
	}

	db, err := openMySQL(assignment, stringValueOrDefault(command.Payload, "db_name", assignment.Database))
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if err := ensureWritableMySQL(ctx, db); err != nil {
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

	db, err := openMySQL(assignment, stringValueOrDefault(command.Payload, "db_name", assignment.Database))
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

func queryRows(ctx context.Context, assignment client.Assignment, database string, sqlText string, limit int) (queryData, error) {
	db, err := openMySQL(assignment, database)
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
	return hasSQLPrefix(normalized, "select") ||
		hasSQLPrefix(normalized, "explain") ||
		hasSQLPrefix(normalized, "describe") ||
		hasSQLPrefix(normalized, "desc") ||
		hasSQLPrefix(normalized, "show")
}

func isExportSQL(sqlText string) bool {
	normalized, ok := normalizeSingleSQLStatement(sqlText)
	return ok && hasSQLPrefix(normalized, "select")
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
	normalized := normalizeSQLStatement(sqlText)
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
