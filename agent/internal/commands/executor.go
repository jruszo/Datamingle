package commands

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

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
	case "query.execute":
		return e.queryExecute(ctx, assignment, command, started)
	case "schema.change":
		return e.schemaChange(ctx, assignment, command, started)
	default:
		return Result{}, fmt.Errorf("unsupported command type %q", command.CommandType)
	}
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

	db, err := openMySQL(assignment, stringValueOrDefault(command.Payload, "db_name", assignment.Database))
	if err != nil {
		return Result{}, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return Result{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return Result{}, err
	}
	values := make([]any, len(columns))
	valuePointers := make([]any, len(columns))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	resultRows := make([]map[string]any, 0, min(limit, 100))
	for rows.Next() {
		if len(resultRows) >= limit {
			break
		}
		if err := rows.Scan(valuePointers...); err != nil {
			return Result{}, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = normalizeSQLValue(values[i])
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return Result{}, err
	}
	return Result{
		Message: "query executed",
		Payload: map[string]any{
			"columns":           columns,
			"rows":              resultRows,
			"row_count":         len(resultRows),
			"execution_seconds": e.now().Sub(started).Seconds(),
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
	db, err := openMySQL(assignment, stringValueOrDefault(command.Payload, "db_name", assignment.Database))
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	result, err := db.ExecContext(ctx, sqlText)
	if err != nil {
		return Result{}, err
	}
	affectedRows, _ := result.RowsAffected()
	return Result{
		Message: "schema change executed",
		Payload: map[string]any{
			"affected_rows":     affectedRows,
			"execution_seconds": e.now().Sub(started).Seconds(),
		},
	}, nil
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

func isReadOnlySQL(sqlText string) bool {
	trimmed := strings.TrimSpace(strings.TrimRight(sqlText, ";"))
	lowered := strings.ToLower(trimmed)
	return strings.HasPrefix(lowered, "select ") ||
		strings.HasPrefix(lowered, "show ") ||
		strings.HasPrefix(lowered, "explain ") ||
		strings.HasPrefix(lowered, "describe ") ||
		strings.HasPrefix(lowered, "desc ")
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
