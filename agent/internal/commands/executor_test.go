package commands

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/jruszo/datamingle/agent/internal/client"
	"github.com/jruszo/datamingle/agent/internal/tools"
)

func TestExecuteRejectsUnassignedInstance(t *testing.T) {
	executor := NewExecutor()

	_, err := executor.Execute(
		context.Background(),
		client.AgentCommand{InstanceID: 42, CommandType: "connection.test"},
		client.AgentConfig{Assignments: []client.Assignment{{InstanceID: 7, DBType: "mysql"}}},
	)

	if err == nil {
		t.Fatal("expected unassigned instance error")
	}
}

func TestExecuteSupportsInventoryCollection(t *testing.T) {
	executor := NewExecutor()

	_, err := executor.Execute(
		context.Background(),
		client.AgentCommand{InstanceID: 42, CommandType: "inventory.collect"},
		client.AgentConfig{Assignments: []client.Assignment{{
			InstanceID: 42,
			DBType:     "mysql",
			Host:       "127.0.0.1",
			Port:       1,
		}}},
	)

	if err == nil {
		t.Fatal("expected connection error")
	}
	if strings.Contains(err.Error(), "unsupported command type") {
		t.Fatalf("inventory command was not dispatched: %v", err)
	}
}

func TestBuildMySQLTopologyPayloadUsesReplicaStatusAliases(t *testing.T) {
	payload := buildMySQLTopologyPayload(
		"server-uuid",
		boolPtr(true),
		boolPtr(false),
		map[string]string{
			"Master_Host": "10.0.0.10",
			"Master_Port": "3307",
		},
		[]map[string]string{
			{
				"MEMBER_HOST":  "10.0.0.11",
				"MEMBER_PORT":  "3306",
				"MEMBER_ROLE":  "SECONDARY",
				"MEMBER_STATE": "ONLINE",
			},
		},
	)

	if payload["server_uuid"] != "server-uuid" {
		t.Fatalf("expected server_uuid in payload: %#v", payload)
	}
	if payload["read_only"] != true {
		t.Fatalf("expected read_only true in payload: %#v", payload)
	}
	if payload["super_read_only"] != false {
		t.Fatalf("expected super_read_only false in payload: %#v", payload)
	}
	if payload["source_host"] != "10.0.0.10" {
		t.Fatalf("expected source_host from Master_Host: %#v", payload)
	}
	if payload["source_port"] != 3307 {
		t.Fatalf("expected source_port from Master_Port: %#v", payload)
	}
	members, ok := payload["group_replication_members"].([]map[string]string)
	if !ok || len(members) != 1 || members[0]["member_role"] != "SECONDARY" {
		t.Fatalf("expected normalized group replication members: %#v", payload)
	}
	if topologyStringFromAny([]byte("uuid-from-bytes")) != "uuid-from-bytes" {
		t.Fatal("expected byte-slice string values to be normalized")
	}
	if topologyIntFromAny([]byte("1")) != 1 {
		t.Fatal("expected byte-slice integer values to be normalized")
	}
}

func TestBuildMySQLTopologyPayloadOmitsUnknownValues(t *testing.T) {
	payload := buildMySQLTopologyPayload(
		"",
		nil,
		nil,
		map[string]string{},
		nil,
	)

	if _, ok := payload["server_uuid"]; ok {
		t.Fatalf("expected unknown server_uuid to be omitted: %#v", payload)
	}
	if _, ok := payload["read_only"]; ok {
		t.Fatalf("expected unknown read_only to be omitted: %#v", payload)
	}
	if _, ok := payload["super_read_only"]; ok {
		t.Fatalf("expected unknown super_read_only to be omitted: %#v", payload)
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func TestMissingGroupReplicationTableErrorIsSuppressed(t *testing.T) {
	if !isMissingMySQLTableError(&mysql.MySQLError{Number: 1146}) {
		t.Fatal("expected missing table errors to be suppressed")
	}
	if isMissingMySQLTableError(&mysql.MySQLError{Number: 1045}) {
		t.Fatal("expected access denied errors to remain warnings")
	}
}

func TestMySQLTopologyWarningDoesNotExposeRawError(t *testing.T) {
	warnings := appendMySQLTopologyWarning(nil, "server_uuid_unavailable", errors.New("access denied for password=secret"))

	if len(warnings) != 1 {
		t.Fatalf("expected one warning, got %#v", warnings)
	}
	if warnings[0] != "server_uuid_unavailable" {
		t.Fatalf("expected stable warning code, got %q", warnings[0])
	}
	if strings.Contains(warnings[0], "secret") || strings.Contains(warnings[0], "access denied") {
		t.Fatalf("warning exposed raw database error: %q", warnings[0])
	}
}

func TestIsReadOnlySQL(t *testing.T) {
	for _, sqlText := range []string{
		"select 1",
		"-- leading comment\nselect 1",
		"/* leading comment */ SELECT 1",
		"EXPLAIN select 1",
		"describe users",
		"select ';' as semicolon",
		"show databases",
		"show create table users",
	} {
		if !isReadOnlySQL(sqlText) {
			t.Fatalf("expected %q to be read-only", sqlText)
		}
	}
	for _, sqlText := range []string{
		"update users set name = 'x'",
		"delete from users",
		"alter table users add column x int",
		"/* hide intent */ update users set name = 'x'",
		"select 1; update users set name = 'x'",
		"select 1; select 2",
		"show grants",
	} {
		if isReadOnlySQL(sqlText) {
			t.Fatalf("expected %q to be rejected", sqlText)
		}
	}
}

func TestClassifyWorkflowSyntax(t *testing.T) {
	for _, sqlText := range []string{
		"create table users (id int)",
		"alter table users add column name varchar(255)",
		"drop table users",
		"rename table users to app_users",
		"truncate table users",
	} {
		if classifyWorkflowSyntax(sqlText) != 1 {
			t.Fatalf("expected %q to be DDL", sqlText)
		}
	}
	for _, sqlText := range []string{
		"insert into users(id) values (1)",
		"update users set name = 'x'",
		"delete from users where id = 1",
		"replace into users(id) values (1)",
	} {
		if classifyWorkflowSyntax(sqlText) != 2 {
			t.Fatalf("expected %q to be DML", sqlText)
		}
	}
	for _, sqlText := range []string{
		"select * from users",
		"drop database prod",
		"grant select on *.* to 'u'@'%'",
	} {
		if classifyWorkflowSyntax(sqlText) != 0 {
			t.Fatalf("expected %q to be rejected", sqlText)
		}
	}
}

func TestIsSafeDDL(t *testing.T) {
	for _, sqlText := range []string{
		"create table users (id int)",
		"alter table users add column name varchar(255)",
		"create index idx_users_name on users (name)",
		"drop index idx_users_name on users",
	} {
		if !isSafeDDL(sqlText) {
			t.Fatalf("expected %q to be safe DDL", sqlText)
		}
	}
	for _, sqlText := range []string{
		"drop table users",
		"drop database prod",
		"truncate table users",
		"grant select on *.* to 'u'@'%'",
		"create table users (id int); drop table users",
	} {
		if isSafeDDL(sqlText) {
			t.Fatalf("expected %q to be rejected", sqlText)
		}
	}
}

func TestWorkflowExecuteUsesGhostArtifactForDDL(t *testing.T) {
	cacheDir := t.TempDir()
	artifact := client.ToolArtifact{
		ToolName:     "gh-ost",
		Version:      "1.1.6",
		Platform:     runtime.GOOS,
		Architecture: runtime.GOARCH,
		DownloadURL:  "https://example.com/gh-ost",
		SHA256:       sha256Hex("ghost"),
	}
	path := tools.ArtifactPath(cacheDir, tools.Artifact{
		ToolName:     artifact.ToolName,
		Version:      artifact.Version,
		Platform:     artifact.Platform,
		Architecture: artifact.Architecture,
		DownloadURL:  artifact.DownloadURL,
		SHA256:       artifact.SHA256,
	})
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	var captured [][]string
	executor := NewExecutorWithToolCache(cacheDir)
	executor.runCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		captured = append(captured, append([]string{name}, args...))
		return []byte("ok"), nil
	}

	result, err := executor.Execute(
		context.Background(),
		client.AgentCommand{
			ID:          77,
			InstanceID:  42,
			CommandType: "workflow.execute",
			Payload: map[string]any{
				"db_name":  "app",
				"sql":      "alter table users add column nickname varchar(64)",
				"executor": "gh-ost",
			},
		},
		client.AgentConfig{
			Assignments: []client.Assignment{{
				InstanceID: 42,
				DBType:     "mysql",
				Host:       "127.0.0.1",
				Port:       3306,
				Username:   "root",
				Password:   "secret",
			}},
			ToolArtifacts: []client.ToolArtifact{artifact},
		},
	)

	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != 1 {
		t.Fatalf("expected one gh-ost command, got %d", len(captured))
	}
	if captured[0][0] != path {
		t.Fatalf("expected cached gh-ost path %q, got %q", path, captured[0][0])
	}
	joined := strings.Join(captured[0], " ")
	if strings.Contains(joined, "secret") || strings.Contains(joined, "--password") {
		t.Fatalf("expected command arguments not to expose password, got %q", joined)
	}
	for _, expected := range []string{
		"--database=app",
		"--table=users",
		"--alter=add column nickname varchar(64)",
		"--execute",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("expected command to include %q, got %q", expected, joined)
		}
	}
	rows := result.Payload["execute_rows"].([]map[string]any)
	if rows[0]["executor"] != "gh-ost" {
		t.Fatalf("expected result row executor gh-ost, got %#v", rows[0]["executor"])
	}
}

func TestWorkflowExecuteRejectsMixedOnlineSchemaBatchBeforeRunningTools(t *testing.T) {
	cacheDir := t.TempDir()
	artifact := client.ToolArtifact{
		ToolName:     "gh-ost",
		Version:      "1.1.6",
		Platform:     runtime.GOOS,
		Architecture: runtime.GOARCH,
		DownloadURL:  "https://example.com/gh-ost",
		SHA256:       sha256Hex("ghost"),
	}
	path := tools.ArtifactPath(cacheDir, tools.Artifact{
		ToolName:     artifact.ToolName,
		Version:      artifact.Version,
		Platform:     artifact.Platform,
		Architecture: artifact.Architecture,
		DownloadURL:  artifact.DownloadURL,
		SHA256:       artifact.SHA256,
	})
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	var captured [][]string
	executor := NewExecutorWithToolCache(cacheDir)
	executor.runCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		captured = append(captured, append([]string{name}, args...))
		return []byte("ok"), nil
	}

	_, err := executor.Execute(
		context.Background(),
		client.AgentCommand{
			ID:          77,
			InstanceID:  42,
			CommandType: "workflow.execute",
			Payload: map[string]any{
				"db_name": "app",
				"sql": strings.Join([]string{
					"alter table users add column nickname varchar(64)",
					"update users set nickname = 'x'",
				}, ";"),
				"executor": "gh-ost",
			},
		},
		client.AgentConfig{
			Assignments: []client.Assignment{{
				InstanceID: 42,
				DBType:     "mysql",
				Host:       "127.0.0.1",
				Port:       3306,
				Username:   "root",
				Password:   "secret",
			}},
			ToolArtifacts: []client.ToolArtifact{artifact},
		},
	)

	if err == nil || !strings.Contains(err.Error(), "only supports DDL statements") {
		t.Fatalf("expected non-DDL batch error, got %v", err)
	}
	if len(captured) != 0 {
		t.Fatalf("expected no tool invocations, got %d", len(captured))
	}
}

func TestRunCommandReturnsOnlyBoundedOutputTailOnFailure(t *testing.T) {
	executor := NewExecutor()
	output, err := executor.runCommand(
		context.Background(),
		"sh",
		"-c",
		"printf 'prefix'; head -c 131072 /dev/zero | tr '\\0' 'x'; printf 'tail'; exit 7",
	)

	if err == nil {
		t.Fatal("expected command failure")
	}
	if len(output) > 64*1024 {
		t.Fatalf("expected bounded command output, got %d bytes", len(output))
	}
	if strings.Contains(string(output), "prefix") {
		t.Fatalf("expected old output prefix to be trimmed, got %q", string(output[:16]))
	}
	if !strings.HasSuffix(string(output), "tail") {
		t.Fatalf("expected output tail to be retained, got suffix %q", string(output[len(output)-8:]))
	}
}

func TestWorkflowExecuteReportsMissingOnlineSchemaArtifact(t *testing.T) {
	executor := NewExecutorWithToolCache(t.TempDir())

	_, err := executor.Execute(
		context.Background(),
		client.AgentCommand{
			InstanceID:  42,
			CommandType: "workflow.execute",
			Payload: map[string]any{
				"db_name":  "app",
				"sql":      "alter table users add column nickname varchar(64)",
				"executor": "pt-osc",
			},
		},
		client.AgentConfig{
			Assignments: []client.Assignment{{
				InstanceID: 42,
				DBType:     "mysql",
			}},
		},
	)

	if err == nil || !strings.Contains(err.Error(), "pt-online-schema-change artifact is not available") {
		t.Fatalf("expected missing artifact error, got %v", err)
	}
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func filepathDir(path string) string {
	index := strings.LastIndex(path, string(os.PathSeparator))
	if index == -1 {
		return "."
	}
	return path[:index]
}
