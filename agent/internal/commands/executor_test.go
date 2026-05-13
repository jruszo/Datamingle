package commands

import (
	"context"
	"testing"

	"github.com/jruszo/datamingle/agent/internal/client"
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

func TestIsReadOnlySQL(t *testing.T) {
	for _, sqlText := range []string{
		"select 1",
		" show databases",
		"EXPLAIN select 1",
		"describe users",
	} {
		if !isReadOnlySQL(sqlText) {
			t.Fatalf("expected %q to be read-only", sqlText)
		}
	}
	for _, sqlText := range []string{
		"update users set name = 'x'",
		"delete from users",
		"alter table users add column x int",
	} {
		if isReadOnlySQL(sqlText) {
			t.Fatalf("expected %q to be rejected", sqlText)
		}
	}
}
