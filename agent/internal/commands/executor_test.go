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
		"-- leading comment\nselect 1",
		"/* leading comment */ SELECT 1",
		"EXPLAIN select 1",
		"describe users",
		"select ';' as semicolon",
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
		"show databases",
		"show grants",
	} {
		if isReadOnlySQL(sqlText) {
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
