package modules

import (
	"context"
	"testing"
	"time"
)

type fakeModule struct {
	name    string
	applied int
	stopped int
}

func (m *fakeModule) Name() string           { return m.name }
func (m *fakeModule) Capabilities() []string { return []string{"test"} }
func (m *fakeModule) ApplyConfig(context.Context, Config) error {
	m.applied++
	return nil
}
func (m *fakeModule) Health(context.Context) Health {
	return Health{Module: m.name, Status: "healthy", UpdatedAt: time.Now()}
}
func (m *fakeModule) Stop(context.Context) error {
	m.stopped++
	return nil
}

func TestManagerAppliesEnabledModulesAndStopsRemovedModules(t *testing.T) {
	module := &fakeModule{name: "logs"}
	manager := NewManager(module)

	if err := manager.Apply(context.Background(), []Config{{Name: "logs", Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	if module.applied != 1 {
		t.Fatalf("expected apply count 1, got %d", module.applied)
	}

	if err := manager.Apply(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if module.stopped != 1 {
		t.Fatalf("expected stop count 1, got %d", module.stopped)
	}
}

func TestManagerRejectsUnknownEnabledModule(t *testing.T) {
	manager := NewManager()

	if err := manager.Apply(context.Background(), []Config{{Name: "missing", Enabled: true}}); err == nil {
		t.Fatal("expected unknown module error")
	}
}
