package modules

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type fakeModule struct {
	name     string
	applied  int
	stopped  int
	applyErr error
	stopErr  error
}

func (m *fakeModule) Name() string           { return m.name }
func (m *fakeModule) Capabilities() []string { return []string{"test"} }
func (m *fakeModule) ApplyConfig(context.Context, Config) error {
	m.applied++
	return m.applyErr
}
func (m *fakeModule) Health(context.Context) Health {
	return Health{Module: m.name, Status: "healthy", UpdatedAt: time.Now()}
}
func (m *fakeModule) Stop(context.Context) error {
	m.stopped++
	return m.stopErr
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

func TestNewManagerRejectsDuplicateModuleNames(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected duplicate module name panic")
		}
	}()
	NewManager(&fakeModule{name: "logs"}, &fakeModule{name: "logs"})
}

func TestManagerRollsBackAppliedModuleOnApplyError(t *testing.T) {
	first := &fakeModule{name: "first"}
	second := &fakeModule{name: "second", applyErr: fmt.Errorf("boom")}
	manager := NewManager(first, second)

	err := manager.Apply(context.Background(), []Config{
		{Name: "first", Enabled: true},
		{Name: "second", Enabled: true},
	})

	if err == nil {
		t.Fatal("expected apply error")
	}
	if first.stopped != 1 {
		t.Fatalf("expected first module rollback stop, got %d", first.stopped)
	}
}
