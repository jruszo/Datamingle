package modules

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type fakeModule struct {
	name     string
	applied  int
	stopped  int
	applyErr error
	stopErr  error
	applyFn  func(context.Context, Config) error
	stopFn   func(context.Context) error
}

func (m *fakeModule) Name() string           { return m.name }
func (m *fakeModule) Capabilities() []string { return []string{"test"} }
func (m *fakeModule) ApplyConfig(ctx context.Context, cfg Config) error {
	m.applied++
	if m.applyFn != nil {
		return m.applyFn(ctx, cfg)
	}
	return m.applyErr
}
func (m *fakeModule) Health(context.Context) Health {
	return Health{Module: m.name, Status: "healthy", UpdatedAt: time.Now()}
}
func (m *fakeModule) Stop(ctx context.Context) error {
	m.stopped++
	if m.stopFn != nil {
		return m.stopFn(ctx)
	}
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

func TestManagerSkipsUnchangedActiveModule(t *testing.T) {
	module := &fakeModule{name: "node_monitoring"}
	manager := NewManager(module)
	cfg := Config{
		Name:     "node_monitoring",
		Enabled:  true,
		Revision: 3,
		Raw: map[string]any{
			"listen_address": "127.0.0.1:9100",
			"collectors":     []any{"cpu", "meminfo"},
		},
	}

	if err := manager.Apply(context.Background(), []Config{cfg}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Apply(context.Background(), []Config{cfg}); err != nil {
		t.Fatal(err)
	}

	if module.applied != 1 {
		t.Fatalf("expected unchanged config to be applied once, got %d", module.applied)
	}
	if module.stopped != 0 {
		t.Fatalf("expected unchanged config not to stop module, got %d", module.stopped)
	}
}

func TestManagerStopStopsActiveModulesAndClearsState(t *testing.T) {
	module := &fakeModule{name: "logs"}
	manager := NewManager(module)

	if err := manager.Apply(context.Background(), []Config{{Name: "logs", Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if module.stopped != 1 {
		t.Fatalf("expected active module to stop once, got %d", module.stopped)
	}

	if err := manager.Apply(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if module.stopped != 1 {
		t.Fatalf("expected stopped module to be inactive, got %d stops", module.stopped)
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

func TestManagerSurfacesRollbackErrors(t *testing.T) {
	first := &fakeModule{name: "first", stopErr: fmt.Errorf("rollback failed")}
	second := &fakeModule{name: "second", applyErr: fmt.Errorf("boom")}
	manager := NewManager(first, second)

	err := manager.Apply(context.Background(), []Config{
		{Name: "first", Enabled: true},
		{Name: "second", Enabled: true},
	})

	if err == nil {
		t.Fatal("expected apply error")
	}
	if !strings.Contains(err.Error(), "rollback stop module first") {
		t.Fatalf("expected rollback error in returned error, got %v", err)
	}
}

func TestManagerRollsBackWhenContextCancelledAfterApply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first := &fakeModule{
		name: "first",
		applyFn: func(context.Context, Config) error {
			cancel()
			return nil
		},
	}
	second := &fakeModule{name: "second"}
	manager := NewManager(first, second)

	err := manager.Apply(ctx, []Config{
		{Name: "first", Enabled: true},
		{Name: "second", Enabled: true},
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation error, got %v", err)
	}
	if first.stopped != 1 {
		t.Fatalf("expected applied module rollback stop, got %d", first.stopped)
	}
	if second.applied != 0 {
		t.Fatalf("expected cancellation before second apply, got %d applies", second.applied)
	}
}
