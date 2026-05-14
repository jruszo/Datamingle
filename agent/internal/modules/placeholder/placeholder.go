package placeholder

import (
	"context"
	"sync"
	"time"

	"github.com/jruszo/datamingle/agent/internal/modules"
)

type Module struct {
	mu           sync.RWMutex
	name         string
	capabilities []string
	enabled      bool
	revision     int64
}

func New(name string, capabilities []string) *Module {
	return &Module{name: name, capabilities: capabilities}
}

func (m *Module) Name() string {
	return m.name
}

func (m *Module) Capabilities() []string {
	return append([]string(nil), m.capabilities...)
}

func (m *Module) ApplyConfig(ctx context.Context, cfg modules.Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.enabled = cfg.Enabled
	m.revision = cfg.Revision
	return nil
}

func (m *Module) Health(ctx context.Context) modules.Health {
	m.mu.RLock()
	enabled := m.enabled
	revision := m.revision
	m.mu.RUnlock()

	status := "disabled"
	message := "module placeholder is waiting for implementation"
	if enabled {
		status = "degraded"
	}
	return modules.Health{
		Module:    m.Name(),
		Status:    status,
		Message:   message,
		UpdatedAt: time.Now().UTC(),
		Details: map[string]any{
			"revision": revision,
		},
	}
}

func (m *Module) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.enabled = false
	return nil
}
