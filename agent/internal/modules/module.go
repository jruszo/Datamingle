package modules

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Config struct {
	Name     string
	Enabled  bool
	Revision int64
	Raw      map[string]any
}

type Health struct {
	Module    string
	Status    string
	Message   string
	UpdatedAt time.Time
	Details   map[string]any
}

type Module interface {
	Name() string
	Capabilities() []string
	ApplyConfig(ctx context.Context, cfg Config) error
	Health(ctx context.Context) Health
	Stop(ctx context.Context) error
}

type Manager struct {
	mu      sync.Mutex
	modules map[string]Module
	active  map[string]Config
}

func NewManager(mods ...Module) *Manager {
	manager := &Manager{
		modules: map[string]Module{},
		active:  map[string]Config{},
	}
	for _, module := range mods {
		manager.modules[module.Name()] = module
	}
	return manager
}

func (m *Manager) Apply(ctx context.Context, configs []Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	desired := map[string]Config{}
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		module, ok := m.modules[cfg.Name]
		if !ok {
			return fmt.Errorf("unknown module %q", cfg.Name)
		}
		if err := module.ApplyConfig(ctx, cfg); err != nil {
			return fmt.Errorf("apply module %s: %w", cfg.Name, err)
		}
		desired[cfg.Name] = cfg
	}

	for name := range m.active {
		if _, ok := desired[name]; ok {
			continue
		}
		if err := m.modules[name].Stop(ctx); err != nil {
			return fmt.Errorf("stop module %s: %w", name, err)
		}
	}

	m.active = desired
	return nil
}

func (m *Manager) Health(ctx context.Context) []Health {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make([]string, 0, len(m.active))
	for name := range m.active {
		names = append(names, name)
	}
	sort.Strings(names)

	health := make([]Health, 0, len(names))
	for _, name := range names {
		health = append(health, m.modules[name].Health(ctx))
	}
	return health
}
