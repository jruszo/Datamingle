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
	applyMu sync.Mutex
	mu      sync.RWMutex
	modules map[string]Module
	active  map[string]Config
}

func NewManager(mods ...Module) *Manager {
	manager := &Manager{
		modules: map[string]Module{},
		active:  map[string]Config{},
	}
	for _, module := range mods {
		name := module.Name()
		if _, exists := manager.modules[name]; exists {
			panic(fmt.Sprintf("duplicate module name %q", name))
		}
		manager.modules[name] = module
	}
	return manager
}

func (m *Manager) Apply(ctx context.Context, configs []Config) error {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	desired := map[string]Config{}
	toApply := map[string]Module{}
	backup := map[string]Config{}

	m.mu.RLock()
	for name, cfg := range m.active {
		backup[name] = cfg
	}
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		module, ok := m.modules[cfg.Name]
		if !ok {
			m.mu.RUnlock()
			return fmt.Errorf("unknown module %q", cfg.Name)
		}
		desired[cfg.Name] = cfg
		toApply[cfg.Name] = module
	}

	type namedModule struct {
		name   string
		module Module
	}
	toStop := make([]namedModule, 0)
	for name := range m.active {
		if _, ok := desired[name]; ok {
			continue
		}
		module, ok := m.modules[name]
		if !ok || module == nil {
			continue
		}
		toStop = append(toStop, namedModule{name: name, module: module})
	}
	m.mu.RUnlock()

	names := make([]string, 0, len(desired))
	for name := range desired {
		names = append(names, name)
	}
	sort.Strings(names)

	applied := make([]string, 0, len(names))
	for _, name := range names {
		if err := toApply[name].ApplyConfig(ctx, desired[name]); err != nil {
			m.rollback(ctx, applied, backup)
			return fmt.Errorf("apply module %s: %w", name, err)
		}
		applied = append(applied, name)
	}

	for _, item := range toStop {
		if err := item.module.Stop(ctx); err != nil {
			m.rollback(ctx, applied, backup)
			return fmt.Errorf("stop module %s: %w", item.name, err)
		}
		applied = append(applied, item.name)
	}

	m.mu.Lock()
	m.active = desired
	m.mu.Unlock()
	return nil
}

func (m *Manager) Health(ctx context.Context) []Health {
	type namedModule struct {
		name   string
		module Module
	}
	m.mu.RLock()
	modules := make([]namedModule, 0, len(m.active))
	for name := range m.active {
		module, ok := m.modules[name]
		if !ok || module == nil {
			continue
		}
		modules = append(modules, namedModule{name: name, module: module})
	}
	m.mu.RUnlock()

	sort.Slice(modules, func(i, j int) bool {
		return modules[i].name < modules[j].name
	})

	health := make([]Health, 0, len(modules))
	for _, item := range modules {
		health = append(health, item.module.Health(ctx))
	}
	return health
}

func (m *Manager) rollback(ctx context.Context, applied []string, backup map[string]Config) {
	modules := map[string]Module{}
	m.mu.RLock()
	for _, name := range applied {
		module, ok := m.modules[name]
		if !ok || module == nil {
			continue
		}
		modules[name] = module
	}
	m.mu.RUnlock()

	for _, name := range applied {
		module, ok := modules[name]
		if !ok {
			continue
		}
		if cfg, wasActive := backup[name]; wasActive {
			_ = module.ApplyConfig(ctx, cfg)
		} else {
			_ = module.Stop(ctx)
		}
	}
}
