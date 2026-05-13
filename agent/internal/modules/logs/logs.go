package logs

import (
	"context"
	"time"

	"github.com/jruszo/datamingle/agent/internal/modules"
)

// Module is a disabled placeholder for the future logs pipeline.
type Module struct {
	enabled bool
}

func New() *Module {
	return &Module{}
}

func (m *Module) Name() string {
	return "logs"
}

func (m *Module) Capabilities() []string {
	return nil
}

func (m *Module) ApplyConfig(ctx context.Context, cfg modules.Config) error {
	m.enabled = cfg.Enabled
	return nil
}

func (m *Module) Health(ctx context.Context) modules.Health {
	status := "disabled"
	if m.enabled {
		status = "degraded"
	}
	return modules.Health{
		Module:    m.Name(),
		Status:    status,
		Message:   "logs module is a V1 placeholder",
		UpdatedAt: time.Now().UTC(),
	}
}

func (m *Module) Stop(ctx context.Context) error {
	m.enabled = false
	return nil
}
