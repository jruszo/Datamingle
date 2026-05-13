package client

import (
	"encoding/json"
	"fmt"
	"time"
)

type RegisterRequest struct {
	InstallID      string `json:"install_id"`
	Name           string `json:"name"`
	Hostname       string `json:"hostname"`
	Platform       string `json:"platform"`
	Architecture   string `json:"architecture"`
	AgentVersion   string `json:"agent_version"`
	ConfigRevision int64  `json:"config_revision"`
}

type RegisterResponse struct {
	AgentID               int64 `json:"agent_id"`
	DesiredConfigRevision int64 `json:"desired_config_revision"`
}

type AgentConfig struct {
	AgentID       int64          `json:"agent_id"`
	Revision      int64          `json:"revision"`
	Hash          string         `json:"config_hash"`
	Assignments   []Assignment   `json:"assignments,omitempty"`
	Modules       []ModuleConfig `json:"modules"`
	ToolArtifacts []ToolArtifact `json:"tool_artifacts,omitempty"`
	Raw           map[string]any `json:"raw,omitempty"`
	Fetched       time.Time      `json:"-"`
}

type ModuleConfig struct {
	Name        string         `json:"name"`
	Enabled     bool           `json:"enabled"`
	Revision    int64          `json:"revision"`
	Assignments []Assignment   `json:"assignments,omitempty"`
	Raw         map[string]any `json:"raw,omitempty"`
}

type Assignment struct {
	ID         int64  `json:"id"`
	InstanceID int64  `json:"instance_id"`
	Name       string `json:"instance_name,omitempty"`
	DBType     string `json:"db_type,omitempty"`
	Host       string `json:"host,omitempty"`
	Port       int    `json:"port,omitempty"`
	Username   string `json:"username,omitempty"`
	// Password is sensitive and must not be logged or printed.
	Password       string         `json:"password,omitempty"`
	Database       string         `json:"database,omitempty"`
	Charset        string         `json:"charset,omitempty"`
	SSL            SSLConfig      `json:"ssl"`
	Modules        []string       `json:"modules,omitempty"`
	Capabilities   []string       `json:"capabilities,omitempty"`
	CommandEnabled bool           `json:"command_enabled"`
	Raw            map[string]any `json:"raw,omitempty"`
}

type assignmentJSON Assignment

func (a Assignment) Redacted() Assignment {
	redacted := a
	if redacted.Password != "" {
		redacted.Password = "<redacted>"
	}
	return redacted
}

func (a Assignment) MarshalJSON() ([]byte, error) {
	redacted := assignmentJSON(a.Redacted())
	return json.Marshal(redacted)
}

func (a Assignment) String() string {
	return fmt.Sprintf("%+v", assignmentJSON(a.Redacted()))
}

type SSLConfig struct {
	Enabled bool `json:"enabled"`
	Verify  bool `json:"verify"`
}

type ToolArtifact struct {
	ID           int64  `json:"id"`
	ToolName     string `json:"tool_name"`
	Version      string `json:"version"`
	Platform     string `json:"platform"`
	Architecture string `json:"architecture"`
	DownloadURL  string `json:"download_url"`
	SHA256       string `json:"sha256"`
	SizeBytes    int64  `json:"size_bytes"`
}

type HeartbeatRequest struct {
	InstallID      string         `json:"install_id"`
	Status         string         `json:"status"`
	ConfigRevision int64          `json:"config_revision"`
	ModuleHealth   []ModuleHealth `json:"module_health"`
}

type ModuleHealth struct {
	Module    string         `json:"module"`
	Status    string         `json:"status"`
	Message   string         `json:"message,omitempty"`
	UpdatedAt time.Time      `json:"updated_at"`
	Details   map[string]any `json:"details,omitempty"`
}

type HeartbeatResponse struct {
	DesiredConfigRevision int64 `json:"desired_config_revision"`
}

type AgentCommand struct {
	ID              int64          `json:"id"`
	AgentID         int64          `json:"agent_id"`
	InstanceID      int64          `json:"instance_id"`
	CommandType     string         `json:"command_type"`
	WorkflowType    string         `json:"workflow_type"`
	WorkflowID      string         `json:"workflow_id"`
	Status          string         `json:"status"`
	IdempotencyKey  string         `json:"idempotency_key"`
	Payload         map[string]any `json:"payload"`
	CancelRequested bool           `json:"cancel_requested"`
}

type CommandAckResponse struct {
	Status string `json:"status"`
}

type CommandLeaseRequest struct {
	LeaseOwner   string `json:"lease_owner,omitempty"`
	LeaseSeconds int64  `json:"lease_seconds,omitempty"`
}

type CommandProgressRequest struct {
	LeaseOwner   string         `json:"lease_owner,omitempty"`
	LeaseSeconds int64          `json:"lease_seconds,omitempty"`
	Message      string         `json:"message,omitempty"`
	Payload      map[string]any `json:"payload,omitempty"`
}

type CommandFinishRequest struct {
	Message string         `json:"message,omitempty"`
	Result  map[string]any `json:"result,omitempty"`
}

type CommandFailRequest struct {
	Message string         `json:"message,omitempty"`
	Error   map[string]any `json:"error,omitempty"`
}

type CommandStatusResponse struct {
	Status string `json:"status"`
}
