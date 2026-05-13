# Datamingle Agent

Go-based host agent for Datamingle.

This first implementation slice includes the local runtime foundation:

- config loading and validation
- API key lookup through `DATAMINGLE_AGENT_API_KEY`
- durable install ID creation
- registration/config/heartbeat REST client
- module reconciliation interfaces
- checksum verification for future tool artifacts
- secret redaction helpers
- websocket notifications with reconnect backoff
- config refresh and command fetch/ack dispatch handling
- `datamingle-agent` CLI entrypoint

## Development

```bash
cd agent
go test ./...
go run ./cmd/datamingle-agent version
```

## Example Config

```yaml
datamingle_url: https://datamingle.example.com
api_key_env: DATAMINGLE_AGENT_API_KEY
agent_name: prod-db-agent-01
data_dir: /var/lib/datamingle-agent
log_dir: /var/log/datamingle-agent
runtime_dir: /run/datamingle-agent
```

The API key is read from the environment and is never written to the config
file.
