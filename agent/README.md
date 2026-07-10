# Datamingle Agent

Go-based host agent for Datamingle.

This first implementation slice includes the local runtime foundation:

- config loading and validation
- API key lookup through the default/example `DATAMINGLE_AGENT_API_KEY` environment variable, configurable with `api_key_env`
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

## Package

Build the Linux AMD64 agent package with its verified gh-ost,
pt-online-schema-change, and pt-archiver executables:

```bash
cd agent
./packaging/build-package.sh
```

The archive is written to `agent/dist/`. Its `data/tools` directory uses the
same versioned layout as the agent runtime cache, so it can be installed below
the configured agent base directory without another tool download. Percona's
tools require Perl DBI and DBD::mysql on the agent host (`libdbi-perl` and
`libdbd-mysql-perl` on Debian or Ubuntu).

## Example Config

```yaml
datamingle_url: https://datamingle.example.com
api_key_env: DATAMINGLE_AGENT_API_KEY
agent_name: prod-db-agent-01
data_dir: /var/lib/datamingle-agent
log_dir: /var/log/datamingle-agent
runtime_dir: /run/datamingle-agent
```

The API key is read from the environment named by `api_key_env` and is never
written to the config file. `DATAMINGLE_AGENT_API_KEY` is the default/example
name, not a required variable name.
