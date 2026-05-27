# Create And Install Agents

Agents must be created in Datamingle before installation.

## Create An Agent

1. Open `Agents`.
2. Select `Create Agent`.
3. Enter a name and optional description.
4. Save the agent.
5. Copy the generated install command.

## Install An Agent

Run the install command on the host or runtime environment that should operate on assigned database services.

Treat the command and API key as sensitive credentials. If a key is exposed, revoke the agent and create a replacement.

## Confirm Installation

After installation, wait for the agent to report `online`. If it stays pending or offline, check network connectivity and agent logs.

