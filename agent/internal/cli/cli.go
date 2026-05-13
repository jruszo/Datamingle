package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jruszo/datamingle/agent/internal/config"
	"github.com/jruszo/datamingle/agent/internal/runtime"
	"github.com/jruszo/datamingle/agent/internal/secrets"
	"github.com/jruszo/datamingle/agent/internal/version"
)

// Run executes the datamingle-agent CLI and returns a process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "run":
		return runCommand(args[1:], stdout, stderr)
	case "status":
		return statusCommand(args[1:], stdout, stderr)
	case "config":
		return configCommand(args[1:], stdout, stderr)
	case "doctor":
		return doctorCommand(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "datamingle-agent %s\n", version.Version)
		return 0
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func runCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", config.DefaultConfigPath, "path to agent config")
	once := flags.Bool("once", false, "run one registration/config/heartbeat pass")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	cfg, err := loadAndValidateConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runner := runtime.NewRunner(cfg)
	if *once {
		if err := runner.RunOnce(ctx); err != nil {
			fmt.Fprintf(stderr, "agent run failed: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, "agent run completed")
		return 0
	}

	if err := runner.Run(ctx); err != nil {
		fmt.Fprintf(stderr, "agent stopped with error: %v\n", err)
		return 1
	}
	return 0
}

func statusCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", config.DefaultConfigPath, "path to agent config")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	cfg, err := loadAndValidateConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: %v\n", err)
		return 1
	}

	installID, err := secrets.LoadInstallID(cfg.DataDir)
	if err != nil {
		fmt.Fprintf(stdout, "status: not registered\n")
		fmt.Fprintf(stdout, "install_id: unavailable (%v)\n", err)
		return 0
	}

	fmt.Fprintf(stdout, "status: configured\n")
	fmt.Fprintf(stdout, "install_id: %s\n", installID)
	fmt.Fprintf(stdout, "datamingle_url: %s\n", cfg.DatamingleURL)
	return 0
}

func configCommand(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "missing config subcommand")
		return 2
	}
	switch args[0] {
	case "check":
		flags := flag.NewFlagSet("config check", flag.ContinueOnError)
		flags.SetOutput(stderr)
		configPath := flags.String("config", config.DefaultConfigPath, "path to agent config")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if _, err := loadAndValidateConfig(*configPath); err != nil {
			fmt.Fprintf(stderr, "configuration error: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, "configuration ok")
		return 0
	default:
		fmt.Fprintf(stderr, "unknown config subcommand %q\n", args[0])
		return 2
	}
}

func doctorCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", config.DefaultConfigPath, "path to agent config")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	cfg, err := loadAndValidateConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: %v\n", err)
		return 1
	}

	fmt.Fprintln(stdout, "configuration: ok")
	if _, ok := os.LookupEnv(cfg.APIKeyEnv); !ok {
		fmt.Fprintf(stderr, "api key: missing environment variable %s\n", cfg.APIKeyEnv)
		return 1
	}
	fmt.Fprintln(stdout, "api key: present")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.NewRunner(cfg).CheckConnectivity(ctx); err != nil {
		fmt.Fprintf(stderr, "connectivity: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "connectivity: ok")
	return 0
}

func loadAndValidateConfig(path string) (config.Config, error) {
	cfg, err := config.LoadFile(path)
	if err != nil {
		return config.Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return config.Config{}, err
	}
	return cfg, nil
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: datamingle-agent <command> [options]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "commands:")
	fmt.Fprintln(w, "  run              start the agent")
	fmt.Fprintln(w, "  status           show local agent state")
	fmt.Fprintln(w, "  config check     validate the config file")
	fmt.Fprintln(w, "  doctor           validate local config and backend reachability")
	fmt.Fprintln(w, "  version          print the agent version")
}
