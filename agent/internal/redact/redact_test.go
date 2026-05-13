package redact

import (
	"strings"
	"testing"
)

func TestStringRedactsSensitiveValues(t *testing.T) {
	input := "Authorization: Bearer sk_live_123 password=supersecret mysql://user:pass@example/db"
	output := String(input)

	for _, leaked := range []string{"sk_live_123", "supersecret", "user:pass"} {
		if strings.Contains(output, leaked) {
			t.Fatalf("expected %q to be redacted in %q", leaked, output)
		}
	}
	if !strings.Contains(output, "mysql://[REDACTED]@example/db") {
		t.Fatalf("expected DSN prefix and host to be preserved in %q", output)
	}
	pathOutput := String("/run/datamingle-agent/mysql-credentials-prod.cnf")
	if pathOutput != "/run/datamingle-agent/[REDACTED]" {
		t.Fatalf("unexpected credential path redaction %q", pathOutput)
	}
}
