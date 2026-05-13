package redact

import "regexp"

const replacement = "[REDACTED]"

var patterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(authorization:\s*bearer\s+)[^\s]+`),
	regexp.MustCompile(`(?i)(api[_-]?key\s*[=:]\s*)[^\s]+`),
	regexp.MustCompile(`(?i)(password\s*[=:]\s*)[^\s]+`),
	regexp.MustCompile(`(?i)(token\s*[=:]\s*)[^\s]+`),
	regexp.MustCompile(`(?i)(secret\s*[=:]\s*)[^\s]+`),
	regexp.MustCompile(`(?i)(mysql|postgres|postgresql)://[^:\s]+:[^@\s]+@`),
	regexp.MustCompile(`/run/datamingle-agent/[A-Za-z0-9._/-]*credentials[A-Za-z0-9._/-]*`),
}

func String(value string) string {
	redacted := value
	for _, pattern := range patterns {
		redacted = pattern.ReplaceAllString(redacted, "${1}"+replacement)
	}
	return redacted
}
