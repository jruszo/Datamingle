package redact

import "regexp"

const replacement = "[REDACTED]"

type rule struct {
	pattern     *regexp.Regexp
	replacement string
}

var patterns = []rule{
	{regexp.MustCompile(`(?i)(authorization:\s*bearer\s+)[^\s]+`), "${1}" + replacement},
	{regexp.MustCompile(`(?i)(api[_-]?key\s*[=:]\s*)[^\s]+`), "${1}" + replacement},
	{regexp.MustCompile(`(?i)(password\s*[=:]\s*)[^\s]+`), "${1}" + replacement},
	{regexp.MustCompile(`(?i)(token\s*[=:]\s*)[^\s]+`), "${1}" + replacement},
	{regexp.MustCompile(`(?i)(secret\s*[=:]\s*)[^\s]+`), "${1}" + replacement},
	{regexp.MustCompile(`(?i)((?:mysql|postgres|postgresql)://)[^:\s]+:[^@\s]+@`), "${1}" + replacement + "@"},
	{regexp.MustCompile(`(/run/datamingle-agent/)[A-Za-z0-9._/-]*credentials[A-Za-z0-9._/-]*`), "${1}" + replacement},
}

func String(value string) string {
	redacted := value
	for _, rule := range patterns {
		redacted = rule.pattern.ReplaceAllString(redacted, rule.replacement)
	}
	return redacted
}
