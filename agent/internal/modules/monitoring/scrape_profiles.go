package monitoring

import (
	"net/url"
	"time"
)

type scrapeProfile struct {
	Name       string
	Interval   time.Duration
	Collectors []string
}

func parseScrapeProfiles(value any, fallbackInterval time.Duration) []scrapeProfile {
	profiles := []scrapeProfile{}
	for _, item := range anyList(value) {
		raw, _ := item.(map[string]any)
		name := stringValue(raw["name"])
		if name == "" {
			continue
		}
		interval := time.Duration(intValue(raw["interval_seconds"], int(fallbackInterval/time.Second))) * time.Second
		if interval <= 0 {
			interval = fallbackInterval
		}
		profiles = append(profiles, scrapeProfile{
			Name:       name,
			Interval:   interval,
			Collectors: stringList(raw["collectors"]),
		})
	}
	return profiles
}

func metricsURLWithCollectors(rawURL string, collectors []string) (string, error) {
	if len(collectors) == 0 {
		return rawURL, nil
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	for _, collector := range collectors {
		if collector == "" {
			continue
		}
		query.Add("collect[]", collector)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
