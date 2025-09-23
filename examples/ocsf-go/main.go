package main

import (
	"context"
	"ocsf-go/mappers"
)

type Processor struct{}

func (p Processor) ProcessLogs(logs []map[string]any) ([]any, error) {
	var normalized []any
	ctx := context.Background()
	for _, log := range logs {
		if sourceType, ok := log["source_type"].(string); ok {
			switch sourceType {
			case "docker_logs":
				mapped, err := mappers.EKSToOCSF(log)
				if err != nil {
					log["__error"] = err.Error()
				}
				normalized = append(normalized, mapped)
			case "syslog":
				mapped, err := mappers.SyslogToOCSF(log)
				if err != nil {
					log["__error"] = err.Error()
				}
				normalized = append(normalized, mapped)
			}
		}

		if _, ok := log["event_type"]; ok {
			mapped, err := mappers.CloudtrailToOCSF(ctx, log)
			if err != nil {
				log["__error"] = err.Error()
			}
			normalized = append(normalized, mapped)
		}
	}
	return normalized, nil
}

func init() {
	Wire(Processor{})
}
