package main

import (
	"context"
	"errors"
	"ocsf-go/mappers"
)

type Processor struct{}

func (p Processor) ProcessLog(log map[string]any) ([]any, error) {
	var normalized []any
	ctx := context.Background()
	if _, ok := log["source_type"]; ok {
		sourceType, ok := log["source_type"].(string)
		if !ok {
			return nil, errors.New("source_type is not a string")
		}
		switch sourceType {
		case "docker_logs":
			mapped, err := mappers.EKSToOCSF(log)
			if err != nil {
				return nil, err
			}
			normalized = append(normalized, mapped)
		case "syslog":
			mapped, err := mappers.SyslogToOCSF(log)
			if err != nil {
				return nil, err
			}
			normalized = append(normalized, mapped)
		}
	}

	if _, ok := log["event_type"]; ok {
		mapped, err := mappers.CloudtrailToOCSF(ctx, log)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, mapped)
	}

	return normalized, nil
}
