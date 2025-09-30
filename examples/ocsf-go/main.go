package main

import (
	"context"
	"errors"
	"fmt"
	"ocsf-go/mappers"
)

type Processor struct{}

func (p Processor) ProcessLog(log map[string]any) ([]any, error) {
	var normalized []any
	ctx := context.Background()
	if len(log) == 0 {
		return normalized, nil
	}
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
	} else if _, ok := log["eventType"]; ok {
		mapped, err := mappers.CloudtrailToOCSF(ctx, log)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, mapped)
	} else {
		return nil, fmt.Errorf("unknown log: {}", log)
	}

	return normalized, nil
}
