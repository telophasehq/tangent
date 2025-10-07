package main

import (
	"context"
	"errors"
	"fmt"
	"ocsf-go/internal/tangent/logs/processor"
	"ocsf-go/mappers"
)

type Processor struct{}

func (p Processor) ProcessLog(log map[string]any) (*LogOutput, error) {
	ctx := context.Background()
	if len(log) == 0 {
		return nil, nil
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
			prefix := "eks/"
			return &LogOutput{
				Data: mapped,
				Sinks: []processor.Sink{
					S3("s3_bucket", &prefix),
				},
			}, nil
		case "syslog":
			mapped, err := mappers.SyslogToOCSF(log)
			if err != nil {
				return nil, err
			}
			prefix := "syslog/"
			return &LogOutput{
				Data: mapped,
				Sinks: []processor.Sink{
					S3("s3_bucket", &prefix),
				},
			}, nil
		}
	} else if _, ok := log["eventType"]; ok {
		mapped, err := mappers.CloudtrailToOCSF(ctx, log)
		if err != nil {
			return nil, err
		}
		prefix := "cloudtrail/"
		return &LogOutput{
			Data: mapped,
			Sinks: []processor.Sink{
				S3("s3_bucket", &prefix),
			},
		}, nil
	} else {
		return nil, fmt.Errorf("unknown log: {%v}", log)
	}

	return nil, nil
}
