package main

import (
	"encoding/json"
	"fmt"

	"ocsf-go/internal/tangent/logs/processor"
	"ocsf-go/mappers"
	"ocsf-go/tangenthelpers"
)

type Processor struct{}

func (p Processor) ProcessLog(log []byte) (*LogOutput, error) {
	if len(log) == 0 {
		return nil, nil
	}

	if tangenthelpers.Has(log, "detail-type") {
		findings, err := mappers.UnpackSHFindings(log)
		if err != nil {
			return nil, err
		}

		if len(findings) == 0 {
			return nil, nil
		}
		var findings_encoded []json.RawMessage
		for idx := range findings {
			finding_enc, err := tangenthelpers.ToRaw(findings[idx])
			if err != nil {
				return nil, err
			}

			findings_encoded = append(findings_encoded, finding_enc)
		}
		prefix := "securityhub/"
		return &LogOutput{
			Items: findings_encoded,
			Sinks: []processor.Sink{S3("s3_bucket", &prefix)},
		}, nil
	}

	if src, isString := tangenthelpers.GetString(log, "source_type"); isString {
		switch src {
		case "docker_logs":
			mapped, err := mappers.EKSToOCSF(log)
			if err != nil {
				return nil, err
			}
			encoded, err := tangenthelpers.ToRaw(mapped)
			if err != nil {
				return nil, err
			}

			prefix := "eks/"
			return &LogOutput{
				Items: []json.RawMessage{encoded},
				Sinks: []processor.Sink{S3("s3_bucket", &prefix)},
			}, nil

		case "syslog":
			mapped, err := mappers.SyslogToOCSF(log)
			if err != nil {
				return nil, err
			}

			encoded, err := tangenthelpers.ToRaw(mapped)
			if err != nil {
				return nil, err
			}
			prefix := "syslog/"
			return &LogOutput{
				Items: []json.RawMessage{encoded},
				Sinks: []processor.Sink{S3("s3_bucket", &prefix)},
			}, nil

		default:
			return nil, fmt.Errorf("unknown source_type %q", src)
		}
	}

	if tangenthelpers.Has(log, "eventType") {
		mapped, err := mappers.CloudtrailToOCSF(log)
		if err != nil {
			return nil, err
		}
		encoded, err := tangenthelpers.ToRaw(mapped)
		if err != nil {
			return nil, err
		}

		prefix := "cloudtrail/"
		return &LogOutput{
			Items: []json.RawMessage{encoded},
			Sinks: []processor.Sink{S3("s3_bucket", &prefix)},
		}, nil
	}

	return nil, fmt.Errorf("unrecognized log shape")
}
