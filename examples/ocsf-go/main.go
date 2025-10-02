package main

import (
	"fmt"
	"ocsf-go/mappers"

	"ocsf-go/row"

	"github.com/vmihailenco/msgpack/v5"
)

type Processor struct{}

func (p Processor) ProcessRow(row row.Row) ([]any, error) {
	var out []any
	if row.Len() == 0 {
		return out, nil
	}

	if src, ok := row.String("source_type"); ok {
		switch src {
		case "docker_logs":
			var event mappers.DockerLog
			err := msgpack.Unmarshal(row.RawBytes, &event)
			if err != nil {
				return nil, err
			}
			mapped, err := mappers.EKSToOCSF(&event)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		case "syslog":
			var event mappers.SysLog
			err := msgpack.Unmarshal(row.RawBytes, &event)
			if err != nil {
				return nil, err
			}
			mapped, err := mappers.SyslogToOCSF(&event)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}
		return out, nil
	}

	if _, ok := row.String("eventType"); ok {
		var cloudtrailLog mappers.CloudtrailEvent
		err := msgpack.Unmarshal(row.RawBytes, &cloudtrailLog)
		if err != nil {
			return nil, err
		}
		mapped, err := mappers.CloudtrailToOCSF(&cloudtrailLog)
		if err != nil {
			return nil, err
		}
		out = append(out, mapped)
		return out, nil
	}

	return nil, fmt.Errorf("unknown log")
}
