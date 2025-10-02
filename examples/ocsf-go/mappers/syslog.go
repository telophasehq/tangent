package mappers

import (
	"time"

	"github.com/Santiago-Labs/go-ocsf/ocsf/v1_5_0"
)

var (
	syslogSeverity = "Informational"
	logonActivity  = "Logon"
)

type SysLog struct {
	Timestamp  time.Time `json:"timestamp"`
	Host       string    `json:"host"`
	SourceType string    `json:"source_type"`
	Stream     string    `json:"stream"`
	Level      string    `json:"level"`
	Message    string    `json:"message"`
}

func SyslogToOCSF(log *SysLog) (*v1_5_0.Authentication, error) {

	return &v1_5_0.Authentication{
		ActivityId:   1,
		ActivityName: &logonActivity,
		Time:         log.Timestamp.UnixMilli(),
		Message:      &log.Message,
		SeverityId:   1,
		Severity:     &syslogSeverity,
	}, nil
}
