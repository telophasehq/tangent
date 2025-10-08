package mappers

import (
	"time"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

var (
	syslogSeverity = "Informational"
	logonActivity  = "Logon"
)

func SyslogToOCSF(log map[string]any) (*v1_5_0.Authentication, error) {
	ts, err := time.Parse(time.RFC3339Nano, log["timestamp"].(string))
	if err != nil {
		return nil, err
	}
	message := log["message"].(string)

	return &v1_5_0.Authentication{
		ActivityId:   1,
		ActivityName: &logonActivity,
		Time:         ts.UnixMilli(),
		Message:      &message,
		SeverityId:   1,
		Severity:     &syslogSeverity,
	}, nil
}
