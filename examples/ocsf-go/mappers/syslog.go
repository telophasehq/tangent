package mappers

import (
	"ocsf-go/tangenthelpers"
	"time"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

var (
	syslogSeverity = "Informational"
	logonActivity  = "Logon"
)

func SyslogToOCSF(log []byte) (*v1_5_0.Authentication, error) {

	var ms int64
	timestamp, isString := tangenthelpers.GetString(log, "timestamp")
	if isString {

	}
	ts, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return nil, err
	}
	ms = ts.UnixMilli()
	message, _ := tangenthelpers.GetString(log, "message")

	return &v1_5_0.Authentication{
		ActivityId:   1,
		ActivityName: &logonActivity,
		Time:         ms,
		Message:      &message,
		SeverityId:   1,
		Severity:     &syslogSeverity,
	}, nil
}
