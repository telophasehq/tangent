package mappers

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	v1_5_0 "github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

var (
	reAccess     = regexp.MustCompile(`^\s*([A-Z]+)\s+(\S+)\s+(\d{3})\s+(\d+)\s*ms\s+"([^"]*)"\s*$`)
	className    = "API Activity"
	categoryName = "Application Activity"
	eksSeverity  = "Informational"
	logName      = "backend"
	provider     = "aws"
)

func parseAccessLine(s string) (method, path string, status int32, latencyMs int32, ok bool) {
	m := reAccess.FindStringSubmatch(s)
	if len(m) != 6 {
		return "", "", 0, 0, false
	}
	method = m[1]
	path = m[2]
	stat, err := strconv.Atoi(m[3])
	if err != nil {
		return "", "", 0, 0, false
	}
	status = int32(stat)
	latency, _ := strconv.ParseInt(m[4], 10, 64)
	latencyMs = int32(latency)
	return method, path, status, latencyMs, true
}

func EKSToOCSF(log []byte) (*v1_5_0.APIActivity, error) {
	var rec EksLog
	err := json.Unmarshal(log, &rec)
	if err != nil {
		return nil, err
	}

	var epochMs int64
	if rec.Timestamp != "" {
		if ts, err := time.Parse(time.RFC3339Nano, rec.Timestamp); err == nil {
			epochMs = ts.UnixMilli()
		}
	}

	method, path, status, latency, okHTTP := parseAccessLine(rec.Message)

	region := getString(rec.Extra, "region")

	activityId, activityName := httpReqToActivity(method)
	ev := &v1_5_0.APIActivity{
		ClassUid:     6003,
		ClassName:    &className,
		ActivityId:   int32(activityId),
		ActivityName: &activityName,
		CategoryUid:  6,
		CategoryName: &categoryName,
		Severity:     &eksSeverity,
		SeverityId:   1,
		Time:         epochMs,
		Metadata: v1_5_0.Metadata{
			LogName: &logName,
		},
		Cloud: v1_5_0.Cloud{
			Provider: provider,
			Region:   &region,
		},
		DstEndpoint: &v1_5_0.NetworkEndpoint{
			Container: &v1_5_0.Container{
				Image: &v1_5_0.Image{
					Name: &rec.Image,
				},
				Uid:  &rec.ContainerID,
				Name: &rec.Container,
			},
		},
	}

	if rec.Message != "" {
		msg := rec.Message
		ev.Message = &msg
	}

	if okHTTP {
		ev.HttpRequest = &v1_5_0.HTTPRequest{
			HttpMethod: &method,
			Url: &v1_5_0.UniformResourceLocator{
				Path: &path,
			},
		}

		ev.HttpResponse = &v1_5_0.HTTPResponse{
			Code:    status,
			Latency: &latency,
		}
	}

	return ev, nil
}

func httpReqToActivity(method string) (int, string) {
	switch method {
	case "POST":
		return 1, "Create"
	case "GET":
		return 2, "Read"
	case "PATCH":
		return 3, "Update"
	case "DELETE":
		return 4, "Delete"
	default:
		return 0, "Unknown"
	}
}

func toEksLog(m map[string]any) (*EksLog, error) {
	if m == nil {
		return nil, errors.New("nil log")
	}
	rec := &EksLog{
		Timestamp:   getString(m, "timestamp"),
		ContainerID: getString(m, "container_id"),
		Container:   getString(m, "container_name"),
		Image:       getString(m, "image"),
		Host:        getString(m, "host"),
		SourceType:  getString(m, "source_type"),
		Stream:      getString(m, "stream"),
		Message:     getString(m, "message"),
	}
	if v, ok := m["label"].(map[string]any); ok && v != nil {
		rec.Label = make(map[string]string, len(v))
		for k, vv := range v {
			rec.Label[k] = toString(vv)
		}
	} else if v2, ok := m["label"].(map[string]string); ok && v2 != nil {
		rec.Label = v2
	}
	if v, ok := m["extra"].(map[string]any); ok && v != nil {
		rec.Extra = v
	}
	return rec, nil
}

func getString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[key]; ok && v != nil {
		return toString(v)
	}
	return ""
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case uint64:
		return strconv.FormatUint(t, 10)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(v)
	}
}

func strPtr(s string) *string { return &s }
