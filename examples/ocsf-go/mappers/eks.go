package mappers

import (
	"regexp"
	"strconv"
	"time"

	v1_5_0 "github.com/Santiago-Labs/go-ocsf/ocsf/v1_5_0"
)

type DockerLog struct {
	Timestamp     time.Time `json:"timestamp"     msgpack:"timestamp"`
	ContainerID   string    `json:"container_id"  msgpack:"container_id"`
	ContainerName string    `json:"container_name" msgpack:"container_name"`
	Image         string    `json:"image"         msgpack:"image"`
	Host          string    `json:"host"          msgpack:"host"`
	SourceType    string    `json:"source_type"   msgpack:"source_type"`
	Stream        string    `json:"stream"        msgpack:"stream"`
	Label         Label     `json:"label"         msgpack:"label"`
	Message       string    `json:"message"       msgpack:"message"`
	Extra         Extra     `json:"extra"         msgpack:"extra"`
}

type Label struct {
	App        string `json:"app"        msgpack:"app"`
	Env        string `json:"env"        msgpack:"env"`
	Maintainer string `json:"maintainer" msgpack:"maintainer"`
}

type Extra struct {
	TraceID string `json:"trace_id" msgpack:"trace_id"`
	SpanID  string `json:"span_id"  msgpack:"span_id"`
	UserID  int64  `json:"user_id"  msgpack:"user_id"`
	Region  string `json:"region"   msgpack:"region"`
}

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

func EKSToOCSF(log *DockerLog) (*v1_5_0.APIActivity, error) {
	epochMs := log.Timestamp.UnixMilli()
	method, path, status, latency, okHTTP := parseAccessLine(log.Message)

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
			Region:   &log.Extra.Region,
		},
		DstEndpoint: &v1_5_0.NetworkEndpoint{
			Container: &v1_5_0.Container{
				Image: &v1_5_0.Image{
					Name: &log.Image,
				},
				Uid:  &log.ContainerID,
				Name: &log.ContainerName,
			},
		},
	}

	if log.Message != "" {
		msg := log.Message
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
