package mappers

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

func toEndpoint(ip string, port int) v1_5_0.Endpoint {
	if ip == "" {
		return v1_5_0.Endpoint{}
	}
	ipStr := ip
	return v1_5_0.Endpoint{Ip: &ipStr}
}

func toNetEndpoint(ip string, port int) *v1_5_0.NetworkEndpoint {
	if ip == "" && port == 0 {
		return nil
	}
	ipStr := ip
	var portPtr *int32
	if port != 0 {
		p := int32(port)
		portPtr = &p
	}
	return &v1_5_0.NetworkEndpoint{Ip: &ipStr, Port: portPtr}
}

func protoToOCSF(p string) (id int, name string) {
	switch strings.ToLower(p) {
	case "tcp":
		return 6, "tcp"
	case "udp":
		return 17, "udp"
	case "icmp":
		return 1, "icmp"
	default:
		return 0, p
	}
}

func dnsRcodeName(id int) string {
	switch id {
	case 0:
		return "NOERROR"
	case 1:
		return "FORMERR"
	case 2:
		return "SERVFAIL"
	case 3:
		return "NXDOMAIN"
	case 4:
		return "NOTIMP"
	case 5:
		return "REFUSED"
	default:
		return "unknown"
	}
}

func dnsQTypeName(id int) string {
	switch id {
	case 1:
		return "A"
	case 2:
		return "NS"
	case 5:
		return "CNAME"
	case 6:
		return "SOA"
	case 12:
		return "PTR"
	case 15:
		return "MX"
	case 16:
		return "TXT"
	case 28:
		return "AAAA"
	case 33:
		return "SRV"
	default:
		return "unknown"
	}
}

func omitEmptyFlags(m map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k, v := range m {
		if v {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func getAny(m map[string]any, k string) (any, bool) {
	v, ok := m[k]
	return v, ok
}

func getString(m map[string]any, k string) string {
	if v, ok := m[k]; ok {
		return toString(v)
	}
	return ""
}

func getInt64(m map[string]any, k string) int64 {
	if v, ok := m[k]; ok {
		return toInt64(v)
	}
	return 0
}

func getFloat(m map[string]any, k string) float64 {
	if v, ok := m[k]; ok {
		return toFloat(v)
	}
	return 0
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func toInt(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case json.Number:
		i, err := t.Int64()
		if err == nil {
			return int(i), true
		}
	}
	return 0, false
}

func toInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case json.Number:
		i, _ := t.Int64()
		return i
	case string:
		if i, err := strconv.ParseInt(t, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case json.Number:
		f, _ := t.Float64()
		return f
	case int64:
		return float64(t)
	case int:
		return float64(t)
	case string:
		if f, err := strconv.ParseFloat(t, 64); err == nil {
			return f
		}
	}
	return 0
}

func toBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return strings.EqualFold(t, "true") || t == "1"
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	}
	return false
}

func toStringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			out = append(out, toString(e))
		}
		return out
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	default:
		return nil
	}
}

func toInt64Slice(v any) []int64 {
	switch t := v.(type) {
	case []int64:
		return t
	case []any:
		out := make([]int64, 0, len(t))
		for _, e := range t {
			out = append(out, toInt64(e))
		}
		return out
	case string:
		if t == "" {
			return nil
		}
		if i, err := strconv.ParseInt(t, 10, 64); err == nil {
			return []int64{i}
		}
	}
	return nil
}
