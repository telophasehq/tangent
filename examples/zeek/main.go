package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"
	"zeek/internal/tangent/logs/log"
	"zeek/internal/tangent/logs/mapper"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
	"go.bytecodealliance.org/cm"
)

var (
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

func Wire() {
	mapper.Exports.Metadata = func() mapper.Meta {
		return mapper.Meta{
			Name:    "zeek-conn → ocsf.network_activity",
			Version: "0.1.0",
		}
	}

	mapper.Exports.Probe = func() cm.List[mapper.Selector] {
		return cm.ToList([]mapper.Selector{
			{
				Any: cm.ToList([]mapper.Pred{}),
				All: cm.ToList([]mapper.Pred{
					mapper.PredHas("uid"),
					mapper.PredEq(
						cm.Tuple[string, mapper.Scalar]{
							F0: "_path",
							F1: log.ScalarStr("conn"),
						},
					)}),
				None: cm.ToList([]mapper.Pred{}),
			},
		})
	}

	mapper.Exports.ProcessLogs = func(input cm.List[log.Logview]) (res cm.Result[cm.List[uint8], cm.List[uint8], string]) {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()

		var items []log.Logview
		items = append(items, input.Slice()...)
		for idx := range items {
			lv := log.Logview(items[idx])
			rawTS, _ := getString(lv, "ts")
			rawWTS, _ := getString(lv, "_write_ts")

			ts, err := time.Parse(time.RFC3339Nano, rawTS)
			if err != nil {
				res.SetErr("bad ts: " + err.Error())
				return
			}
			timeMs := ts.UnixMilli()

			var writeTimeMs int64
			if rawWTS != "" {
				if wts, err := time.Parse(time.RFC3339Nano, rawWTS); err == nil {
					writeTimeMs = wts.UnixMilli()
				}
			}

			const classUID int32 = 4001 // network_activity
			const categoryUID int32 = 4 // Network Activity
			var activityID int32 = 2
			var severityID int32 = 1
			typeUID := int64(classUID)*100 + int64(activityID)

			uid, _ := getString(lv, "uid")
			path, _ := getString(lv, "_path")
			systemName, _ := getString(lv, "_system_name")

			localOrig := getBoolPtr(lv, "local_orig")
			localResp := getBoolPtr(lv, "local_resp")

			var directionID *int32
			switch {
			case localOrig != nil && *localOrig && localResp != nil && !*localResp:
				out := int32(2) // outbound
				directionID = &out
			case localOrig != nil && !*localOrig && localResp != nil && *localResp:
				in := int32(1) // inbound
				directionID = &in
			}

			var duration *int64
			if d := getFloatPtr(lv, "duration"); d != nil {
				ms := int64(math.Round(*d * 1000.0))
				duration = &ms
			}

			var startTime, endTime int64
			if duration != nil {
				startTime = timeMs
				endTime = timeMs + *duration
			}

			origH, _ := getString(lv, "id.orig_h")
			origP, _ := getInt(lv, "id.orig_p")
			respH, _ := getString(lv, "id.resp_h")
			respP, _ := getInt(lv, "id.resp_p")

			src := toNetEndpoint(origH, origP)
			dst := toNetEndpoint(respH, respP)

			if mac := getStringPtr(lv, "orig_l2_addr"); mac != nil && *mac != "" {
				src.Mac = mac
			}
			if mac := getStringPtr(lv, "resp_l2_addr"); mac != nil && *mac != "" {
				dst.Mac = mac
			}
			if cc := getStringPtr(lv, "resp_cc"); cc != nil && *cc != "" {
				dst.Location = &v1_5_0.GeoLocation{Country: cc}
			}

			proto, _ := getString(lv, "proto")
			pn, pName := protoToOCSF(proto)
			connInfo := &v1_5_0.NetworkConnectionInformation{}
			if pName != "" {
				p := pName
				connInfo.ProtocolName = &p
			}
			if pn != 0 {
				pnum := int32(pn)
				connInfo.ProtocolNum = &pnum
			}
			if directionID != nil {
				connInfo.DirectionId = *directionID
			}
			if h, ok := getString(lv, "history"); ok && h != "" {
				connInfo.FlagHistory = &h
			}
			if connInfo.ProtocolName == nil && connInfo.ProtocolNum == nil && connInfo.FlagHistory == nil {
				connInfo = nil
			}

			// Traffic counters
			ob := getInt64Ptr(lv, "orig_bytes")
			rb := getInt64Ptr(lv, "resp_bytes")
			mb := getInt64Ptr(lv, "missed_bytes")
			op := getInt64Ptr(lv, "orig_pkts")
			rp := getInt64Ptr(lv, "resp_pkts")

			var totalBytes, totalPkts *int64
			if ob != nil || rb != nil || op != nil || rp != nil {
				tb, tp := int64(0), int64(0)
				if ob != nil {
					tb += *ob
				}
				if rb != nil {
					tb += *rb
				}
				if op != nil {
					tp += *op
				}
				if rp != nil {
					tp += *rp
				}
				totalBytes, totalPkts = &tb, &tp
			}

			var traffic *v1_5_0.NetworkTraffic
			if ob != nil || rb != nil || mb != nil || op != nil || rp != nil {
				traffic = &v1_5_0.NetworkTraffic{
					BytesOut:    ob,
					PacketsOut:  op,
					BytesIn:     rb,
					PacketsIn:   rp,
					BytesMissed: mb,
					Bytes:       totalBytes,
					Packets:     totalPkts,
				}
			}

			// Metadata
			ver := "1.5.0"
			productName := "Zeek"
			vendorName := "Zeek"
			md := v1_5_0.Metadata{
				Version: ver,
				Uid:     &uid,
				Product: v1_5_0.Product{
					Name:       &productName,
					VendorName: &vendorName,
				},
				LogName: &path,
			}
			if writeTimeMs != 0 {
				md.LoggedTime = writeTimeMs
			}
			if systemName != "" {
				md.Loggers = []v1_5_0.Logger{{Name: &systemName}}
			}

			// Optional strings
			var appName *string
			if s, ok := getString(lv, "service"); ok && s != "" {
				appName = &s
			}
			var statusCode *string
			if cs, ok := getString(lv, "conn_state"); ok && cs != "" {
				statusCode = &cs
			}

			// Observables (hostname lists)
			objs := buildObservablesFromLogview(lv)

			na := v1_5_0.NetworkActivity{
				ActivityId:     activityID,
				CategoryUid:    categoryUID,
				ClassUid:       classUID,
				SeverityId:     severityID,
				TypeUid:        typeUID,
				Time:           timeMs,
				Metadata:       md,
				AppName:        appName,
				SrcEndpoint:    src,
				DstEndpoint:    dst,
				ConnectionInfo: connInfo,
				Traffic:        traffic,
				Duration:       duration,
				StatusCode:     statusCode,
				Observables:    objs,
			}
			if duration != nil {
				na.StartTime = startTime
				na.EndTime = endTime
			}

			line, err := json.Marshal(na)
			if err != nil {
				res.SetErr(err.Error())
				return
			}

			buf.Write(line)
			buf.WriteByte('\n')
		}

		res.SetOK(cm.ToList(buf.Bytes()))
		return
	}
}

func get(v log.Logview, path string) cm.Option[mapper.Scalar] {
	return v.Get(path)
}

func getString(v log.Logview, path string) (string, bool) {
	opt := v.Get(path)
	if opt.None() {
		return "", false
	}
	s := opt.Value()
	if p := s.Str(); p != nil {
		return *p, true
	}
	return "", false
}

func getStringList(v log.Logview, path string) ([]string, bool) {
	opt := v.GetList(path)
	if opt.None() {
		return nil, false
	}
	lst := opt.Value()
	out := make([]string, 0, lst.Len())
	data := lst.Slice()
	for i := 0; i < int(lst.Len()); i++ {
		if p := data[i].Str(); p != nil {
			out = append(out, *p)
		}
	}
	return out, true
}

func getStringPtr(v log.Logview, path string) *string {
	if s, ok := getString(v, path); ok {
		return &s
	}
	return nil
}

func getBoolPtr(v log.Logview, path string) *bool {
	opt := get(v, path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Boolean()
}

func getInt(v log.Logview, path string) (int, bool) {
	opt := get(v, path)
	if opt.None() {
		return 0, false
	}
	s := opt.Value()
	if s.Int() != nil {
		return int(*s.Int()), true
	}
	return 0, false
}
func getInt64Ptr(v log.Logview, path string) *int64 {
	opt := get(v, path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Int()
}
func getFloatPtr(v log.Logview, path string) *float64 {
	opt := get(v, path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Float()
}

/* ---------------- helpers: domain-specific ---------------- */

func toNetEndpoint(ip string, port int) *v1_5_0.NetworkEndpoint {
	ep := &v1_5_0.NetworkEndpoint{}
	if ip != "" {
		ep.Ip = &ip
	}
	if port != 0 {
		p := int32(port)
		ep.Port = &p
	}
	return ep
}

// Simplified: return (num, name) for OCSF proto fields.
func protoToOCSF(p string) (int, string) {
	switch p {
	case "tcp":
		return 6, "tcp"
	case "udp":
		return 17, "udp"
	default:
		return 0, p
	}
}

func buildObservablesFromLogview(v log.Logview) []v1_5_0.Observable {
	var out []v1_5_0.Observable

	srcProvider, _ := getString(v, "id.orig_h_name.src")
	if vals, ok := getStringList(v, "id.orig_h_name.vals"); ok {
		for _, s := range vals {
			name := "src_endpoint.hostname"
			typ := int32(1)
			val := s
			base := float64(0)
			scoreID := int32(0)
			rep := &v1_5_0.Reputation{
				Provider:  &srcProvider,
				BaseScore: base,
				ScoreId:   scoreID,
			}
			out = append(out, v1_5_0.Observable{
				Name:       &name,
				TypeId:     typ,
				Value:      &val,
				Reputation: rep,
			})
		}
	}

	dstProvider, _ := getString(v, "id.resp_h_name.src")
	if vals, ok := getStringList(v, "id.resp_h_name.vals"); ok {
		for _, s := range vals {
			name := "dst_endpoint.hostname"
			typ := int32(1)
			val := s
			base := float64(0)
			scoreID := int32(0)
			rep := &v1_5_0.Reputation{
				Provider:  &dstProvider,
				BaseScore: base,
				ScoreId:   scoreID,
			}
			out = append(out, v1_5_0.Observable{
				Name:       &name,
				TypeId:     typ,
				Value:      &val,
				Reputation: rep,
			})
		}
	}
	return out
}

func parseScalarTime(s mapper.Scalar) (time.Time, error) {
	if f := s.Float(); f != nil {
		secs := int64(*f)
		nsec := int64((*f - float64(secs)) * 1e9)
		return time.Unix(secs, nsec).UTC(), nil
	}
	if i := s.Int(); i != nil {
		return time.Unix(*i, 0).UTC(), nil
	}
	if p := s.Str(); p != nil {
		if fv, err := strconv.ParseFloat(*p, 64); err == nil {
			secs := int64(fv)
			nsec := int64((fv - float64(secs)) * 1e9)
			return time.Unix(secs, nsec).UTC(), nil
		}
		if t, err := time.Parse(time.RFC3339Nano, *p); err == nil {
			return t.UTC(), nil
		}
		if t, err := time.Parse(time.RFC3339, *p); err == nil {
			return t.UTC(), nil
		}
		return time.Time{}, fmt.Errorf("unsupported time string: %q", *p)
	}
	return time.Time{}, fmt.Errorf("unsupported scalar variant for time")
}

func init() {
	Wire()
}

func main() {}
