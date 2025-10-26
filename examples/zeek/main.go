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
	bufPool = sync.Pool{
		New: func() any { return bytes.NewBuffer(make([]byte, 0, 16<<10)) },
	}
	encPool = sync.Pool{
		New: func() any {
			buf := bytes.NewBuffer(make([]byte, 0, 16<<10))
			e := json.NewEncoder(buf)
			e.SetEscapeHTML(false)
			return e
		},
	}
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
		enc := encPool.Get().(*json.Encoder)
		enc.SetEscapeHTML(false)

		defer func() {
			encPool.Put(enc)
			bufPool.Put(buf)
		}()

		var items []log.Logview
		items = append(items, input.Slice()...)

		paths := []string{
			"ts", "_write_ts", "uid", "_path", "_system_name",
			"local_orig", "local_resp", "duration",
			"id.orig_h", "id.orig_p", "id.resp_h", "id.resp_p",
			"proto", "history",
			"orig_l2_addr", "resp_l2_addr", "resp_cc",
			"orig_bytes", "resp_bytes", "missed_bytes", "orig_pkts", "resp_pkts",
			"service", "conn_state",
		}

		for idx := range items {
			lv := log.Logview(items[idx])
			vals := lv.GetMany(cm.ToList(paths))

			rawTS, _ := getStringI(vals, 0)
			rawWTS, _ := getStringI(vals, 1)

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

			uid, _ := getStringI(vals, 2)
			path, _ := getStringI(vals, 3)
			systemName, _ := getStringI(vals, 4)

			localOrig := getBoolPtrI(vals, 5)
			localResp := getBoolPtrI(vals, 6)

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
			if d := getFloatPtrI(vals, 7); d != nil {
				ms := int64(math.Round(*d * 1000.0))
				duration = &ms
			}

			var startTime, endTime int64
			if duration != nil {
				startTime = timeMs
				endTime = timeMs + *duration
			}

			origH, _ := getStringI(vals, 8)
			origP, _ := getIntI(vals, 9)
			respH, _ := getStringI(vals, 10)
			respP, _ := getIntI(vals, 11)

			src := toNetEndpoint(origH, origP)
			dst := toNetEndpoint(respH, respP)

			if mac := getStringPtrI(vals, 12); mac != nil && *mac != "" {
				src.Mac = mac
			}
			if mac := getStringPtrI(vals, 13); mac != nil && *mac != "" {
				dst.Mac = mac
			}
			if cc := getStringPtrI(vals, 14); cc != nil && *cc != "" {
				dst.Location = &v1_5_0.GeoLocation{Country: cc}
			}

			proto, _ := getStringI(vals, 15)
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
			if h, ok := getStringI(vals, 16); ok && h != "" {
				connInfo.FlagHistory = &h
			}
			if connInfo.ProtocolName == nil && connInfo.ProtocolNum == nil && connInfo.FlagHistory == nil {
				connInfo = nil
			}

			// Traffic counters
			ob := getInt64PtrI(vals, 17)
			rb := getInt64PtrI(vals, 18)
			mb := getInt64PtrI(vals, 19)
			op := getInt64PtrI(vals, 20)
			rp := getInt64PtrI(vals, 21)

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
			if s, ok := getStringI(vals, 22); ok && s != "" {
				appName = &s
			}
			var statusCode *string
			if cs, ok := getStringI(vals, 23); ok && cs != "" {
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

			if err := enc.Encode(&na); err != nil {
				res.SetErr(err.Error())
				return
			}
		}

		res.SetOK(cm.ToList(buf.Bytes()))
		return
	}
}

// ---- GetMany-backed helpers (I-suffixed) ----

func getStringI(vals cm.List[cm.Option[mapper.Scalar]], idx int) (string, bool) {
	opt := vals.Slice()[idx]
	if opt.None() {
		return "", false
	}
	s := opt.Value() // make addressable
	if p := s.Str(); p != nil {
		return *p, true
	}
	return "", false
}

func getStringPtrI(vals cm.List[cm.Option[mapper.Scalar]], idx int) *string {
	if s, ok := getStringI(vals, idx); ok {
		return &s // stable pointer
	}
	return nil
}

func getBoolPtrI(vals cm.List[cm.Option[mapper.Scalar]], idx int) *bool {
	opt := vals.Slice()[idx]
	if opt.None() {
		return nil
	}
	s := opt.Value() // make addressable
	if p := s.Boolean(); p != nil {
		b := *p
		return &b
	}
	return nil
}

func getIntI(vals cm.List[cm.Option[mapper.Scalar]], idx int) (int, bool) {
	opt := vals.Slice()[idx]
	if opt.None() {
		return 0, false
	}
	s := opt.Value() // make addressable
	if p := s.Int(); p != nil {
		return int(*p), true
	}
	return 0, false
}

func getInt64PtrI(vals cm.List[cm.Option[mapper.Scalar]], idx int) *int64 {
	opt := vals.Slice()[idx]
	if opt.None() {
		return nil
	}
	s := opt.Value() // make addressable
	if p := s.Int(); p != nil {
		v := *p
		return &v
	}
	return nil
}

func getFloatPtrI(vals cm.List[cm.Option[mapper.Scalar]], idx int) *float64 {
	opt := vals.Slice()[idx]
	if opt.None() {
		return nil
	}
	s := opt.Value() // make addressable
	if p := s.Float(); p != nil {
		v := *p
		return &v
	}
	return nil
}

// ---- Per-path helpers (same fix for pointer-receiver methods) ----

func getString(v log.Logview, path string) (string, bool) {
	opt := v.Get(path)
	if opt.None() {
		return "", false
	}
	s := opt.Value() // make addressable
	if p := s.Str(); p != nil {
		return *p, true
	}
	return "", false
}

func getStringPtr(v log.Logview, path string) *string {
	if s, ok := getString(v, path); ok {
		return &s
	}
	return nil
}

func getBoolPtr(v log.Logview, path string) *bool {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	if p := s.Boolean(); p != nil {
		b := *p
		return &b
	}
	return nil
}

func getInt(v log.Logview, path string) (int, bool) {
	opt := v.Get(path)
	if opt.None() {
		return 0, false
	}
	s := opt.Value()
	if p := s.Int(); p != nil {
		return int(*p), true
	}
	return 0, false
}

func getInt64Ptr(v log.Logview, path string) *int64 {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	if p := s.Int(); p != nil {
		v := *p
		return &v
	}
	return nil
}

func getFloatPtr(v log.Logview, path string) *float64 {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	if p := s.Float(); p != nil {
		v := *p
		return &v
	}
	return nil
}

func get(v log.Logview, path string) cm.Option[mapper.Scalar] {
	return v.Get(path)
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
