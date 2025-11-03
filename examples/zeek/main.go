package main

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"
	"zeek/internal/tangent/logs/log"
	"zeek/internal/tangent/logs/mapper"
	"zeek/tangenthelpers"

	"github.com/segmentio/encoding/json"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
	"go.bytecodealliance.org/cm"
)

var (
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

type SPCap struct {
	URL     *string `json:"url,omitempty"`
	Rule    *int64  `json:"rule,omitempty"`
	Trigger *string `json:"trigger,omitempty"`
}

type OCSFUnMapped struct {
	MissedBytes      *int64   `json:"missed_bytes,omitempty"`
	VLAN             *int64   `json:"vlan,omitempty"`
	App              []string `json:"app,omitempty"`
	TunnelParent     []string `json:"tunnel_parents,omitempty"`
	SuriIDs          []string `json:"suri_ids,omitempty"`
	LocalOrig        *bool    `json:"local_orig,omitempty"`
	LocalResp        *bool    `json:"local_resp,omitempty"`
	OrigIPBytes      *int64   `json:"orig_ip_bytes,omitempty"`
	RespIPBytes      *int64   `json:"resp_ip_bytes,omitempty"`
	Pcr              *float64 `json:"pcr,omitempty"`
	CorelightShunted *bool    `json:"corelight_shunted,omitempty"`
	SPCap            *SPCap   `json:"spcap,omitempty"`
}

func Wire() {
	mapper.Exports.Metadata = func() mapper.Meta {
		return mapper.Meta{
			Name:    "zeek-conn → ocsf.network_activity",
			Version: "0.1.3",
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

	mapper.Exports.ProcessLogs = func(input cm.List[cm.Rep]) (res cm.Result[cm.List[uint8], cm.List[uint8], string]) {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()

		var items []cm.Rep
		items = append(items, input.Slice()...)
		for idx := range items {
			lv := log.Logview(items[idx])
			rawTS := tangenthelpers.GetString(lv, "ts")
			rawWTS := tangenthelpers.GetString(lv, "_write_ts")

			ts, err := time.Parse(time.RFC3339Nano, *rawTS)
			if err != nil {
				res.SetErr("bad ts: " + err.Error())
				return
			}
			timeMs := ts.UnixMilli()

			var writeTimeMs int64
			if rawWTS != nil {
				if wts, err := time.Parse(time.RFC3339Nano, *rawWTS); err == nil {
					writeTimeMs = wts.UnixMilli()
				}
			}

			const classUID int32 = 4001 // network_activity
			const categoryUID int32 = 4 // Network Activity
			var activityID int32 = 2
			var severityID int32 = 1
			typeUID := int64(classUID)*100 + int64(activityID)

			uid := tangenthelpers.GetString(lv, "uid")
			path := tangenthelpers.GetString(lv, "_path")
			systemName := tangenthelpers.GetString(lv, "_system_name")

			localOrig := tangenthelpers.GetBool(lv, "local_orig")
			localResp := tangenthelpers.GetBool(lv, "local_resp")

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
			if d := tangenthelpers.GetFloat(lv, "duration"); d != nil {
				ms := int64(math.Round(*d))
				duration = &ms
			}

			var startTime, endTime int64
			if duration != nil {
				startTime = timeMs
				endTime = timeMs + *duration
			}

			origP := tangenthelpers.GetInt64(lv, "id.orig_p")
			origH := tangenthelpers.GetString(lv, "id.orig_h")
			respH := tangenthelpers.GetString(lv, "id.resp_h")
			respP := tangenthelpers.GetInt64(lv, "id.resp_p")

			var src, dst *v1_5_0.NetworkEndpoint
			if origH != nil && origP != nil {
				src = toNetEndpoint(*origH, int(*origP))

				if srcMac := tangenthelpers.GetString(lv, "orig_l2_addr"); srcMac != nil {
					src.Mac = srcMac
				}
			}

			if respH != nil && respP != nil {
				dst = toNetEndpoint(*respH, int(*respP))
				if dstMac := tangenthelpers.GetString(lv, "resp_l2_addr"); dstMac != nil {
					dst.Mac = dstMac
				}
				if cc := tangenthelpers.GetString(lv, "resp_cc"); cc != nil {
					dst.Location = &v1_5_0.GeoLocation{Country: cc}
				}
			}

			proto := tangenthelpers.GetString(lv, "proto")
			var pn int
			var pName string
			if proto != nil {
				pn, pName = protoToOCSF(*proto)
			}
			connInfo := &v1_5_0.NetworkConnectionInformation{}
			if pName != "" {
				p := pName
				connInfo.ProtocolName = &p
			}
			if communityUid := tangenthelpers.GetString(lv, "community_id"); communityUid != nil {
				connInfo.CommunityUid = communityUid
			}
			if pn != 0 {
				pnum := int32(pn)
				connInfo.ProtocolNum = &pnum
			}
			if directionID != nil {
				connInfo.DirectionId = *directionID
			}
			if h := tangenthelpers.GetString(lv, "history"); h != nil {
				connInfo.FlagHistory = h
			}
			if connInfo.ProtocolName == nil && connInfo.ProtocolNum == nil && connInfo.FlagHistory == nil {
				connInfo = nil
			}

			// Traffic counters
			ob := tangenthelpers.GetInt64(lv, "orig_bytes")
			rb := tangenthelpers.GetInt64(lv, "resp_bytes")
			mb := tangenthelpers.GetInt64(lv, "missed_bytes")
			op := tangenthelpers.GetInt64(lv, "orig_pkts")
			rp := tangenthelpers.GetInt64(lv, "resp_pkts")

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
				Uid:     uid,
				Product: v1_5_0.Product{
					Name:       &productName,
					VendorName: &vendorName,
				},
				LogName: path,
			}
			if writeTimeMs != 0 {
				md.LoggedTime = writeTimeMs
			}
			if systemName != nil {
				md.Loggers = []v1_5_0.Logger{{Name: systemName}}
			}

			// Optional strings
			var appName *string
			if s := tangenthelpers.GetString(lv, "service"); s != nil {
				appName = s
			}
			var statusCode *string
			if cs := tangenthelpers.GetString(lv, "conn_state"); cs != nil {
				statusCode = cs
			}

			// Observables (hostname lists)
			objs := buildObservablesFromLogview(lv)

			var unmapped OCSFUnMapped

			if missedBytes := tangenthelpers.GetInt64(lv, "missed_bytes"); missedBytes != nil {
				unmapped.MissedBytes = missedBytes
			}

			if vlan := tangenthelpers.GetInt64(lv, "vlan"); vlan != nil {
				unmapped.VLAN = vlan
			}

			app, _ := tangenthelpers.GetStringList(lv, "app")
			unmapped.App = app

			tunnelParents, _ := tangenthelpers.GetStringList(lv, "tunnel_parents")
			unmapped.TunnelParent = tunnelParents

			suriIDs, _ := tangenthelpers.GetStringList(lv, "suri_ids")
			unmapped.SuriIDs = suriIDs

			var sp SPCap
			sp.Trigger = tangenthelpers.GetString(lv, "spcap.trigger")

			sp.URL = tangenthelpers.GetString(lv, "spcap.url")

			if rule := tangenthelpers.GetInt64(lv, "spcap.rule"); rule != nil {
				sp.Rule = rule
			}

			if localOrig != nil {
				unmapped.LocalOrig = localOrig
			}

			if localResp != nil {
				unmapped.LocalResp = localResp
			}

			if origIPBytes := tangenthelpers.GetInt64(lv, "orig_ip_bytes"); origIPBytes != nil {
				unmapped.OrigIPBytes = origIPBytes
			}

			if respIPBytes := tangenthelpers.GetInt64(lv, "resp_ip_bytes"); respIPBytes != nil {
				unmapped.RespIPBytes = respIPBytes
			}

			if pcr := tangenthelpers.GetFloat(lv, "pcr"); pcr != nil {
				unmapped.Pcr = pcr
			}

			if corelightShunted := tangenthelpers.GetBool(lv, "corelight_shunted"); corelightShunted != nil {
				unmapped.CorelightShunted = corelightShunted
			}
			unmapped.SPCap = &sp

			var unmappedPtr *string
			if b, err := json.Marshal(unmapped); err == nil {
				s := string(b)
				unmappedPtr = &s
			}

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
				Unmapped:       unmappedPtr,
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

	srcProvider := tangenthelpers.GetString(v, "id.orig_h_name.src")
	if vals, ok := tangenthelpers.GetStringList(v, "id.orig_h_name.vals"); ok {
		for _, s := range vals {
			name := "src_endpoint.hostname"
			typ := int32(1)
			val := s
			base := float64(0)
			scoreID := int32(0)
			rep := &v1_5_0.Reputation{
				Provider:  srcProvider,
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

	dstProvider := tangenthelpers.GetString(v, "id.resp_h_name.src")
	if vals, ok := tangenthelpers.GetStringList(v, "id.resp_h_name.vals"); ok {
		for _, s := range vals {
			name := "dst_endpoint.hostname"
			typ := int32(1)
			val := s
			base := float64(0)
			scoreID := int32(0)
			rep := &v1_5_0.Reputation{
				Provider:  dstProvider,
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
