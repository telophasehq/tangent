package main

import (
	"errors"
	"math"
	"time"

	tangent_sdk "github.com/ethanblackburn/tangent-sdk-go"
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

var metadata = tangent_sdk.Metadata{
	Name:    "zeek-conn → ocsf.network_activity",
	Version: "0.2.0",
}

var selectors = []tangent_sdk.Selector{
	{
		All: []tangent_sdk.Predicate{
			tangent_sdk.Has("uid"),
			tangent_sdk.Has("ts"),
			tangent_sdk.EqString("_path", "conn"),
		},
	},
}

func ZeekMapper(lv tangent_sdk.Log) (*NetworkActivity, error) {
	timeMs, ok := zeekTime(lv, "ts")
	if !ok {
		return nil, errors.New("zeek conn: ts is neither ISO 8601 text nor epoch seconds")
	}
	writeTimeMs, _ := zeekTime(lv, "_write_ts")

	const classUID int32 = 4001 // network_activity
	const categoryUID int32 = 4 // Network Activity
	var activityID int32 = 2
	var severityID int32 = 1
	typeUID := int64(classUID)*100 + int64(activityID)

	uid := lv.GetString("uid")
	path := lv.GetString("_path")
	systemName := lv.GetString("_system_name")

	localOrig := lv.GetBool("local_orig")
	localResp := lv.GetBool("local_resp")

	var directionID *int32
	switch {
	case localOrig != nil && *localOrig && localResp != nil && !*localResp:
		out := int32(2) // outbound
		directionID = &out
	case localOrig != nil && !*localOrig && localResp != nil && *localResp:
		in := int32(1) // inbound
		directionID = &in
	case localOrig != nil && *localOrig && localResp != nil && *localResp:
		lateral := int32(3) // lateral: both ends inside the network
		directionID = &lateral
	}

	// Zeek's duration is seconds; OCSF's is milliseconds.
	var duration *int64
	if d := number(lv, "duration"); d != nil {
		ms := int64(math.Round(*d * 1000))
		duration = &ms
	}

	var startTime, endTime int64
	if duration != nil {
		startTime = timeMs
		endTime = timeMs + *duration
	}

	origP := lv.GetInt64("id.orig_p")
	origH := lv.GetString("id.orig_h")
	respH := lv.GetString("id.resp_h")
	respP := lv.GetInt64("id.resp_p")

	var src, dst *NetworkEndpoint
	if origH != nil && origP != nil {
		src = toNetEndpoint(*origH, int(*origP))

		if srcMac := lv.GetString("orig_l2_addr"); srcMac != nil {
			src.Mac = srcMac
		}
	}

	if respH != nil && respP != nil {
		dst = toNetEndpoint(*respH, int(*respP))
		if dstMac := lv.GetString("resp_l2_addr"); dstMac != nil {
			dst.Mac = dstMac
		}
		if cc := lv.GetString("resp_cc"); cc != nil {
			dst.Location = &GeoLocation{Country: cc}
		}
	}

	proto := lv.GetString("proto")
	var pn int
	var pName string
	if proto != nil {
		pn, pName = protoToOCSF(*proto)
	}
	connInfo := &NetworkConnectionInformation{}
	if pName != "" {
		p := pName
		connInfo.ProtocolName = &p
	}
	if communityUid := lv.GetString("community_id"); communityUid != nil {
		connInfo.CommunityUid = communityUid
	}
	if pn != 0 {
		pnum := int32(pn)
		connInfo.ProtocolNum = &pnum
	}
	if directionID != nil {
		connInfo.DirectionId = *directionID
	}
	if h := lv.GetString("history"); h != nil {
		connInfo.FlagHistory = h
	}
	if connInfo.ProtocolName == nil && connInfo.ProtocolNum == nil && connInfo.FlagHistory == nil {
		connInfo = nil
	}

	// Traffic counters
	ob := lv.GetInt64("orig_bytes")
	rb := lv.GetInt64("resp_bytes")
	mb := lv.GetInt64("missed_bytes")
	op := lv.GetInt64("orig_pkts")
	rp := lv.GetInt64("resp_pkts")

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

	var traffic *NetworkTraffic
	if ob != nil || rb != nil || mb != nil || op != nil || rp != nil {
		traffic = &NetworkTraffic{
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
	md := Metadata{
		Version: ver,
		Uid:     uid,
		Product: Product{
			Name:       &productName,
			VendorName: &vendorName,
		},
		LogName: path,
	}
	if writeTimeMs != 0 {
		md.LoggedTime = writeTimeMs
	}
	if systemName != nil {
		md.Loggers = []Logger{{Name: systemName}}
	}

	// Optional strings
	var appName *string
	if s := lv.GetString("service"); s != nil {
		appName = s
	}
	var statusCode *string
	if cs := lv.GetString("conn_state"); cs != nil {
		statusCode = cs
	}

	// Observables (hostname lists)
	objs := buildObservablesFromLogview(lv)

	var unmapped OCSFUnMapped

	if missedBytes := lv.GetInt64("missed_bytes"); missedBytes != nil {
		unmapped.MissedBytes = missedBytes
	}

	if vlan := lv.GetInt64("vlan"); vlan != nil {
		unmapped.VLAN = vlan
	}

	app, _ := lv.GetStringList("app")
	unmapped.App = app

	tunnelParents, _ := lv.GetStringList("tunnel_parents")
	unmapped.TunnelParent = tunnelParents

	suriIDs, _ := lv.GetStringList("suri_ids")
	unmapped.SuriIDs = suriIDs

	var sp SPCap
	sp.Trigger = lv.GetString("spcap.trigger")

	sp.URL = lv.GetString("spcap.url")

	if rule := lv.GetInt64("spcap.rule"); rule != nil {
		sp.Rule = rule
	}

	if localOrig != nil {
		unmapped.LocalOrig = localOrig
	}

	if localResp != nil {
		unmapped.LocalResp = localResp
	}

	if origIPBytes := lv.GetInt64("orig_ip_bytes"); origIPBytes != nil {
		unmapped.OrigIPBytes = origIPBytes
	}

	if respIPBytes := lv.GetInt64("resp_ip_bytes"); respIPBytes != nil {
		unmapped.RespIPBytes = respIPBytes
	}

	if pcr := lv.GetFloat64("pcr"); pcr != nil {
		unmapped.Pcr = pcr
	}

	if corelightShunted := lv.GetBool("corelight_shunted"); corelightShunted != nil {
		unmapped.CorelightShunted = corelightShunted
	}
	if sp.URL != nil || sp.Rule != nil || sp.Trigger != nil {
		unmapped.SPCap = &sp
	}

	na := NetworkActivity{
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
		Unmapped:       &unmapped,
	}
	if duration != nil {
		na.StartTime = startTime
		na.EndTime = endTime
	}

	return &na, nil
}

/* ---------------- helpers: domain-specific ---------------- */

// zeekTime reads a Zeek timestamp as epoch milliseconds: ISO 8601 text, as
// Corelight writes it (and Zeek with LogAscii::json_timestamps =
// JSON::TS_ISO8601), or epoch seconds, Zeek's own JSON default.
func zeekTime(lv tangent_sdk.Log, path string) (int64, bool) {
	if s := lv.GetString(path); s != nil {
		t, err := time.Parse(time.RFC3339Nano, *s)
		if err != nil {
			return 0, false
		}
		return t.UnixMilli(), true
	}
	if secs := number(lv, path); secs != nil {
		return int64(math.Round(*secs * 1000)), true
	}
	return 0, false
}

// number reads a JSON number at path, written with or without a fraction.
func number(lv tangent_sdk.Log, path string) *float64 {
	if f := lv.GetFloat64(path); f != nil {
		return f
	}
	if i := lv.GetInt64(path); i != nil {
		f := float64(*i)
		return &f
	}
	return nil
}

func toNetEndpoint(ip string, port int) *NetworkEndpoint {
	ep := &NetworkEndpoint{}
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

func buildObservablesFromLogview(v tangent_sdk.Log) []Observable {
	var out []Observable

	srcProvider := v.GetString("id.orig_h_name.src")
	if vals, ok := v.GetStringList("id.orig_h_name.vals"); ok {
		for _, s := range vals {
			name := "src_endpoint.hostname"
			typ := int32(1)
			val := s
			base := float64(0)
			scoreID := int32(0)
			rep := &Reputation{
				Provider:  srcProvider,
				BaseScore: base,
				ScoreId:   scoreID,
			}
			out = append(out, Observable{
				Name:       &name,
				TypeId:     typ,
				Value:      &val,
				Reputation: rep,
			})
		}
	}

	dstProvider := v.GetString("id.resp_h_name.src")
	if vals, ok := v.GetStringList("id.resp_h_name.vals"); ok {
		for _, s := range vals {
			name := "dst_endpoint.hostname"
			typ := int32(1)
			val := s
			base := float64(0)
			scoreID := int32(0)
			rep := &Reputation{
				Provider:  dstProvider,
				BaseScore: base,
				ScoreId:   scoreID,
			}
			out = append(out, Observable{
				Name:       &name,
				TypeId:     typ,
				Value:      &val,
				Reputation: rep,
			})
		}
	}
	return out
}

func init() {
	tangent_sdk.Wire[*NetworkActivity](
		metadata,
		selectors,
		ZeekMapper,
		nil,
	)
}
func main() {}
