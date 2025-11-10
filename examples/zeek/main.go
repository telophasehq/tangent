package main

import (
	"bytes"
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
	"github.com/telophasehq/tangent-sdk-go/helpers"

	tangent_sdk "github.com/telophasehq/tangent-sdk-go"
)

var (
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

type NetworkActivityAlias v1_5_0.NetworkActivity

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
	Version: "0.1.3",
}

var selectors = []tangent_sdk.Selector{
	{
		All: []tangent_sdk.Predicate{
			tangent_sdk.Has("uid"),
			tangent_sdk.EqString("_path", "conn"),
		},
	},
}

func ZeekMapper(lv tangent_sdk.Log) (*NetworkActivityAlias, error) {
	rawTS := helpers.GetString(lv, "ts")
	rawWTS := helpers.GetString(lv, "_write_ts")

	ts, err := time.Parse(time.RFC3339Nano, *rawTS)
	if err != nil {
		return nil, err
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

	uid := helpers.GetString(lv, "uid")
	path := helpers.GetString(lv, "_path")
	systemName := helpers.GetString(lv, "_system_name")

	localOrig := helpers.GetBool(lv, "local_orig")
	localResp := helpers.GetBool(lv, "local_resp")

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
	if d := helpers.GetFloat64(lv, "duration"); d != nil {
		ms := int64(math.Round(*d))
		duration = &ms
	}

	var startTime, endTime int64
	if duration != nil {
		startTime = timeMs
		endTime = timeMs + *duration
	}

	origP := helpers.GetInt64(lv, "id.orig_p")
	origH := helpers.GetString(lv, "id.orig_h")
	respH := helpers.GetString(lv, "id.resp_h")
	respP := helpers.GetInt64(lv, "id.resp_p")

	var src, dst *v1_5_0.NetworkEndpoint
	if origH != nil && origP != nil {
		src = toNetEndpoint(*origH, int(*origP))

		if srcMac := helpers.GetString(lv, "orig_l2_addr"); srcMac != nil {
			src.Mac = srcMac
		}
	}

	if respH != nil && respP != nil {
		dst = toNetEndpoint(*respH, int(*respP))
		if dstMac := helpers.GetString(lv, "resp_l2_addr"); dstMac != nil {
			dst.Mac = dstMac
		}
		if cc := helpers.GetString(lv, "resp_cc"); cc != nil {
			dst.Location = &v1_5_0.GeoLocation{Country: cc}
		}
	}

	proto := helpers.GetString(lv, "proto")
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
	if communityUid := helpers.GetString(lv, "community_id"); communityUid != nil {
		connInfo.CommunityUid = communityUid
	}
	if pn != 0 {
		pnum := int32(pn)
		connInfo.ProtocolNum = &pnum
	}
	if directionID != nil {
		connInfo.DirectionId = *directionID
	}
	if h := helpers.GetString(lv, "history"); h != nil {
		connInfo.FlagHistory = h
	}
	if connInfo.ProtocolName == nil && connInfo.ProtocolNum == nil && connInfo.FlagHistory == nil {
		connInfo = nil
	}

	// Traffic counters
	ob := helpers.GetInt64(lv, "orig_bytes")
	rb := helpers.GetInt64(lv, "resp_bytes")
	mb := helpers.GetInt64(lv, "missed_bytes")
	op := helpers.GetInt64(lv, "orig_pkts")
	rp := helpers.GetInt64(lv, "resp_pkts")

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
	if s := helpers.GetString(lv, "service"); s != nil {
		appName = s
	}
	var statusCode *string
	if cs := helpers.GetString(lv, "conn_state"); cs != nil {
		statusCode = cs
	}

	// Observables (hostname lists)
	objs := buildObservablesFromLogview(lv)

	var unmapped OCSFUnMapped

	if missedBytes := helpers.GetInt64(lv, "missed_bytes"); missedBytes != nil {
		unmapped.MissedBytes = missedBytes
	}

	if vlan := helpers.GetInt64(lv, "vlan"); vlan != nil {
		unmapped.VLAN = vlan
	}

	app, _ := helpers.GetStringList(lv, "app")
	unmapped.App = app

	tunnelParents, _ := helpers.GetStringList(lv, "tunnel_parents")
	unmapped.TunnelParent = tunnelParents

	suriIDs, _ := helpers.GetStringList(lv, "suri_ids")
	unmapped.SuriIDs = suriIDs

	var sp SPCap
	sp.Trigger = helpers.GetString(lv, "spcap.trigger")

	sp.URL = helpers.GetString(lv, "spcap.url")

	if rule := helpers.GetInt64(lv, "spcap.rule"); rule != nil {
		sp.Rule = rule
	}

	if localOrig != nil {
		unmapped.LocalOrig = localOrig
	}

	if localResp != nil {
		unmapped.LocalResp = localResp
	}

	if origIPBytes := helpers.GetInt64(lv, "orig_ip_bytes"); origIPBytes != nil {
		unmapped.OrigIPBytes = origIPBytes
	}

	if respIPBytes := helpers.GetInt64(lv, "resp_ip_bytes"); respIPBytes != nil {
		unmapped.RespIPBytes = respIPBytes
	}

	if pcr := helpers.GetFloat64(lv, "pcr"); pcr != nil {
		unmapped.Pcr = pcr
	}

	if corelightShunted := helpers.GetBool(lv, "corelight_shunted"); corelightShunted != nil {
		unmapped.CorelightShunted = corelightShunted
	}
	unmapped.SPCap = &sp

	var unmappedPtr *string
	if b, err := json.Marshal(unmapped); err == nil {
		s := string(b)
		unmappedPtr = &s
	}

	na := NetworkActivityAlias{
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

	return &na, nil
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

func buildObservablesFromLogview(v tangent_sdk.Log) []v1_5_0.Observable {
	var out []v1_5_0.Observable

	srcProvider := helpers.GetString(v, "id.orig_h_name.src")
	if vals, ok := helpers.GetStringList(v, "id.orig_h_name.vals"); ok {
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

	dstProvider := helpers.GetString(v, "id.resp_h_name.src")
	if vals, ok := helpers.GetStringList(v, "id.resp_h_name.vals"); ok {
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

func init() {
	tangent_sdk.Wire[*NetworkActivityAlias](
		metadata,
		selectors,
		ZeekMapper,
	)
}
func main() {}
