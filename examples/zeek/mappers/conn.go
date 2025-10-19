package mappers

import (
	"encoding/json"
	"math"
	"strings"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

type ZeekConn struct {
	// --- normalized times ---
	TimeMs      int64 // from "ts" (RFC3339 or float seconds)
	WriteTimeMs int64 // from "_write_ts" (RFC3339)

	// --- raw time & meta (optional, useful for debugging/round-trips) ---
	RawTS      string `json:"ts,omitempty"`
	RawWriteTS string `json:"_write_ts,omitempty"`
	Path       string `json:"_path,omitempty"`
	SystemName string `json:"_system_name,omitempty"`

	// --- core ids/endpoints ---
	UID   string `json:"uid"`
	OrigH string `json:"id.orig_h"`
	OrigP int    `json:"id.orig_p"`
	RespH string `json:"id.resp_h"`
	RespP int    `json:"id.resp_p"`

	// --- hostname annotations (for observables) ---
	OrigHNameSrc  string   `json:"id.orig_h_name.src,omitempty"`
	OrigHNameVals []string `json:"id.orig_h_name.vals,omitempty"`
	RespHNameSrc  string   `json:"id.resp_h_name.src,omitempty"`
	RespHNameVals []string `json:"id.resp_h_name.vals,omitempty"`

	// --- protocol/service/direction-ish flags ---
	Proto     string `json:"proto"`
	Service   string `json:"service,omitempty"`
	ConnState string `json:"conn_state,omitempty"`
	LocalOrig *bool  `json:"local_orig,omitempty"`
	LocalResp *bool  `json:"local_resp,omitempty"`

	// --- sizes/packets/duration ---
	Duration    *float64 `json:"duration,omitempty"`
	OrigBytes   *int64   `json:"orig_bytes,omitempty"`
	RespBytes   *int64   `json:"resp_bytes,omitempty"`
	MissedBytes *int64   `json:"missed_bytes,omitempty"`
	OrigPkts    *int64   `json:"orig_pkts,omitempty"`
	RespPkts    *int64   `json:"resp_pkts,omitempty"`
	OrigIPBytes *int64   `json:"orig_ip_bytes,omitempty"`
	RespIPBytes *int64   `json:"resp_ip_bytes,omitempty"`

	// --- L2 / geo / community ---
	OrigMAC     *string `json:"orig_l2_addr,omitempty"`
	RespMAC     *string `json:"resp_l2_addr,omitempty"`
	RespCC      *string `json:"resp_cc,omitempty"`
	CommunityID *string `json:"community_id,omitempty"`

	// --- extras seen in the sample ---
	App              []string `json:"app,omitempty"`
	CorelightShunted *bool    `json:"corelight_shunted,omitempty"`
	PCR              *float64 `json:"pcr,omitempty"`
	SuriIDs          []string `json:"suri_ids,omitempty"`
	SpcapRule        *int     `json:"spcap.rule,omitempty"`
	SpcapTrigger     string   `json:"spcap.trigger,omitempty"`
	SpcapURL         string   `json:"spcap.url,omitempty"`
	TunnelParents    []string `json:"tunnel_parents,omitempty"`
	VLAN             *int     `json:"vlan,omitempty"`
	History          string   `json:"history,omitempty"`
}

func MapZeekConn(in map[string]any) v1_5_0.NetworkActivity {
	zc := FromGenericConn(in)

	_, tms := parseZeekTime(in, "ts")
	_, writeMs := parseZeekTime(in, "_write_ts")

	const classUID int32 = 4001 // network_activity
	const categoryUID int32 = 4 // Network Activity
	var activityID int32 = 2
	var severityID int32 = 1
	typeUID := int64(classUID)*100 + int64(activityID)

	var duration *int64
	if zc.Duration != nil {
		ms := int64(math.Round(*zc.Duration))
		duration = &ms
	}

	var startTime, endTime int64
	if duration != nil {
		endTime = tms + *duration
		startTime = tms
	}

	var directionID *int32
	switch {
	case zc.LocalOrig != nil && *zc.LocalOrig && zc.LocalResp != nil && !*zc.LocalResp:
		out := int32(2)
		directionID = &out
	case zc.LocalOrig != nil && !*zc.LocalOrig && zc.LocalResp != nil && *zc.LocalResp:
		in := int32(1)
		directionID = &in
	}

	protoNum, protoName := protoToOCSF(zc.Proto)
	connInfo := &v1_5_0.NetworkConnectionInformation{}
	if protoName != "" {
		pn := protoName
		connInfo.ProtocolName = &pn
	}
	if protoNum != 0 {
		pnum := int32(protoNum)
		connInfo.ProtocolNum = &pnum
	}
	connInfo.CommunityUid = zc.CommunityID
	if directionID != nil {
		connInfo.DirectionId = *directionID
	}
	if zc.History != "" {
		h := zc.History
		connInfo.FlagHistory = &h
	}
	if connInfo.ProtocolName == nil && connInfo.ProtocolNum == nil && connInfo.FlagHistory == nil && connInfo.CommunityUid == nil {
		connInfo = nil
	}

	var totalBytes, totalPkts *int64
	if zc.OrigBytes != nil || zc.RespBytes != nil || zc.OrigPkts != nil || zc.RespPkts != nil {
		tb, tp := int64(0), int64(0)
		if zc.OrigBytes != nil {
			tb += *zc.OrigBytes
		}
		if zc.RespBytes != nil {
			tb += *zc.RespBytes
		}
		if zc.OrigPkts != nil {
			tp += *zc.OrigPkts
		}
		if zc.RespPkts != nil {
			tp += *zc.RespPkts
		}
		totalBytes, totalPkts = &tb, &tp
	}
	var traffic *v1_5_0.NetworkTraffic
	if zc.OrigBytes != nil || zc.RespBytes != nil || zc.MissedBytes != nil || zc.OrigPkts != nil || zc.RespPkts != nil {
		traffic = &v1_5_0.NetworkTraffic{
			BytesOut:    zc.OrigBytes,
			PacketsOut:  zc.OrigPkts,
			BytesIn:     zc.RespBytes,
			PacketsIn:   zc.RespPkts,
			BytesMissed: zc.MissedBytes,
			Bytes:       totalBytes,
			Packets:     totalPkts,
		}
	}

	// Endpoints
	src := toNetEndpoint(zc.OrigH, zc.OrigP)
	if mac := getString(in, "orig_l2_addr"); mac != "" {
		src.Mac = &mac
	}
	dst := toNetEndpoint(zc.RespH, zc.RespP)
	if mac := getString(in, "resp_l2_addr"); mac != "" {
		dst.Mac = &mac
	}
	// dst geolocation
	if cc := getString(in, "resp_cc"); cc != "" {
		dst.Location = &v1_5_0.GeoLocation{Country: &cc}
	}

	// Metadata (1.5)
	ver := "1.5.0"
	productName := "Zeek"
	vendorName := "Zeek"
	logName := "conn"
	uid := zc.UID
	md := v1_5_0.Metadata{
		Version: ver,
		Uid:     &uid,
		Product: v1_5_0.Product{
			Name:       &productName,
			VendorName: &vendorName,
		},
		LogName: &logName,
	}

	if writeMs != 0 {
		sec := writeMs / 1000
		md.LoggedTime = sec
	}
	if name := getString(in, "_system_name"); name != "" {
		md.Loggers = []v1_5_0.Logger{{Name: &name}}
	}
	if p := getString(in, "_path"); p != "" {
		md.LogName = &p
	}

	var appName *string
	if s := strings.TrimSpace(getString(in, "service")); s != "" {
		appName = &s
	}

	var statusCode *string
	if zc.ConnState != "" {
		cs := zc.ConnState
		statusCode = &cs
	}

	observables := buildConnObservables(in)

	unmappedObj := map[string]any{}
	copyIfPresent := func(key string) {
		if v, ok := in[key]; ok {
			parentKeys := strings.Split(key, ".")
			childMap := unmappedObj

			for idx, pKey := range parentKeys {
				if idx+1 == len(parentKeys) {
					childMap[pKey] = v
				} else {
					if _, ok := childMap[pKey]; !ok {
						childMap[pKey] = map[string]any{}
					}
					childMap = childMap[pKey].(map[string]any)
				}
			}
		}
	}
	for _, k := range []string{
		"missed_bytes", "vlan",
		"app", "tunnel_parents", "local_orig",
		"local_resp", "orig_ip_bytes", "resp_ip_bytes",
		"suri_ids", "spcap.rule", "spcap.trigger", "spcap.url",
		"pcr", "corelight_shunted",
	} {
		copyIfPresent(k)
	}
	var unmappedPtr *string
	if b, err := json.Marshal(unmappedObj); err == nil && len(unmappedObj) > 0 {
		s := string(b)
		unmappedPtr = &s
	}

	na := v1_5_0.NetworkActivity{
		ActivityId:  activityID,
		CategoryUid: categoryUID,
		ClassUid:    classUID,
		SeverityId:  severityID,
		TypeUid:     typeUID,
		Time:        tms,
		Metadata:    md,

		AppName: appName,

		SrcEndpoint: src,
		DstEndpoint: dst,

		ConnectionInfo: connInfo,
		Traffic:        traffic,

		Duration:   duration,
		StatusCode: statusCode,

		Observables: observables,
		Unmapped:    unmappedPtr,
	}

	if duration != nil {
		na.StartTime = startTime
		na.EndTime = endTime
	}

	return na
}

// TODO: use struct directly.
func FromGenericConn(m map[string]any) ZeekConn {
	c := ZeekConn{}

	// Parse ts / _write_ts from RFC3339 OR float seconds
	_, c.TimeMs = parseZeekTime(m, "ts")
	_, c.WriteTimeMs = parseZeekTime(m, "_write_ts")

	c.UID = getString(m, "uid")
	c.OrigH = getString(m, "id.orig_h")
	c.OrigP = int(getInt64(m, "id.orig_p"))
	c.RespH = getString(m, "id.resp_h")
	c.RespP = int(getInt64(m, "id.resp_p"))

	c.Proto = getString(m, "proto")
	c.Service = getString(m, "service")

	if v, ok := getAny(m, "duration"); ok {
		f := toFloat(v)
		c.Duration = &f
	}
	if v, ok := getAny(m, "orig_bytes"); ok {
		iv := toInt64(v)
		c.OrigBytes = &iv
	}
	if v, ok := getAny(m, "resp_bytes"); ok {
		iv := toInt64(v)
		c.RespBytes = &iv
	}
	c.ConnState = getString(m, "conn_state")
	if v, ok := getAny(m, "local_orig"); ok {
		b := toBool(v)
		c.LocalOrig = &b
	}
	if v, ok := getAny(m, "local_resp"); ok {
		b := toBool(v)
		c.LocalResp = &b
	}
	if v, ok := getAny(m, "missed_bytes"); ok {
		iv := toInt64(v)
		c.MissedBytes = &iv
	}
	c.History = getString(m, "history")
	if v, ok := getAny(m, "orig_pkts"); ok {
		iv := toInt64(v)
		c.OrigPkts = &iv
	}
	if v, ok := getAny(m, "resp_pkts"); ok {
		iv := toInt64(v)
		c.RespPkts = &iv
	}
	if v, ok := getAny(m, "orig_ip_bytes"); ok {
		iv := toInt64(v)
		c.OrigIPBytes = &iv
	}
	if v, ok := getAny(m, "resp_ip_bytes"); ok {
		iv := toInt64(v)
		c.RespIPBytes = &iv
	}

	if s := getString(m, "orig_l2_addr"); s != "" {
		c.OrigMAC = &s
	}
	if s := getString(m, "resp_l2_addr"); s != "" {
		c.RespMAC = &s
	}
	if s := getString(m, "resp_cc"); s != "" {
		c.RespCC = &s
	}
	if s := getString(m, "community_id"); s != "" {
		c.CommunityID = &s
	}

	return c
}

func buildConnObservables(in map[string]any) []v1_5_0.Observable {
	var out []v1_5_0.Observable

	srcProvider := getString(in, "id.orig_h_name.src")
	if vals, ok := getAny(in, "id.orig_h_name.vals"); ok {
		for _, s := range toStringSlice(vals) {
			name := "src_endpoint.hostname"
			typ := int32(1)
			val := s
			base := float64(0)
			scoreID := int32(0)
			reputation := &v1_5_0.Reputation{
				Provider:  &srcProvider,
				BaseScore: base,
				ScoreId:   scoreID,
			}
			out = append(out, v1_5_0.Observable{
				Name:       &name,
				TypeId:     typ,
				Value:      &val,
				Reputation: reputation,
			})
		}
	}
	dstProvider := getString(in, "id.resp_h_name.src")
	if vals, ok := getAny(in, "id.resp_h_name.vals"); ok {
		for _, s := range toStringSlice(vals) {
			name := "dst_endpoint.hostname"
			typ := int32(1)
			val := s
			base := float64(0)
			scoreID := int32(0)
			reputation := &v1_5_0.Reputation{
				Provider:  &dstProvider,
				BaseScore: base,
				ScoreId:   scoreID,
			}
			out = append(out, v1_5_0.Observable{
				Name:       &name,
				TypeId:     typ,
				Value:      &val,
				Reputation: reputation,
			})
		}
	}
	return out
}
