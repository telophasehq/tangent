package mappers

import (
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

type ZeekConn struct {
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

func MapZeekConn(in []byte) (*v1_5_0.NetworkActivity, error) {
	var zc ZeekConn

	if err := json.Unmarshal(in, &zc); err != nil {
		return nil, err
	}

	ts, err := time.Parse(time.RFC3339Nano, zc.RawTS)
	if err != nil {
		return nil, err
	}
	timeMs := ts.UnixMilli()

	wts, err := time.Parse(time.RFC3339Nano, zc.RawWriteTS)
	if err != nil {
		return nil, err
	}
	writeTimeMs := wts.UnixMilli()

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
		endTime = timeMs + *duration
		startTime = timeMs
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
	if zc.OrigMAC != nil && *zc.OrigMAC != "" {
		src.Mac = zc.OrigMAC
	}
	dst := toNetEndpoint(zc.RespH, zc.RespP)
	if zc.RespMAC != nil && *zc.RespMAC != "" {
		dst.Mac = zc.RespMAC
	}
	// dst geolocation
	if zc.RespCC != nil && *zc.RespCC != "" {
		dst.Location = &v1_5_0.GeoLocation{Country: zc.RespCC}
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

	if writeTimeMs != 0 {
		sec := writeTimeMs
		md.LoggedTime = sec
	}
	if zc.SystemName != "" {
		md.Loggers = []v1_5_0.Logger{{Name: &zc.SystemName}}
	}
	if zc.Path != "" {
		md.LogName = &zc.Path
	}

	var appName *string
	if s := strings.TrimSpace(zc.Service); s != "" {
		appName = &s
	}

	var statusCode *string
	if zc.ConnState != "" {
		cs := zc.ConnState
		statusCode = &cs
	}

	observables := buildConnObservables(&zc)

	unmappedObj := map[string]any{}
	if zc.MissedBytes != nil {
		unmappedObj["missed_bytes"] = *zc.MissedBytes
	}
	if zc.VLAN != nil {
		unmappedObj["vlan"] = *zc.VLAN
	}
	unmappedObj["app"] = zc.App
	unmappedObj["tunnel_parents"] = zc.TunnelParents
	unmappedObj["suri_ids"] = zc.SuriIDs
	unmappedObj["spcap"] = map[string]any{}
	unmappedObj["spcap"].(map[string]any)["trigger"] = zc.SpcapTrigger
	unmappedObj["spcap"].(map[string]any)["url"] = zc.SpcapURL

	if zc.LocalOrig != nil {
		unmappedObj["local_orig"] = *zc.LocalOrig
	}
	if zc.LocalResp != nil {
		unmappedObj["local_resp"] = *zc.LocalResp
	}
	if zc.OrigIPBytes != nil {
		unmappedObj["orig_ip_bytes"] = *zc.OrigIPBytes
	}
	if zc.RespIPBytes != nil {
		unmappedObj["resp_ip_bytes"] = *zc.RespIPBytes
	}
	if zc.SpcapRule != nil {
		unmappedObj["spcap"].(map[string]any)["rule"] = zc.SpcapRule
	}
	if zc.PCR != nil {
		unmappedObj["pcr"] = *zc.PCR
	}
	if zc.CorelightShunted != nil {
		unmappedObj["corelight_shunted"] = *zc.CorelightShunted
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
		Time:        timeMs,
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

	return &na, nil
}

func buildConnObservables(zc *ZeekConn) []v1_5_0.Observable {
	var out []v1_5_0.Observable

	srcProvider := zc.OrigHNameSrc
	for _, s := range zc.OrigHNameVals {
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

	dstProvider := zc.RespHNameSrc
	for _, s := range zc.RespHNameVals {
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
	return out
}
