package mappers

import (
	"encoding/json"
	"time"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

type ZeekDNS struct {

	// ---- raw meta/time ----
	Path       string `json:"_path,omitempty"`        // "dns"
	SystemName string `json:"_system_name,omitempty"` // "sensor"
	RawWriteTS string `json:"_write_ts,omitempty"`    // RFC3339
	RawTS      string `json:"ts"`                     // RFC3339

	// ---- ids / endpoints ----
	UID   string `json:"uid"`
	Proto string `json:"proto"` // "udp"/"tcp"

	OrigH string `json:"id.orig_h"`
	OrigP int    `json:"id.orig_p"`
	RespH string `json:"id.resp_h"`
	RespP int    `json:"id.resp_p"`

	// ---- query / response ----
	Query      string   `json:"query"`
	QClass     *int     `json:"qclass,omitempty"`
	QClassName *string  `json:"qclass_name,omitempty"`
	QType      *int     `json:"qtype,omitempty"`
	QTypeName  *string  `json:"qtype_name,omitempty"`
	TransID    *int     `json:"trans_id,omitempty"`
	RTT        *float64 `json:"rtt,omitempty"` // seconds

	Rcode     *int    `json:"rcode,omitempty"`
	RcodeName *string `json:"rcode_name,omitempty"`

	// ---- flags ----
	AA *bool `json:"AA,omitempty"`
	TC *bool `json:"TC,omitempty"`
	RD *bool `json:"RD,omitempty"`
	RA *bool `json:"RA,omitempty"`
	Z  *int  `json:"Z,omitempty"`

	Answers []string `json:"answers,omitempty"`
	TTLs    []int64  `json:"TTLs,omitempty"`

	Rejected *bool `json:"rejected,omitempty"`

	// ---- extra ICANN/enrichment fields ----
	ICANNDomain        *string `json:"icann_domain,omitempty"`
	ICANNHostSubdomain *string `json:"icann_host_subdomain,omitempty"`
	ICANNTLD           *string `json:"icann_tld,omitempty"`
	IsTrustedDomain    *bool   `json:"is_trusted_domain,omitempty"`
}

func MapZeekDNS(in []byte) (*v1_5_0.DNSActivity, error) {
	var z ZeekDNS

	if err := json.Unmarshal(in, &z); err != nil {
		return nil, err
	}

	wts, err := time.Parse(time.RFC3339Nano, z.RawWriteTS)
	if err != nil {
		return nil, err
	}
	writeTimeMs := wts.UnixMilli()

	const classUID int32 = 4003
	const categoryUID int32 = 4
	activityID := dnsActivityID(z)
	var severityID int32 = 1
	typeUID := int64(classUID)*100 + int64(activityID)

	_, protoName := protoToOCSF(z.Proto)
	var connInfo *v1_5_0.NetworkConnectionInformation
	if protoName != "" {
		pn := protoName
		connInfo = &v1_5_0.NetworkConnectionInformation{ProtocolName: &pn}
	}

	var qTypePtr, qClassPtr *string
	if z.QTypeName != nil {
		qTypePtr = z.QTypeName
	} else if z.QType != nil {
		s := dnsQTypeName(*z.QType)
		qTypePtr = &s
	}
	if z.QClassName != nil {
		qClassPtr = z.QClassName
	}
	var packetUidPtr *int32
	if z.TransID != nil {
		pu := int32(*z.TransID)
		packetUidPtr = &pu
	}

	q := &v1_5_0.DNSQuery{
		Hostname:  z.Query,
		Type:      qTypePtr,
		Class:     qClassPtr,
		PacketUid: packetUidPtr,
	}

	var answers []v1_5_0.DNSAnswer
	if len(z.Answers) > 0 {
		for i, a := range z.Answers {
			ans := v1_5_0.DNSAnswer{Rdata: a}
			if i < len(z.TTLs) {
				ttl := int32(z.TTLs[i])
				ans.Ttl = &ttl
			}
			if packetUidPtr != nil {
				ans.PacketUid = packetUidPtr
			}
			if ids := dnsFlagIDs(z); len(ids) > 0 {
				ans.FlagIds = ids
			}
			answers = append(answers, ans)
		}
	}

	// rcode
	var rcodePtr *string
	var rcodeIdPtr *int32
	if z.RcodeName != nil {
		rcodePtr = z.RcodeName
	} else if z.Rcode != nil {
		s := dnsRcodeName(*z.Rcode)
		rcodePtr = &s
	}
	if z.Rcode != nil {
		rid := int32(*z.Rcode)
		rcodeIdPtr = &rid
	}

	ver := "1.5.0"
	product := "Zeek"
	vendor := "Zeek"
	logName := "dns"
	md := v1_5_0.Metadata{
		Version: ver,
		Uid:     &z.UID,
		Product: v1_5_0.Product{Name: &product, VendorName: &vendor},
		LogName: &logName,
	}
	if writeTimeMs != 0 {
		md.LoggedTime = writeTimeMs
	}
	md.Loggers = []v1_5_0.Logger{{Name: &z.SystemName}}
	md.LogName = &z.Path

	var rtPtr float64
	if z.RTT != nil {
		rtPtr = *z.RTT
	}

	var statusId *int32
	if z.Rejected != nil {
		if *z.Rejected {
			failed := int32(2)
			statusId = &failed
		} else {
			success := int32(1)
			statusId = &success
		}
	}

	unmapped := map[string]any{}
	if z.ICANNHostSubdomain != nil {
		unmapped["icann_host_subdomain"] = *z.ICANNHostSubdomain
	}
	if z.ICANNDomain != nil {
		unmapped["icann_domain"] = *z.ICANNDomain
	}
	if z.ICANNTLD != nil {
		unmapped["icann_tld"] = *z.ICANNTLD
	}
	if z.IsTrustedDomain != nil {
		unmapped["is_trusted_domain"] = *z.IsTrustedDomain
	}
	if z.QClass != nil {
		unmapped["qclass"] = *z.QClass
	}
	if z.QType != nil {
		unmapped["qtype"] = *z.QType
	}
	var unmappedPtr *string
	if len(unmapped) > 0 {
		if b, err := json.Marshal(unmapped); err == nil {
			s := string(b)
			unmappedPtr = &s
		}
	}

	out := v1_5_0.DNSActivity{
		ActivityId:  activityID,
		CategoryUid: categoryUID,
		ClassUid:    classUID,
		SeverityId:  severityID,
		StatusId:    statusId,
		TypeUid:     typeUID,

		Time:      writeTimeMs,
		StartTime: writeTimeMs,

		Metadata:       md,
		SrcEndpoint:    toNetEndpoint(z.OrigH, z.OrigP),
		DstEndpoint:    toNetEndpoint(z.RespH, z.RespP),
		ConnectionInfo: connInfo,

		Query:        q,
		Answers:      answers,
		Rcode:        rcodePtr,
		RcodeId:      rcodeIdPtr,
		ResponseTime: int64(rtPtr * 1000),

		Unmapped: unmappedPtr,
	}

	return &out, nil
}

func dnsFlagIDs(z ZeekDNS) []int32 {
	var ids []int32
	if z.AA != nil && *z.AA {
		ids = append(ids, 1)
	} // example: AA
	if z.TC != nil && *z.TC {
		ids = append(ids, 2)
	} // example: TC
	if z.RD != nil && *z.RD {
		ids = append(ids, 3)
	} // example: RD
	if z.RA != nil && *z.RA {
		ids = append(ids, 4)
	} // example: RA
	return ids
}

func dnsActivityID(z ZeekDNS) int32 {
	hasQuery := z.Query != ""
	hasResp := (len(z.Answers) > 0) || ((z.RcodeName != nil && *z.RcodeName != "") || (z.Rcode != nil && *z.Rcode != 0))

	var activity int
	switch {
	case hasQuery && hasResp:
		activity = 6 // Traffic
	case hasQuery && !hasResp:
		activity = 1 // Query
	case !hasQuery && hasResp:
		activity = 2 // Response
	default:
		activity = 0 // Unknown
	}
	return int32(activity)
}
