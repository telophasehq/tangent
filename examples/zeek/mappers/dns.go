package mappers

import (
	"encoding/json"
	"strings"

	"github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

type ZeekDNS struct {
	TimeMs      int64
	WriteTimeMs int64

	UID   string
	Proto string

	OrigH string
	OrigP int
	RespH string
	RespP int

	Query      string
	QClass     *int
	QClassName *string
	QType      *int
	QTypeName  *string
	TransID    *int
	RTT        *float64

	Rcode     *int
	RcodeName *string

	AA *bool
	TC *bool
	RD *bool
	RA *bool
	Z  *int

	Answers []string
	TTLs    []int64

	Rejected *bool
}

func MapZeekDNS(in map[string]any) v1_5_0.DNSActivity {
	z := FromGenericDNS(in)

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
	if z.WriteTimeMs != 0 {
		sec := z.WriteTimeMs
		md.LoggedTime = sec
	}
	if sys := getString(in, "_system_name"); sys != "" {
		md.Loggers = []v1_5_0.Logger{{Name: &sys}}
	}
	if p := getString(in, "_path"); p != "" {
		md.LogName = &p
	}

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
	for _, k := range []string{
		"icann_host_subdomain", "icann_domain", "icann_tld",
		"is_trusted_domain", "qclass", "qtype",
	} {
		if v, ok := in[k]; ok {
			unmapped[k] = v
		}
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

		Time:      z.WriteTimeMs,
		StartTime: z.WriteTimeMs,

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

	return out
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

func FromGenericDNS(m map[string]any) ZeekDNS {
	z := ZeekDNS{}

	// times
	_, z.TimeMs = parseZeekTime(m, "ts")
	_, z.WriteTimeMs = parseZeekTime(m, "_write_ts")

	z.UID = getString(m, "uid")
	z.Proto = strings.ToLower(getString(m, "proto"))
	z.Query = getString(m, "query")

	z.OrigH = getString(m, "id.orig_h")
	z.OrigP = int(getInt64(m, "id.orig_p"))
	z.RespH = getString(m, "id.resp_h")
	z.RespP = int(getInt64(m, "id.resp_p"))

	if v, ok := getAny(m, "qclass"); ok {
		iv, _ := toInt(v)
		z.QClass = &iv
	}
	if v, ok := getAny(m, "qclass_name"); ok {
		s := toString(v)
		z.QClassName = &s
	}
	if v, ok := getAny(m, "qtype"); ok {
		iv, _ := toInt(v)
		z.QType = &iv
	}
	if v, ok := getAny(m, "qtype_name"); ok {
		s := toString(v)
		z.QTypeName = &s
	}
	if v, ok := getAny(m, "trans_id"); ok {
		iv, _ := toInt(v)
		z.TransID = &iv
	}
	if v, ok := getAny(m, "rtt"); ok {
		f := toFloat(v)
		z.RTT = &f
	}

	if v, ok := getAny(m, "rcode"); ok {
		iv, _ := toInt(v)
		z.Rcode = &iv
	}
	if v, ok := getAny(m, "rcode_name"); ok {
		s := toString(v)
		z.RcodeName = &s
	}

	if v, ok := getAny(m, "AA"); ok {
		b := toBool(v)
		z.AA = &b
	}
	if v, ok := getAny(m, "TC"); ok {
		b := toBool(v)
		z.TC = &b
	}
	if v, ok := getAny(m, "RD"); ok {
		b := toBool(v)
		z.RD = &b
	}
	if v, ok := getAny(m, "RA"); ok {
		b := toBool(v)
		z.RA = &b
	}
	if v, ok := getAny(m, "Z"); ok {
		iv, _ := toInt(v)
		z.Z = &iv
	}

	if v, ok := getAny(m, "answers"); ok {
		z.Answers = toStringSlice(v)
	}
	if v, ok := getAny(m, "TTLs"); ok {
		z.TTLs = toInt64Slice(v)
	}

	if v, ok := getAny(m, "rejected"); ok {
		b := toBool(v)
		z.Rejected = &b
	}

	return z
}
