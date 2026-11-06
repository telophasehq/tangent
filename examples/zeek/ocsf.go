package main

// OCSF 1.5.0 Network Activity (class 4001), cut down to the fields this
// mapper fills. Names and JSON tags follow the OCSF schema (and
// github.com/telophasehq/go-ocsf's v1_5_0 package): a field without
// omitempty is required by OCSF and always written. `unmapped` is an object,
// as OCSF has it, where go-ocsf types it as a JSON string.
//
// The types are here rather than imported from go-ocsf because go-ocsf's
// package pulls in apache/arrow-go, whose hash/maphash import TinyGo 0.43
// cannot build with Go 1.27 ("undefined: abi.MapType").

type NetworkActivity struct {
	ActivityId     int32                         `json:"activity_id"`
	AppName        *string                       `json:"app_name,omitempty"`
	CategoryUid    int32                         `json:"category_uid"`
	ClassUid       int32                         `json:"class_uid"`
	ConnectionInfo *NetworkConnectionInformation `json:"connection_info,omitempty"`
	DstEndpoint    *NetworkEndpoint              `json:"dst_endpoint,omitempty"`
	Duration       *int64                        `json:"duration,omitempty"` // milliseconds
	EndTime        int64                         `json:"end_time,omitempty"`
	Metadata       Metadata                      `json:"metadata"`
	Observables    []Observable                  `json:"observables,omitempty"`
	SeverityId     int32                         `json:"severity_id"`
	SrcEndpoint    *NetworkEndpoint              `json:"src_endpoint,omitempty"`
	StartTime      int64                         `json:"start_time,omitempty"`
	StatusCode     *string                       `json:"status_code,omitempty"`
	Time           int64                         `json:"time"`
	Traffic        *NetworkTraffic               `json:"traffic,omitempty"`
	TypeUid        int64                         `json:"type_uid"`
	Unmapped       *OCSFUnMapped                 `json:"unmapped,omitempty"` // OCSF's unmapped is an object
}

type NetworkEndpoint struct {
	Ip       *string      `json:"ip,omitempty"`
	Location *GeoLocation `json:"location,omitempty"`
	Mac      *string      `json:"mac,omitempty"`
	Port     *int32       `json:"port,omitempty"`
}

type GeoLocation struct {
	Country *string `json:"country,omitempty"`
}

type NetworkConnectionInformation struct {
	CommunityUid *string `json:"community_uid,omitempty"`
	DirectionId  int32   `json:"direction_id"`
	FlagHistory  *string `json:"flag_history,omitempty"`
	ProtocolName *string `json:"protocol_name,omitempty"`
	ProtocolNum  *int32  `json:"protocol_num,omitempty"`
}

type NetworkTraffic struct {
	Bytes       *int64 `json:"bytes,omitempty"`
	BytesIn     *int64 `json:"bytes_in,omitempty"`
	BytesMissed *int64 `json:"bytes_missed,omitempty"`
	BytesOut    *int64 `json:"bytes_out,omitempty"`
	Packets     *int64 `json:"packets,omitempty"`
	PacketsIn   *int64 `json:"packets_in,omitempty"`
	PacketsOut  *int64 `json:"packets_out,omitempty"`
}

type Metadata struct {
	LogName    *string  `json:"log_name,omitempty"`
	LoggedTime int64    `json:"logged_time,omitempty"`
	Loggers    []Logger `json:"loggers,omitempty"`
	Product    Product  `json:"product"`
	Uid        *string  `json:"uid,omitempty"`
	Version    string   `json:"version"`
}

type Product struct {
	Name       *string `json:"name,omitempty"`
	VendorName *string `json:"vendor_name,omitempty"`
}

type Logger struct {
	Name *string `json:"name,omitempty"`
}

type Observable struct {
	Name       *string     `json:"name,omitempty"`
	Reputation *Reputation `json:"reputation,omitempty"`
	TypeId     int32       `json:"type_id"`
	Value      *string     `json:"value,omitempty"`
}

type Reputation struct {
	BaseScore float64 `json:"base_score"`
	Provider  *string `json:"provider,omitempty"`
	ScoreId   int32   `json:"score_id"`
}
