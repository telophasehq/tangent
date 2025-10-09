package mappers

import (
	"encoding/json"
	"time"
)

type EksLog struct {
	Timestamp   string            `json:"timestamp"`
	ContainerID string            `json:"container_id"`
	Container   string            `json:"container_name"`
	Image       string            `json:"image"`
	Host        string            `json:"host"`
	SourceType  string            `json:"source_type"`
	Stream      string            `json:"stream"`
	Label       map[string]string `json:"label"`
	Message     string            `json:"message"`
	Extra       map[string]any    `json:"extra"`
}

type CloudtrailEvent struct {
	EventVersion string    `json:"eventVersion"`
	EventID      string    `json:"eventID"`
	EventTime    time.Time `json:"eventTime"`
	EventSource  string    `json:"eventSource"`
	EventName    string    `json:"eventName"`
	AwsRegion    string    `json:"awsRegion"`
	EventType    string    `json:"eventType"`
	SourceIP     string    `json:"sourceIPAddress"`
	UserAgent    string    `json:"userAgent"`

	UserIdentity UserIdentity `json:"userIdentity"`

	ErrorCode    *string `json:"errorCode,omitempty"`
	ErrorMessage *string `json:"errorMessage,omitempty"`

	RequestParameters   json.RawMessage `json:"requestParameters,omitempty"`
	ResponseElements    json.RawMessage `json:"responseElements,omitempty"`
	AdditionalEventData json.RawMessage `json:"additionalEventData,omitempty"`

	Resources []ResourceRef `json:"resources,omitempty"`

	ReadOnly            *bool           `json:"readOnly,omitempty"`
	ManagementEvent     *bool           `json:"managementEvent,omitempty"`
	RecipientAccountID  string          `json:"recipientAccountId"`
	SharedEventID       *string         `json:"sharedEventID,omitempty"`
	ServiceEventDetails json.RawMessage `json:"serviceEventDetails,omitempty"`
	TlsDetails          json.RawMessage `json:"tlsDetails,omitempty"`
	VpcEndpointID       *string         `json:"vpcEndpointId,omitempty"`
}

type UserIdentity struct {
	Type        string  `json:"type"`
	PrincipalID string  `json:"principalId"`
	Arn         string  `json:"arn"`
	AccountID   *string `json:"accountId,omitempty"`
	AccessKeyID string  `json:"accessKeyId,omitempty"`

	UserName *string `json:"userName,omitempty"`

	InvokedBy *string `json:"invokedBy,omitempty"`

	SessionContext *SessionContext `json:"sessionContext,omitempty"`
}

type SessionContext struct {
	Attributes struct {
		MfaAuthenticated string    `json:"mfaAuthenticated"`
		CreationDate     time.Time `json:"creationDate"`
	} `json:"attributes"`

	// present when the session was assumed via STS
	SessionIssuer *SessionIssuer `json:"sessionIssuer,omitempty"`
}

type SessionIssuer struct {
	Type        string `json:"type"`
	PrincipalID string `json:"principalId"`
	Arn         string `json:"arn"`
	AccountID   string `json:"accountId"`
	UserName    string `json:"userName"`
}

type ResourceRef struct {
	ARN       string  `json:"ARN"`
	AccountID *string `json:"accountId,omitempty"`
	Type      string  `json:"type,omitempty"`
}
