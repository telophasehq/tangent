package mappers

import (
	"strings"
	"time"

	ocsf "github.com/Santiago-Labs/go-ocsf/ocsf/v1_5_0"
)

type CloudtrailEvent struct {
	EventVersion        string              `json:"eventVersion"`
	UserIdentity        UserIdentity        `json:"userIdentity"`
	EventTime           time.Time           `json:"eventTime"`
	EventSource         string              `json:"eventSource"`
	EventName           string              `json:"eventName"`
	ErrorCode           *string             `json:"errorCode,omitempty"`
	AWSRegion           string              `json:"awsRegion"`
	SourceIPAddress     string              `json:"sourceIPAddress"`
	UserAgent           string              `json:"userAgent"`
	RequestParameters   any                 `json:"requestParameters"`
	ResponseElements    any                 `json:"responseElements"`
	AdditionalEventData AdditionalEventData `json:"additionalEventData"`
	RequestID           string              `json:"requestID"`
	EventID             string              `json:"eventID"`
	ReadOnly            bool                `json:"readOnly"`
	EventType           string              `json:"eventType"`
	ManagementEvent     bool                `json:"managementEvent"`
	RecipientAccountID  string              `json:"recipientAccountId"`
	ServiceEventDetails map[string]string   `json:"serviceEventDetails"`
	EventCategory       string              `json:"eventCategory"`
	Resources           []Resource          `json:"resources"`
}

type Resource struct {
	Arn  string `json:"arn"`
	Type string `json:"type"`
}

type UserIdentity struct {
	Type         string      `json:"type"`
	Arn          string      `json:"arn"`
	AccountID    *string     `json:"accountId,omitempty"`
	AccessKeyID  string      `json:"accessKeyId"`
	OnBehalfOf   *OnBehalfOf `json:"onBehalfOf,omitempty"`
	CredentialID string      `json:"credentialId"`
	Username     *string     `json:"userName,omitempty"`
}

type OnBehalfOf struct {
	UserID           string `json:"userId"`
	IdentityStoreArn string `json:"identityStoreArn"`
}

type AdditionalEventData struct {
	AuthWorkflowID string `json:"AuthWorkflowID"`
	UserName       string `json:"UserName"`
	CredentialType string `json:"CredentialType"`
}

func CloudtrailToOCSF(event *CloudtrailEvent) (*ocsf.APIActivity, error) {
	classUID := 6003
	categoryUID := 6
	categoryName := "Application Activity"
	className := "API Activity"

	var activityID int
	var activityName string
	var typeUID int
	var typeName string

	eventName := event.EventName
	if strings.HasPrefix(eventName, "Create") || strings.HasPrefix(eventName, "Add") ||
		strings.HasPrefix(eventName, "Put") || strings.HasPrefix(eventName, "Insert") {
		activityID = 1
		activityName = "create"
		typeUID = classUID*100 + activityID
		typeName = "API Activity: Create"
	} else if strings.HasPrefix(eventName, "Get") || strings.HasPrefix(eventName, "Describe") ||
		strings.HasPrefix(eventName, "List") || strings.HasPrefix(eventName, "Search") {
		activityID = 2
		activityName = "read"
		typeUID = classUID*100 + activityID
		typeName = "API Activity: Read"
	} else if strings.HasPrefix(eventName, "Update") || strings.HasPrefix(eventName, "Modify") ||
		strings.HasPrefix(eventName, "Set") {
		activityID = 3
		activityName = "update"
		typeUID = classUID*100 + activityID
		typeName = "API Activity: Update"
	} else if strings.HasPrefix(eventName, "Delete") || strings.HasPrefix(eventName, "Remove") {
		activityID = 4
		activityName = "delete"
		typeUID = classUID*100 + activityID
		typeName = "API Activity: Delete"
	} else {
		activityID = 0
		activityName = "unknown"
		typeUID = classUID*100 + activityID
		typeName = "API Activity: Unknown"
	}

	status := "unknown"
	statusID := 0
	severity := "informational"
	severityID := 1

	if event.ErrorCode == nil {
		status = "success"
		statusID = 1
	} else {
		status = "failure"
		statusID = 2
		severity = "medium"
		severityID = 3
	}

	var actor ocsf.Actor
	username := event.UserIdentity.Username
	if username != nil {
		actor = ocsf.Actor{
			AppName: stringPtr(event.EventSource),
			User: &ocsf.User{
				Name: username,
			},
		}
		acctID := event.UserIdentity.AccountID
		if acctID != nil {
			actor.User.Account = &ocsf.Account{
				TypeId: int32Ptr(10),
				Type:   stringPtr("AWS Account"),
				Uid:    acctID,
			}
		}
	} else {
		actor = ocsf.Actor{
			AppName: stringPtr(event.EventSource),
		}
	}

	// Parse API information
	api := ocsf.API{
		Operation: eventName,
		Service: &ocsf.Service{
			Name: stringPtr(event.EventSource),
		},
	}

	// Parse resource information
	var resources []ocsf.ResourceDetails
	for _, resource := range event.Resources {
		resources = append(resources, ocsf.ResourceDetails{
			Name: stringPtr(resource.Arn),
			Type: stringPtr(resource.Type),
			Uid:  stringPtr(resource.Arn),
		})
	}

	epochMs := event.EventTime.UnixMilli()
	srcEndpoint := ocsf.NetworkEndpoint{
		SvcName: stringPtr(event.EventSource),
	}

	// Create the OCSF API Activity
	activity := ocsf.APIActivity{
		ActivityId:   int32(activityID),
		ActivityName: &activityName,
		Actor:        actor,
		Api:          api,
		CategoryName: &categoryName,
		CategoryUid:  int32(categoryUID),
		ClassName:    &className,
		ClassUid:     int32(classUID),
		Status:       &status,
		StatusId:     int32Ptr(int32(statusID)),
		Cloud: ocsf.Cloud{
			Provider: "AWS",
			Region:   stringPtr(event.AWSRegion),
			Account: &ocsf.Account{
				TypeId: int32Ptr(10), // AWS Account
				Type:   stringPtr("AWS Account"),
				Uid:    stringPtr(event.RecipientAccountID),
			},
		},

		Resources:  resources,
		Severity:   &severity,
		SeverityId: int32(severityID),

		Metadata: ocsf.Metadata{
			CorrelationUid: stringPtr(event.EventID),
		},

		SrcEndpoint:    srcEndpoint,
		Time:           epochMs,
		TypeName:       &typeName,
		TypeUid:        int64(typeUID),
		TimezoneOffset: int32Ptr(0),
	}

	return &activity, nil
}

// Helper functions
func stringPtr(s string) *string {
	return &s
}

func int32Ptr(i int32) *int32 {
	return &i
}
