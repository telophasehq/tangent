package mappers

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	ocsf "github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

func CloudtrailToOCSF(event []byte) (*ocsf.APIActivity, error) {
	classUID := 6003
	categoryUID := 6
	categoryName := "Application Activity"
	className := "API Activity"

	var ctEvent CloudtrailEvent
	err := json.Unmarshal(event, &ctEvent)
	if err != nil {
		return nil, err
	}

	var activityID int
	var activityName string
	var typeUID int
	var typeName string

	eventName := ctEvent.EventName

	if eventName == "" {
		return nil, errors.New("missing eventName field")
	}
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
	if ctEvent.ErrorCode == nil || *ctEvent.ErrorCode == "" {
		status = "success"
		statusID = 1
	} else {
		status = "failure"
		statusID = 2
		severity = "medium"
		severityID = 3
	}

	var actor ocsf.Actor
	userIdentity := ctEvent.UserIdentity
	username := userIdentity.UserName
	eventSource := ctEvent.EventSource

	if username != nil {
		actor = ocsf.Actor{
			AppName: &eventSource,
			User: &ocsf.User{
				Name: username,
			},
		}
		acctID := userIdentity.AccountID
		if acctID != nil {
			actor.User.Account = &ocsf.Account{
				TypeId: int32Ptr(10),
				Type:   stringPtr("AWS Account"),
				Uid:    acctID,
			}
		}
	} else {
		actor = ocsf.Actor{
			AppName: &eventSource,
		}
	}

	api := ocsf.API{
		Operation: eventName,
		Service: &ocsf.Service{
			Name: &eventSource,
		},
	}

	var resources []ocsf.ResourceDetails
	if ctEvent.Resources != nil {
		for _, resource := range ctEvent.Resources {
			resources = append(resources, ocsf.ResourceDetails{
				Name: &resource.ARN,
				Type: &resource.Type,
				Uid:  &resource.ARN,
			})
		}
	}

	var srcEndpoint ocsf.NetworkEndpoint
	if ctEvent.SourceIP != "" {
		srcEndpoint = ocsf.NetworkEndpoint{
			Ip: &ctEvent.SourceIP,
		}
	} else {
		srcEndpoint = ocsf.NetworkEndpoint{
			SvcName: &ctEvent.EventSource,
		}
	}

	var ts time.Time
	if !ctEvent.EventTime.IsZero() {
		ts = ctEvent.EventTime
	} else {
		ts = time.Now()
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
			Region:   &ctEvent.AwsRegion,
			Account: &ocsf.Account{
				TypeId: int32Ptr(10), // AWS Account
				Type:   stringPtr("AWS Account"),
				Uid:    &ctEvent.RecipientAccountID,
			},
		},

		Resources:  resources,
		Severity:   &severity,
		SeverityId: int32(severityID),

		Metadata: ocsf.Metadata{
			CorrelationUid: &ctEvent.EventID,
		},

		SrcEndpoint:    srcEndpoint,
		Time:           ts.UnixMilli(),
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
