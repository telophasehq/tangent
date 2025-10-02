package mappers

import (
	"context"
	"errors"
	"strings"
	"time"

	ocsf "github.com/Santiago-Labs/go-ocsf/ocsf/v1_5_0"
)

func CloudtrailToOCSF(ctx context.Context, event map[string]any) (*ocsf.APIActivity, error) {
	classUID := 6003
	categoryUID := 6
	categoryName := "Application Activity"
	className := "API Activity"

	var activityID int
	var activityName string
	var typeUID int
	var typeName string

	eventName := getString(event, "eventName")
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
	if event["errorCode"] == nil || event["errorCode"] == "" {
		status = "success"
		statusID = 1
	} else {
		status = "failure"
		statusID = 2
		severity = "medium"
		severityID = 3
	}

	var actor ocsf.Actor
	userIdentity := event["userIdentity"].(map[string]any)
	username := getString(userIdentity, "userName")
	eventSource := getString(event, "eventSource")

	if username != "" {
		actor = ocsf.Actor{
			AppName: stringPtr(eventSource),
			User: &ocsf.User{
				Name: stringPtr(username),
			},
		}
		acctID := getString(userIdentity, "accountId")
		if acctID != "" {
			actor.User.Account = &ocsf.Account{
				TypeId: int32Ptr(10),
				Type:   stringPtr("AWS Account"),
				Uid:    stringPtr(acctID),
			}
		}
	} else {
		actor = ocsf.Actor{
			AppName: stringPtr(eventSource),
		}
	}

	// Parse API information
	api := ocsf.API{
		Operation: eventName,
		Service: &ocsf.Service{
			Name: stringPtr(eventSource),
		},
	}

	// Parse resource information
	var resources []ocsf.ResourceDetails
	eventResources, ok := event["resources"].([]map[string]any)
	if ok {
		for _, resource := range eventResources {
			resources = append(resources, ocsf.ResourceDetails{
				Name: stringPtr(resource["arn"].(string)),
				Type: stringPtr(resource["type"].(string)),
				Uid:  stringPtr(resource["arn"].(string)),
			})
		}
	}

	eventTime, ok := event["eventTime"].(string)

	var epochMs int64
	if ok && eventTime != "" {
		if ts, err := time.Parse(time.RFC3339Nano, eventTime); err == nil {
			epochMs = ts.UnixMilli()
		}
	}

	// Parse source endpoint information
	var srcEndpoint ocsf.NetworkEndpoint
	sourceIP := getString(event, "sourceIPAddress")
	if sourceIP != "" {
		srcEndpoint = ocsf.NetworkEndpoint{
			Ip: stringPtr(sourceIP),
		}
	} else {
		srcEndpoint = ocsf.NetworkEndpoint{
			SvcName: stringPtr(eventSource),
		}
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
			Region:   stringPtr(getString(event, "awsRegion")),
			Account: &ocsf.Account{
				TypeId: int32Ptr(10), // AWS Account
				Type:   stringPtr("AWS Account"),
				Uid:    stringPtr(getString(event, "recipientAccountId")),
			},
		},

		Resources:  resources,
		Severity:   &severity,
		SeverityId: int32(severityID),

		Metadata: ocsf.Metadata{
			CorrelationUid: stringPtr(getString(event, "eventId")),
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
