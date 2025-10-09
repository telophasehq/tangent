package mappers

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	ocsf "github.com/telophasehq/go-ocsf/ocsf/v1_5_0"
)

type SHEvent struct {
	DetailType string `json:"detail-type"`
	Detail     struct {
		Findings []json.RawMessage `json:"findings"`
	} `json:"detail"`
}

func UnpackSHFindings(
	line []byte,
) ([]*ocsf.VulnerabilityFinding, error) {
	var ev SHEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return nil, nil
	}

	var out []*ocsf.VulnerabilityFinding
	for _, raw := range ev.Detail.Findings {
		var f types.AwsSecurityFinding
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, err
		}
		vf, err := SecurityHubToOCSF(f)
		if err != nil {
			return nil, fmt.Errorf("ToOCSF: %w", err)
		}
		out = append(out, &vf)
	}
	return out, nil
}

func SecurityHubToOCSF(securityHubFinding types.AwsSecurityFinding) (ocsf.VulnerabilityFinding, error) {
	severity, severityID := mapSecurityHubSeverity(securityHubFinding.Severity)
	status, statusID := mapSecurityHubStatus(securityHubFinding.Workflow)

	var createdAt *time.Time
	if securityHubFinding.CreatedAt != nil {
		parsedTime, err := time.Parse(time.RFC3339, *securityHubFinding.CreatedAt)
		if err == nil {
			createdAt = &parsedTime
		}
	}

	var endTime *time.Time
	if status == "Closed" {
		if securityHubFinding.UpdatedAt != nil {
			parsedTime, err := time.Parse(time.RFC3339, *securityHubFinding.UpdatedAt)
			if err == nil {
				endTime = &parsedTime
			}
		}
	}

	vendorName := "AWS"
	exploitAvailable := false

	var fixAvailable bool
	if securityHubFinding.Remediation != nil && securityHubFinding.Remediation.Recommendation != nil {
		fixAvailable = true
	}

	var remediation *ocsf.Remediation
	if securityHubFinding.Remediation != nil {
		var description string
		if securityHubFinding.Remediation.Recommendation != nil && securityHubFinding.Remediation.Recommendation.Text != nil {
			description = *securityHubFinding.Remediation.Recommendation.Text
		}

		var references []string
		if securityHubFinding.Remediation.Recommendation != nil && securityHubFinding.Remediation.Recommendation.Url != nil {
			references = append(references, *securityHubFinding.Remediation.Recommendation.Url)
		}

		remediation = &ocsf.Remediation{
			Desc:       description,
			References: references,
		}
	}

	var title string
	if securityHubFinding.Title != nil {
		title = *securityHubFinding.Title
	}

	var lastSeenTime *time.Time
	if securityHubFinding.UpdatedAt != nil {
		parsedTime, err := time.Parse(time.RFC3339, *securityHubFinding.UpdatedAt)
		if err == nil {
			lastSeenTime = &parsedTime
		}
	}

	var createdTimeInt int64
	if createdAt != nil {
		createdTimeInt = createdAt.UnixMilli()
	}

	var lastSeenTimeInt int64
	if lastSeenTime != nil {
		lastSeenTimeInt = lastSeenTime.UnixMilli()
	}

	vulnerabilities := []ocsf.VulnerabilityDetails{
		{
			Cwe:                mapSecurityHubCWE(securityHubFinding),
			Cve:                mapSecurityHubCVE(securityHubFinding),
			Desc:               securityHubFinding.Description,
			Title:              &title,
			Severity:           &severity,
			IsExploitAvailable: &exploitAvailable,
			FirstSeenTime:      createdTimeInt,
			IsFixAvailable:     &fixAvailable,
			LastSeenTime:       lastSeenTimeInt,
			VendorName:         &vendorName,
			Remediation:        remediation,
		},
	}

	var activityID int32
	var activityName string
	var typeUID int64
	var typeName string
	var eventTime time.Time
	className := "Vulnerability Finding"
	categoryUID := int32(2)
	categoryName := "Findings"
	classUID := int32(2002)

	if securityHubFinding.UpdatedAt == securityHubFinding.CreatedAt {
		activityID = int32(1)
		activityName = "Create"
		typeUID = int64(classUID)*100 + int64(activityID)
		typeName = "Vulnerability Finding: Create"
		eventTime = *createdAt
	} else if status == "Closed" {
		activityID = int32(3)
		activityName = "Close"
		typeUID = int64(classUID)*100 + int64(activityID)
		typeName = "Vulnerability Finding: Close"
		eventTime = *endTime
	} else {
		activityID = int32(2)
		activityName = "Update"
		typeUID = int64(classUID)*100 + int64(activityID)
		typeName = "Vulnerability Finding: Update"
		parsedTime, err := time.Parse(time.RFC3339, *securityHubFinding.UpdatedAt)
		if err != nil {
			return ocsf.VulnerabilityFinding{}, fmt.Errorf("failed to parse time: %w", err)
		}
		eventTime = parsedTime
	}

	productName := "SecurityHub"

	metadata := ocsf.Metadata{
		Product: ocsf.Product{
			Name:       &productName,
			VendorName: &vendorName,
		},
		Version: "1.4.0",
	}

	var modifiedTime *time.Time
	if securityHubFinding.UpdatedAt != nil {
		parsedTime, err := time.Parse(time.RFC3339, *securityHubFinding.UpdatedAt)
		if err == nil {
			modifiedTime = &parsedTime
		}
	}

	var modifiedTimeInt int64
	if modifiedTime != nil {
		modifiedTimeInt = modifiedTime.UnixMilli()
	}

	var endTimeInt int64
	if endTime != nil {
		endTimeInt = endTime.UnixMilli()
	}

	findingInfo := ocsf.FindingInformation{
		Uid:           *securityHubFinding.Id,
		Title:         securityHubFinding.Title,
		Desc:          securityHubFinding.Description,
		CreatedTime:   createdTimeInt,
		FirstSeenTime: createdTimeInt,
		LastSeenTime:  lastSeenTimeInt,
		ModifiedTime:  modifiedTimeInt,
		DataSources:   []string{"securityhub"},
		Types:         []string{"Vulnerability"},
	}

	finding := ocsf.VulnerabilityFinding{
		Time:            eventTime.UnixMilli(),
		StartTime:       createdTimeInt,
		EndTime:         endTimeInt,
		ActivityId:      activityID,
		ActivityName:    &activityName,
		CategoryUid:     categoryUID,
		CategoryName:    &categoryName,
		ClassUid:        classUID,
		ClassName:       &className,
		Message:         securityHubFinding.Description,
		Metadata:        metadata,
		Resources:       mapSecurityHubResources(securityHubFinding),
		Status:          &status,
		StatusId:        &statusID,
		TypeUid:         typeUID,
		TypeName:        &typeName,
		Vulnerabilities: vulnerabilities,
		FindingInfo:     findingInfo,
		SeverityId:      int32(severityID),
		Severity:        &severity,
	}

	return finding, nil
}

// ----------------------------------------------------------------------------
// Helper Functions
// ----------------------------------------------------------------------------

func mapSecurityHubSeverity(severity *types.Severity) (string, int) {
	if severity == nil {
		return "unknown", 0
	}

	// SeverityLabel is an enum, not a pointer
	switch severity.Label {
	case types.SeverityLabelInformational:
		return "informational", 1
	case types.SeverityLabelLow:
		return "low", 2
	case types.SeverityLabelMedium:
		return "medium", 3
	case types.SeverityLabelHigh:
		return "high", 4
	case types.SeverityLabelCritical:
		return "critical", 5
	default:
		return "unknown", 0
	}
}

func mapSecurityHubStatus(workflow *types.Workflow) (string, int32) {
	if workflow == nil {
		return "open", 1
	}

	// WorkflowStatus is an enum, not a pointer
	switch workflow.Status {
	case types.WorkflowStatusNew, types.WorkflowStatusNotified:
		return "open", 1
	case types.WorkflowStatusSuppressed:
		return "suppressed", 3
	case types.WorkflowStatusResolved:
		return "closed", 4
	default:
		return "unknown", 0
	}
}

func mapSecurityHubResources(finding types.AwsSecurityFinding) []ocsf.ResourceDetails {
	var resources []ocsf.ResourceDetails
	for _, resource := range finding.Resources {
		resourceType := *resource.Type
		if resource.Id == nil || *resource.Id == "" {
			continue
		}

		resources = append(resources, ocsf.ResourceDetails{
			Uid:  resource.Id,
			Type: &resourceType,
		})
	}

	return resources
}

func mapSecurityHubCVE(finding types.AwsSecurityFinding) *ocsf.CVE {
	if len(finding.Vulnerabilities) > 0 {
		for _, vuln := range finding.Vulnerabilities {
			if vuln.Id != nil && vuln.Cvss != nil && len(vuln.Cvss) > 0 {
				var cvss []ocsf.CVSSScore
				for _, c := range vuln.Cvss {
					if c.BaseScore != nil && c.Version != nil {
						// The field is VectorString, not Vector
						cvss = append(cvss, ocsf.CVSSScore{
							BaseScore:    *c.BaseScore,
							VectorString: c.BaseVector,
							Version:      *c.Version,
						})
					}
				}

				var references []string
				if vuln.ReferenceUrls != nil {
					references = vuln.ReferenceUrls
				}

				return &ocsf.CVE{
					Uid:        *vuln.Id,
					References: references,
					Cvss:       cvss,
				}
			}
		}
	}
	return nil
}

func mapSecurityHubCWE(finding types.AwsSecurityFinding) *ocsf.CWE {
	if finding.Types != nil {
		for _, t := range finding.Types {
			if len(t) > 4 && t[:4] == "CWE-" {
				url := "https://cwe.mitre.org/data/definitions/" + t[4:] + ".html"
				return &ocsf.CWE{
					Uid:    t,
					SrcUrl: &url,
				}
			}
		}
	}
	return nil
}
