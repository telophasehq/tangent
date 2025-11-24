package main

import (
	"encoding/json"
	"fmt"
	"os"

	tangent_sdk "github.com/telophasehq/tangent-sdk-go"
	"github.com/telophasehq/tangent-sdk-go/cache"
	"github.com/telophasehq/tangent-sdk-go/helpers"
	"github.com/telophasehq/tangent-sdk-go/http"
)

//easyjson:json
type Alert struct {
	Triggered bool `json:"triggered"` // This field isn't necessary, but is helpful for testing.
}

var Metadata = tangent_sdk.Metadata{
	Name:    "detection",
	Version: "0.1.0",
}

var selectors = []tangent_sdk.Selector{
	{
		All: []tangent_sdk.Predicate{
			tangent_sdk.EqString("source.name", "myservice"),
		},
	},
}

// Triggers a slack alert if source.name is seen twice.
func ExampleAlert(lv tangent_sdk.Log) (Alert, error) {
	var out Alert

	serviceName := helpers.GetString(lv, "source.name")
	seen, ok, err := cache.Get(*serviceName)
	if err != nil {
		return Alert{}, err
	}

	if ok && seen.(bool) {
		accessToken := os.Getenv("SLACK_ACCESS_TOKEN")
		if accessToken == "" {
			return Alert{}, fmt.Errorf("SLACK_ACCESS_TOKEN not set")
		}

		type slackPayload struct {
			Text    string `json:"text"`
			Channel string `json:"channel"`
		}
		body, err := json.Marshal(slackPayload{
			Text:    "Alert: duplicate source.name detected: " + *serviceName,
			Channel: "slack-app-testing",
		})
		if err != nil {
			return Alert{}, err
		}

		resp, err := http.Call(http.RemoteRequest{
			ID:     "slack-alert",
			Method: http.RemoteMethodPost,
			URL:    "https://slack.com/api/chat.postMessage",
			Body:   body,
			Headers: []http.RemoteHeader{
				{
					Name:  "Content-Type",
					Value: "application/json",
				},
				{
					Name:  "Authorization",
					Value: "Bearer " + accessToken,
				},
			},
		})

		if err != nil {
			return Alert{}, err
		}

		var result struct {
			OK      bool   `json:"ok"`
			TS      string `json:"ts"`
			Error   string `json:"error,omitempty"`
			Channel string `json:"channel"`
		}
		json.Unmarshal(resp[0].Body, &result)

		if !result.OK {
			return Alert{}, fmt.Errorf("failed to post to slack: %s", result.Error)
		}

		out.Triggered = true
	} else {
		err = cache.Set(*serviceName, true, nil)
		if err != nil {
			return Alert{}, err
		}
	}

	return out, nil
}

func init() {
	tangent_sdk.Wire[Alert](
		Metadata,
		selectors,
		ExampleAlert,
		nil,
	)
}

func main() {}
