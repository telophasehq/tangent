package main

import (
	"encoding/json"
	"fmt"
	"os"

	tangent_sdk "github.com/ethanblackburn/tangent-sdk-go"
	"github.com/ethanblackburn/tangent-sdk-go/http"
	"github.com/ethanblackburn/tangent-sdk-go/state"
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

// Flags a record whose source.name has been seen before, and posts the alert
// to Slack when SLACK_ACCESS_TOKEN is set.
//
// A name already in state is a repeat: one read. A name not in state may still
// be a repeat, of a record another worker is handling at the same moment, so
// the first sighting is decided by Incr, which is atomic: of the workers that
// get here for one name, exactly one gets 1. A Set in its place would let both
// count themselves first.
func ExampleAlert(lv tangent_sdk.Log) (Alert, error) {
	var out Alert

	serviceName := lv.GetString("source.name")
	key := "seen:" + *serviceName
	_, seen, err := state.Get(key)
	if err != nil {
		return Alert{}, err
	}
	if !seen {
		n, err := state.Incr(key, 1, nil)
		if err != nil {
			return Alert{}, err
		}
		seen = n > 1
	}

	if seen {
		accessToken := os.Getenv("SLACK_ACCESS_TOKEN")
		if accessToken == "" {
			// Without a token, as in tests, the alert is flagged but not posted.
			out.Triggered = true
			return out, nil
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

		resp, err := http.Call(http.Request{
			ID:     "slack-alert",
			Method: http.MethodPost,
			URL:    "https://slack.com/api/chat.postMessage",
			Body:   body,
			Headers: []http.Header{
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
		if resp.Error != nil {
			return Alert{}, fmt.Errorf("failed to post to slack: %s", *resp.Error)
		}

		var result struct {
			OK      bool   `json:"ok"`
			TS      string `json:"ts"`
			Error   string `json:"error,omitempty"`
			Channel string `json:"channel"`
		}
		json.Unmarshal(resp.Body, &result)

		if !result.OK {
			// With remote calls disabled, as in `tangent plugin test`, a call gets 204 and no body.
			return Alert{}, fmt.Errorf("failed to post to slack: HTTP %d: %q", resp.Status, result.Error)
		}

		out.Triggered = true
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
