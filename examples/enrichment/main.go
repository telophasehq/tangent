package main

import (
	"encoding/json"
	"fmt"
	"net/url"

	tangent_sdk "github.com/telophasehq/tangent-sdk-go"
	"github.com/telophasehq/tangent-sdk-go/helpers"
	"github.com/telophasehq/tangent-sdk-go/http"
)

//easyjson:json
type EnrichedOutput struct {
	IPAddress string `json:"ip_address"`
	Country   string `json:"country"`
	Service   string `json:"service"`
}

var Metadata = tangent_sdk.Metadata{
	Name:    "ip-country-enrichment",
	Version: "0.2.0",
}

var selectors = []tangent_sdk.Selector{
	{
		All: []tangent_sdk.Predicate{
			tangent_sdk.EqString("service", "myservice"),
		},
	},
}

func ExampleMapper(lvs []tangent_sdk.Log) ([]EnrichedOutput, error) {
	outs := make([]EnrichedOutput, len(lvs))

	ipToIdx := make(map[string][]int)

	for i, lv := range lvs {
		if svc := helpers.GetString(lv, "service"); svc != nil {
			outs[i].Service = *svc
		}

		ipPtr := helpers.GetString(lv, "ip_address")
		if ipPtr == nil || *ipPtr == "" {
			continue
		}

		ip := *ipPtr
		outs[i].IPAddress = ip
		ipToIdx[ip] = append(ipToIdx[ip], i)
	}

	if len(ipToIdx) == 0 {
		return outs, nil
	}

	reqs := make([]http.RemoteRequest, 0, len(ipToIdx))
	for ip := range ipToIdx {
		u := "https://ipinfo.io/" + url.QueryEscape(ip)

		reqs = append(reqs, http.RemoteRequest{
			ID:     ip,
			Method: http.RemoteMethodGet,
			URL:    u,
		})
	}

	resps, err := http.RemoteCallBatch(reqs)
	if err != nil {
		return nil, fmt.Errorf("remote batch call failed: %w", err)
	}

	type ipinfoPayload struct {
		Country string `json:"country"`
	}

	ipToCountry := make(map[string]string, len(resps))

	for _, resp := range resps {
		if resp.Error != nil && *resp.Error != "" {
			return nil, fmt.Errorf("remote error for ip %s: %s", resp.ID, *resp.Error)
		}

		if resp.Status != 200 {
			return nil, fmt.Errorf("remote returned status %d for ip %s", resp.Status, resp.ID)
		}

		var payload ipinfoPayload
		if err := json.Unmarshal(resp.Body, &payload); err != nil {
			return nil, fmt.Errorf("failed to decode ipinfo response for %s: %w", resp.ID, err)
		}

		ipToCountry[resp.ID] = payload.Country
	}

	for ip, idxs := range ipToIdx {
		country := ipToCountry[ip]
		for _, i := range idxs {
			outs[i].Country = country
		}
	}

	return outs, nil
}

func init() {
	tangent_sdk.Wire[EnrichedOutput](
		Metadata,
		selectors,
		nil,           // per-log handler
		ExampleMapper, // batch handler
	)
}

func main() {}
