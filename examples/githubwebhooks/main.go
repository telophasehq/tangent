package main

import (
	"fmt"
	"strings"
	"time"

	tangent_sdk "github.com/telophasehq/tangent-sdk-go"
	"github.com/telophasehq/tangent-sdk-go/cache"
)

//easyjson:json
type Alert struct {
	Triggered bool `json:"triggered"`
}

var Metadata = tangent_sdk.Metadata{
	Name:    "githubwebhooks",
	Version: "0.1.0",
}

var selectors = []tangent_sdk.Selector{
	{
		Any: []tangent_sdk.Predicate{
			tangent_sdk.Regex("message", ".*npm publish.*"),
			tangent_sdk.Prefix("message", "npm notice name:"),
			tangent_sdk.Prefix("message", "npm notice shasum:"),
			tangent_sdk.Prefix("message", "+ "),
		},
	},
}

func DetectNPMPublish(lv tangent_sdk.Log) (Alert, error) {
	var out Alert

	msg := lv.GetString("message")
	if msg == nil {
		return out, nil
	}
	m := strings.TrimSpace(*msg)

	sha := lv.GetString("github.sha")
	if sha == nil || *sha == "" {
		return out, nil
	}

	logName := lv.GetString("github.log_file")
	if logName == nil || *logName == "" {
		return out, nil
	}

	cacheTTL := 15 * time.Minute
	startedKey := fmt.Sprintf("npm-publish-started-%s-logname", *sha)
	pkgKey := fmt.Sprintf("npm-publish-started-%s-pkgname", *sha)
	shasumKey := fmt.Sprintf("npm-publish-started-%s-npmSHA", *sha)

	// 1) Mark that this SHA started an npm publish step, and record which log file
	if strings.Contains(m, "##[group]Run") && strings.Contains(m, "npm publish") {
		if err := cache.Set(startedKey, *logName, &cacheTTL); err != nil {
			return out, err
		}
		return out, nil
	}

	// Everything below requires that we previously saw a publish step for this SHA/log
	startedLogNameVal, ok, err := cache.Get(startedKey)
	if err != nil {
		return out, err
	}
	if !ok || startedLogNameVal.(string) != *logName {
		// Either no publish step yet, or this is a different log file
		return out, nil
	}

	// 2) Capture package name from `npm notice name: foo`
	if strings.HasPrefix(m, "npm notice name:") {
		pkgName := strings.TrimSpace(strings.TrimPrefix(m, "npm notice name:"))
		if err := cache.Set(pkgKey, pkgName, &cacheTTL); err != nil {
			return out, err
		}
		return out, nil
	}

	// 3) Capture npm tarball shasum from `npm notice shasum: abc123...`
	if strings.HasPrefix(m, "npm notice shasum:") {
		npmSHA := strings.TrimSpace(strings.TrimPrefix(m, "npm notice shasum:"))
		if err := cache.Set(shasumKey, npmSHA, &cacheTTL); err != nil {
			return out, err
		}
		return out, nil
	}

	// 4) Success line: `+ package@version`
	if strings.HasPrefix(m, "+ ") {
		// m = "+ tangent-home-js@1.0.0"
		publishToken := strings.TrimSpace(strings.TrimPrefix(m, "+ "))

		successKey := fmt.Sprintf("npm-publish-success-%s", publishToken)
		if err := cache.Set(successKey, true, &cacheTTL); err != nil {
			return out, err
		}

		out.Triggered = true
		return out, nil
	}

	return out, nil
}

func init() {
	tangent_sdk.Wire[Alert](
		Metadata,
		selectors,
		DetectNPMPublish,
		nil,
	)
}

func main() {}
