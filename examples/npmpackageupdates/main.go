// npmpackageupdates reads the records the npm_registry source sends, one per
// package version published in the last 500 minutes:
//
//	{"kind": "npm_package_version", "npm": <the version's registry manifest>}
//
// and writes one alert per version. A version is flagged (triggered) when it
// runs a script at install time (preinstall, install or postinstall) and was
// not published from CI: no trusted publisher and no provenance attestation.
// That is how npm worms spread: a stolen token publishes a new version that
// adds an install script, from a laptop rather than the project's CI.
package main

import (
	tangent_sdk "github.com/ethanblackburn/tangent-sdk-go"
)

//easyjson:json
type Alert struct {
	Triggered     bool   `json:"triggered"`
	PackageName   string `json:"package_name"`
	Version       string `json:"version"`
	Shasum        string `json:"shasum"`
	Publisher     string `json:"publisher"`
	Provenance    bool   `json:"provenance"`
	InstallScript string `json:"install_script"`
}

var Metadata = tangent_sdk.Metadata{
	Name:    "npmpackageupdates",
	Version: "0.2.0",
}

var selectors = []tangent_sdk.Selector{
	{
		All: []tangent_sdk.Predicate{
			tangent_sdk.EqString("kind", "npm_package_version"),
		},
	},
}

// The scripts npm runs when the package is installed.
var installHooks = []string{"preinstall", "install", "postinstall"}

func str(lv tangent_sdk.Log, path string) string {
	if s := lv.GetString(path); s != nil {
		return *s
	}
	return ""
}

func Detect(lv tangent_sdk.Log) (Alert, error) {
	out := Alert{
		PackageName: str(lv, "npm.name"),
		Version:     str(lv, "npm.version"),
		Shasum:      str(lv, "npm.dist.shasum"),
		Publisher:   str(lv, "npm._npmUser.name"),
	}

	// Published from CI: npm's trusted publishing (OIDC) names the CI
	// provider, and `npm publish --provenance` attaches an attestation.
	out.Provenance = str(lv, "npm._npmUser.trustedPublisher.id") != "" ||
		str(lv, "npm.dist.attestations.url") != ""

	for _, hook := range installHooks {
		if s := str(lv, "npm.scripts."+hook); s != "" {
			out.InstallScript = hook + ": " + s
			break
		}
	}

	out.Triggered = out.InstallScript != "" && !out.Provenance
	return out, nil
}

func init() {
	tangent_sdk.Wire[Alert](
		Metadata,
		selectors,
		Detect,
		nil,
	)
}

func main() {}
