package main

import (
	"fmt"

	tangent_sdk "github.com/telophasehq/tangent-sdk-go"
)

//easyjson:json
type Alert struct {
	Triggered   bool   `json:"triggered"`
	PackageName string `json:"package_name"`
	Version     string `json:"version"`
}

var Metadata = tangent_sdk.Metadata{
	Name:    "npmpackageupdates",
	Version: "0.1.0",
}

var selectors = []tangent_sdk.Selector{
	{
		All: []tangent_sdk.Predicate{
			tangent_sdk.EqString("kind", "npm_package_version"),
		},
	},
}

func Detect(lv tangent_sdk.Log) (Alert, error) {
	var out Alert

	sha := lv.GetString("npm.dist.shasum")
	fmt.Println(*sha)

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
