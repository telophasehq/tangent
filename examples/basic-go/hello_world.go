//go:generate go tool wit-bindgen-go generate --world process --out internal ./tangent:logs@0.1.0.wasm

package main

import (
	"basic-go/internal/tangent/logs/processor"
	"encoding/json"

	"go.bytecodealliance.org/cm"
)

func init() {
	processor.Exports.ProcessLogs = ProcessLogs
}

func ProcessLogs(logs string) (result cm.Result[string, struct{}, string]) {
	var v any
	if e := json.Unmarshal([]byte(logs), &v); e != nil {
		result.SetErr(e.Error())
	}
	result.SetOK(struct{}{})

	return result
}

func main() {}
