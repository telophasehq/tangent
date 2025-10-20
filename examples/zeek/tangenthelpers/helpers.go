package tangenthelpers

import (
	"github.com/segmentio/encoding/json"

	"github.com/buger/jsonparser"
)

func Has(b []byte, keys ...string) bool {
	_, _, _, err := jsonparser.Get(b, keys...)
	return err == nil
}

func GetString(b []byte, keys ...string) (string, bool) {
	v, t, _, err := jsonparser.Get(b, keys...)
	return string(v), err == nil && t == jsonparser.String
}

func ToRaw[T any](v T) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}
