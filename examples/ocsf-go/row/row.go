package row

import "github.com/vmihailenco/msgpack/v5"

type Row struct {
	Raw      map[string]msgpack.RawMessage
	RawBytes []byte
}

func (r Row) Len() int          { return len(r.Raw) }
func (r Row) Has(k string) bool { _, ok := r.Raw[k]; return ok }

func (r Row) String(k string) (string, bool) {
	b, ok := r.Raw[k]
	if !ok {
		return "", false
	}
	var s string
	if err := msgpack.Unmarshal(b, &s); err != nil {
		return "", false
	}
	return s, true
}

func (r Row) Int64(k string) (int64, bool) {
	b, ok := r.Raw[k]
	if !ok {
		return 0, false
	}
	var v int64
	if err := msgpack.Unmarshal(b, &v); err != nil {
		return 0, false
	}
	return v, true
}

func (r Row) path(keys ...string) (msgpack.RawMessage, bool) {
	cur := r.Raw
	for i, k := range keys {
		val, ok := cur[k]
		if !ok {
			return nil, false
		}
		if i == len(keys)-1 {
			return val, true
		}
		var next map[string]msgpack.RawMessage
		if err := msgpack.Unmarshal(val, &next); err != nil {
			return nil, false
		}
		cur = next
	}
	return nil, false
}
func (r Row) StringAt(path ...string) (string, bool) {
	if b, ok := r.path(path...); ok {
		var s string
		if err := msgpack.Unmarshal(b, &s); err == nil {
			return s, true
		}
	}
	return "", false
}

func (r Row) ToMap() (map[string]any, error) {
	m := make(map[string]any, len(r.Raw))
	for k, b := range r.Raw {
		var v any
		if err := msgpack.Unmarshal(b, &v); err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

func (r Row) Decode(key string, dest any) (bool, error) {
	b, ok := r.Raw[key]
	if !ok {
		return false, nil
	}
	return true, msgpack.Unmarshal(b, dest)
}
