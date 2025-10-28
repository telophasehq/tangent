package tangenthelpers

import "golang/internal/tangent/logs/log"

func GetBool(v log.Logview, path string) *bool {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Boolean()
}

func GetInt64(v log.Logview, path string) *int64 {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Int()
}

func GetFloat64(v log.Logview, path string) *float64 {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Float()
}

func GetString(v log.Logview, path string) *string {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Str()
}

func GetStringList(v log.Logview, path string) ([]string, bool) {
	opt := v.GetList(path)
	if opt.None() {
		return nil, false
	}
	lst := opt.Value()
	out := make([]string, 0, lst.Len())
	data := lst.Slice()
	for i := 0; i < int(lst.Len()); i++ {
		if p := data[i].Str(); p != nil {
			out = append(out, *p)
		}
	}
	return out, true
}

func GetFloat64List(v log.Logview, path string) ([]float64, bool) {
	opt := v.GetList(path)
	if opt.None() {
		return nil, false
	}
	lst := opt.Value()
	out := make([]float64, 0, lst.Len())
	data := lst.Slice()
	for i := 0; i < int(lst.Len()); i++ {
		if p := data[i].Float(); p != nil {
			out = append(out, *p)
		}
	}
	return out, true
}

func GetInt64List(v log.Logview, path string) ([]int64, bool) {
	opt := v.GetList(path)
	if opt.None() {
		return nil, false
	}
	lst := opt.Value()
	out := make([]int64, 0, lst.Len())
	data := lst.Slice()
	for i := 0; i < int(lst.Len()); i++ {
		if p := data[i].Int(); p != nil {
			out = append(out, *p)
		}
	}
	return out, true
}

