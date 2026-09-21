package inspection

import (
	"strconv"
	"strings"
	"time"
)

type cronSimpleField struct {
	Any   bool
	Step  int
	Value *int
}

type cronSimpleSpec struct {
	Minute cronSimpleField
	Hour   cronSimpleField
}

func parseCronSimple5(expr string) (cronSimpleSpec, bool) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return cronSimpleSpec{}, false
	}
	for i := 2; i < 5; i++ {
		if fields[i] != "*" {
			return cronSimpleSpec{}, false
		}
	}
	minField, ok := parseCronSimpleField(fields[0], 0, 59)
	if !ok {
		return cronSimpleSpec{}, false
	}
	hourField, ok := parseCronSimpleField(fields[1], 0, 23)
	if !ok {
		return cronSimpleSpec{}, false
	}
	return cronSimpleSpec{Minute: minField, Hour: hourField}, true
}

func parseCronSimpleField(s string, min, max int) (cronSimpleField, bool) {
	if s == "*" {
		return cronSimpleField{Any: true}, true
	}
	if strings.HasPrefix(s, "*/") {
		n, err := strconv.Atoi(strings.TrimPrefix(s, "*/"))
		if err != nil || n <= 0 {
			return cronSimpleField{}, false
		}
		return cronSimpleField{Step: n}, true
	}
	if strings.ContainsAny(s, ",-/") {
		return cronSimpleField{}, false
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < min || v > max {
		return cronSimpleField{}, false
	}
	return cronSimpleField{Value: &v}, true
}

func (f cronSimpleField) match(v int) bool {
	if f.Any {
		return true
	}
	if f.Step > 0 {
		return v%f.Step == 0
	}
	if f.Value != nil {
		return v == *f.Value
	}
	return false
}

func (c cronSimpleSpec) match(t time.Time) bool {
	return c.Minute.match(t.Minute()) && c.Hour.match(t.Hour())
}

func (c cronSimpleSpec) next(after time.Time, loc *time.Location) (time.Time, bool) {
	t := after.In(loc).Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < 60*48; i++ {
		if c.match(t) {
			return t, true
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}, false
}

