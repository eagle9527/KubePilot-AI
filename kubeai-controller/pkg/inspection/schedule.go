package inspection

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type dailySchedule struct {
	MinuteAny bool
	HourAny   bool
	Minute    int
	Hour      int
}

func parseCron5(expr string) (*dailySchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("invalid cron expression: %s", expr)
	}

	s := &dailySchedule{}

	if fields[0] == "*" {
		s.MinuteAny = true
	} else {
		v, err := strconv.Atoi(fields[0])
		if err != nil || v < 0 || v > 59 {
			return nil, fmt.Errorf("invalid minute field: %s", fields[0])
		}
		s.Minute = v
	}

	if fields[1] == "*" {
		s.HourAny = true
	} else {
		v, err := strconv.Atoi(fields[1])
		if err != nil || v < 0 || v > 23 {
			return nil, fmt.Errorf("invalid hour field: %s", fields[1])
		}
		s.Hour = v
	}

	for i := 2; i < 5; i++ {
		if fields[i] != "*" {
			return nil, fmt.Errorf("unsupported cron field (only * supported for day/month/dow): %s", fields[i])
		}
	}

	return s, nil
}

func (s *dailySchedule) next(after time.Time, loc *time.Location) time.Time {
	t := after.In(loc)
	if s.HourAny && s.MinuteAny {
		n := t.Truncate(time.Minute).Add(time.Minute)
		return n
	}
	if s.HourAny && !s.MinuteAny {
		n := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), s.Minute, 0, 0, loc)
		if !n.After(t) {
			n = n.Add(time.Hour)
		}
		return n
	}
	if !s.HourAny && s.MinuteAny {
		n := time.Date(t.Year(), t.Month(), t.Day(), s.Hour, t.Minute(), 0, 0, loc)
		if !n.After(t) {
			n = n.Add(time.Minute)
			if n.Hour() != s.Hour {
				n = time.Date(t.Year(), t.Month(), t.Day(), s.Hour, 0, 0, 0, loc)
				if !n.After(t) {
					n = n.AddDate(0, 0, 1)
				}
			}
		}
		return n
	}

	n := time.Date(t.Year(), t.Month(), t.Day(), s.Hour, s.Minute, 0, 0, loc)
	if !n.After(t) {
		n = n.AddDate(0, 0, 1)
	}
	return n
}
