package main

import "time"

type field struct {
	min, max int
	name     string
}

var (
	secondField  = field{0, 59, "second"}
	minuteField  = field{0, 59, "minute"}
	hourField    = field{0, 23, "hour"}
	dayField     = field{1, 31, "day"}
	monthField   = field{1, 12, "month"}
	weekdayField = field{0, 6, "weekday"}
	yearField    = field{2014, 9999, "year"}
)

type valueSpec interface {
	// wildcard determines whether the range matches all values for the given field.
	wildcard(field) bool
	// matches determines whether a given value of a given field falls within this range.
	matches(int) bool
}

type rangeSpec struct {
	// First and last values for this range.
	start, end int
	// Distance between values.
	step int
}

func (r rangeSpec) wildcard(f field) bool {
	return r.step == 1 && r.start == f.min && r.end == f.max
}

func (r rangeSpec) matches(i int) bool {
	return r.start <= i && i <= r.end && (i-r.start)%r.step == 0
}

// valid determines whether the range is valid for the specified field.
func (r rangeSpec) valid(f field) bool {
	return f.min <= r.start && r.start <= r.end && r.end <= f.max
}

type listSpec []rangeSpec

func (l listSpec) wildcard(f field) bool {
	return len(l) == 1 && l[0].wildcard(f)
}

func (l listSpec) matches(i int) bool {
	for _, r := range l {
		if r.start <= i && i <= r.end && (i-r.start)%r.step == 0 {
			return true
		}
	}
	return false
}

// Schedule is a set of constraints on the minute/hour/day/month/weekday of a date.
type Schedule struct {
	second, minute, hour, day, month, weekday, year listSpec
}

// dayMatches determines wheter the day and weekday fields match the given date.
// If either is unrestricted, both fields must match for the date to match.
// Otherwise, if either matches, the date matches.
func (s Schedule) dayMatches(t time.Time) bool {
	dayWildcard := s.day.wildcard(dayField)
	weekdayWildcard := s.weekday.wildcard(weekdayField)

	dayMatches := s.day.matches(t.Day())
	weekdayMatches := s.weekday.matches(int(t.Weekday()))
	if dayWildcard || weekdayWildcard {
		return dayMatches && weekdayMatches
	}
	return dayMatches || weekdayMatches
}

func (s Schedule) Next(begin, end int64) (int64, bool) {
	start := time.Unix(begin, 0)
	stop := time.Unix(end, 0)
	stopYear := stop.Year()

	var cur, org int
	has_reset := false

wrap:
	for start.Before(stop) {
		cur = start.Year()
		org = cur
		for !s.year.matches(cur) {
			//fmt.Println("year", cur, s.year)
			cur = cur + 1
			if cur > stopYear {
				break wrap
			}
		}
		if cur != org {
			start = time.Date(start.Year()+(cur-org), 1, 1, 0, 0, 0, 0, start.Location())
		}

		cur = int(start.Month())
		org = cur
		for !s.month.matches(cur) {
			//fmt.Println("month", cur, s.month)
			cur = cur + 1
			if cur == 13 {
				start = start.AddDate(0, cur-org, 0)
				if has_reset == false {
					start = time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, start.Location())
				}
				continue wrap
			}
		}
		if cur != org {
			start = time.Date(start.Year(), start.Month()+time.Month(cur-org), 1, 0, 0, 0, 0, start.Location())
		}

		for !s.dayMatches(start) {
			//fmt.Println("day", start)
			start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())
			start = start.AddDate(0, 0, 1)
			if start.Day() == 1 {
				continue wrap
			}
		}
		org = start.Hour()
		cur = org
		for !s.hour.matches(cur) {
			//fmt.Println("hour", cur, s.hour)
			cur = cur + 1
			if cur == 24 {
				start = start.Add(time.Duration(cur-org) * time.Hour)
				if has_reset == false {
					start = time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), 0, 0, 0, start.Location())
				}
				//fmt.Println(start)
				continue wrap
			}
		}
		if org != cur {
			start = time.Date(start.Year(), start.Month(), start.Day(), start.Hour()+(cur-org), 0, 0, 0, start.Location())
		}

		org = start.Minute()
		cur = org
		for !s.minute.matches(cur) {
			//fmt.Println("min", cur, s.minute)
			cur = cur + 1
			if cur == 60 {
				start = start.Add(time.Duration(cur-org) * time.Minute)
				if has_reset == false {
					start = time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), start.Minute(), 0, 0, start.Location())
				}
				//fmt.Println(start)
				continue wrap
			}
		}
		if org != cur {
			start = time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), start.Minute()+(cur-org), 0, 0, start.Location())
		}
		org = start.Second()
		cur = org
		for !s.second.matches(cur) {
			cur = cur + 1
			if cur == 60 {
				start = start.Add(time.Duration(cur-org) * time.Second)
				if has_reset == false {
					start = time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), start.Minute(), start.Second(), 0, start.Location())
				}
				continue wrap
			}
		}
		ret := start.Unix() + int64(cur-org)
		return ret, ret < end
	}
	return 0, false
}
