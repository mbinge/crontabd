package main

import (
	"fmt"
	"strconv"
	"strings"
)

var monthSubstitutions = map[string]int{
	"jan": 1,
	"feb": 2,
	"mar": 3,
	"apr": 4,
	"may": 5,
	"jun": 6,
	"jul": 7,
	"aug": 8,
	"sep": 9,
	"oct": 10,
	"nov": 11,
	"dec": 12,
}

var weekdaySubstitutions = map[string]int{
	"sun": 0,
	"mon": 1,
	"tue": 2,
	"wed": 3,
	"thu": 4,
	"fri": 5,
	"sat": 6,
	"7":   0,
}

func parseRangeSpec(s string, field field, substitutions map[string]int) (rangeSpec, error) {
	var start, end, step int

	slashParts := strings.SplitN(s, "/", 2)
	if len(slashParts) == 2 {
		parsedStep, err := strconv.Atoi(slashParts[1])
		if err != nil {
			return rangeSpec{}, fmt.Errorf("invalid range (can't parse part after slash): %s", err)
		}
		step = parsedStep
	} else {
		step = 1
	}

	if slashParts[0] == "*" || slashParts[0] == "?" {
		start = field.min
		end = field.max
	} else {
		dashParts := strings.SplitN(slashParts[0], "-", 2)
		if substitution, ok := substitutions[dashParts[0]]; ok {
			start = substitution
		} else {
			parsedStart, err := strconv.Atoi(dashParts[0])
			if err != nil {
				return rangeSpec{}, fmt.Errorf("invalid range (can't parse start value): %s", err)
			}
			start = parsedStart
		}

		if len(dashParts) > 1 {
			if substitution, ok := substitutions[dashParts[1]]; ok {
				end = substitution
			} else {
				parsedEnd, err := strconv.Atoi(dashParts[1])
				if err != nil {
					return rangeSpec{}, fmt.Errorf("invalid range (can't parse end value): %s", err)
				}
				end = parsedEnd
			}
		} else {
			end = start
		}
	}

	r := rangeSpec{start, end, step}

	if !r.valid(field) {
		return rangeSpec{}, fmt.Errorf("%s must be between %d and %d", field.name, field.min, field.max)
	}

	return r, nil
}

func parseListSpec(s string, field field, substitutions map[string]int) (listSpec, error) {
	var rangeSpecs listSpec
	for _, rangeString := range strings.Split(s, ",") {
		rangeSpec, err := parseRangeSpec(rangeString, field, substitutions)
		if err != nil {
			return nil, err
		}
		rangeSpecs = append(rangeSpecs, rangeSpec)
	}
	return rangeSpecs, nil
}

// ParseSchedule parses a 5-element array containing the first 5 colunms of a crontab line.
func ParseSchedule(fields []string) (s Schedule, err error) {
	if len(fields) != 7 {
		err = fmt.Errorf("wrong number of fields; expected 5")
		return
	}
	var second, minute, hour, day, month, weekday, year listSpec

	second, err = parseListSpec(fields[0], minuteField, nil)
	if err != nil {
		return
	}
	minute, err = parseListSpec(fields[1], minuteField, nil)
	if err != nil {
		return
	}
	hour, err = parseListSpec(fields[2], hourField, nil)
	if err != nil {
		return
	}
	day, err = parseListSpec(fields[3], dayField, nil)
	if err != nil {
		return
	}
	month, err = parseListSpec(fields[4], monthField, monthSubstitutions)
	if err != nil {
		return
	}
	weekday, err = parseListSpec(fields[5], weekdayField, weekdaySubstitutions)
	if err != nil {
		return
	}
	year, err = parseListSpec(fields[6], yearField, nil)
	if err != nil {
		return
	}

	s.second = second
	s.minute = minute
	s.hour = hour
	s.day = day
	s.month = month
	s.weekday = weekday
	s.year = year
	return
}
