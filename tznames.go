// Copyright 2019, 2021 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"os"
	"errors"
	"strconv"
	"fmt"
	"sort"
	"time"
	"strings"
	"sync"
	_ "embed"

	"github.com/godror/godror/dsn"
	"github.com/godror/godror/slog"
)

//go:generate go run generate_tznames.go -o tznames_generated.txt
//go:embed tznames_generated.txt
var tzNamesRaw string
var (
	tzNames, tzNamesLC []string
	tzNamesOnce        sync.Once
)

func findProperTZName(dbTZ string) (*time.Location, error) {
	tzNamesOnce.Do(func() {
		lines := strings.Split(tzNamesRaw, "\n")
		tzNames = make([]string, len(lines))
		tzNamesLC = make([]string, len(tzNames))
		for i, line := range lines {
			length := len(line) / 2
			tzNames[i] = line[length:]
			tzNamesLC[i] = line[:length]
		}
	})
	tz, err := time.LoadLocation(dbTZ)
	if err != nil {
		needle := strings.ToLower(dbTZ)
		if i := sort.SearchStrings(tzNamesLC, needle); i < len(tzNames) && tzNamesLC[i] == needle {
			tz, err = time.LoadLocation(tzNames[i])
		}
	}
	if err == nil {
		if tz == nil {
			return time.UTC, nil
		}
		return tz, nil
	}
	return nil, err
}

func calculateTZ(dbTZ, dbOSTZ string, noTZCheck bool, logger *slog.Logger) (*time.Location, int, error) {
	if dbTZ == "" && dbOSTZ != "" {
		dbTZ = dbOSTZ
	} else if dbTZ != "" && dbOSTZ == "" {
		dbOSTZ = dbTZ
	}
	if dbTZ != dbOSTZ {
		atoi := func(s string) (int, error) {
			var i int
			s = strings.Map(
				func(r rune) rune {
					if r == '+' || r == '-' {
						i++
						if i == 1 {
							return r
						}
						return -1
					} else if '0' <= r && r <= '9' {
						i++
						return r
					}
					return -1
				},
				s,
			)
			if s == "" {
				return 0, errors.New("NaN")
			}
			return strconv.Atoi(s)
		}

		// Oracle DB has three time zones: SESSIONTIMEZONE, {SESSION,DB}TIMEZONE, and OS time zone (SYSDATE, SYSTIMESTAMP): https://stackoverflow.com/a/29272926
		if !noTZCheck {
			if dbI, err := atoi(dbTZ); err == nil {
				if tzI, err := atoi(dbOSTZ); err == nil && dbI != tzI &&
					dbI+100 != tzI && tzI+100 != dbI { // Compensate for Daylight Savings
					fmt.Fprintf(os.Stderr, "godror WARNING: discrepancy between SESSIONTIMEZONE (%q=%d) and SYSTIMESTAMP (%q=%d) - set connection timezone, see https://github.com/godror/godror/blob/master/doc/timezone.md\n", dbTZ, dbI, dbOSTZ, tzI)
				}
			}
		}
	}
	if (dbTZ == "+00:00" || dbTZ == "UTC") && (dbOSTZ == "+00:00" || dbOSTZ == "UTC") {
		return time.UTC, 0, nil
	}
	if logger != nil {
		logger.Debug("calculateTZ", "dbTZ", dbTZ, "dbOSTZ", dbOSTZ)
	}

	var tz *time.Location
	var off int
	now := time.Now()
	_, localOff := now.Local().Zone()
	// If it's a name, try to use it.
	if dbTZ != "" && strings.Contains(dbTZ, "/") {
		var err error
		if tz, err = findProperTZName(dbTZ); err == nil {
			_, off = now.In(tz).Zone()
			return tz, off, nil
		}
		if logger != nil {
			logger.Error("LoadLocation", "tz", dbTZ, "error", err)
		}
	}
	// If not, use the numbers.
	var err error
	if dbOSTZ != "" {
		if off, err = dsn.ParseTZ(dbOSTZ); err != nil {
			return tz, off, fmt.Errorf("ParseTZ(%q): %w", dbOSTZ, err)
		}
	} else if off, err = dsn.ParseTZ(dbTZ); err != nil {
		return tz, off, fmt.Errorf("ParseTZ(%q): %w", dbTZ, err)
	}
	// This is dangerous, but I just cannot get whether the DB time zone
	// setting has DST or not - {SESSION,DB}TIMEZONE returns just a fixed offset.
	//
	// So if the given offset is the same as with the Local time zone,
	// then keep the local.
	//fmt.Printf("off=%d localOff=%d tz=%p\n", off, localOff, tz)
	if off == localOff {
		return time.Local, off, nil
	}
	if off == 0 {
		tz = time.UTC
	} else {
		if tz = time.FixedZone(dbOSTZ, off); tz == nil {
			tz = time.UTC
		}
	}
	return tz, off, nil
}


