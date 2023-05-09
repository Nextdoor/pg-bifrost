/*
  Copyright 2019 Nextdoor.com, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package utils

import (
	"fmt"
	"strconv"
	"time"
)

//go:generate mockgen -destination mocks/mock_timesource.go -package=mocks github.com/Nextdoor/pg-bifrost.git/utils TimeSource
type TimeSource interface {
	UnixNano() int64
	DateString() (string, string, string, string, string)
}

type RealTime struct{}

// UnixNano gets the current time in a Unix Nanoseconds format
func (rt RealTime) UnixNano() int64 {
	return time.Now().UnixNano()
}

// intDateToNormalString normalizes single digit integers to a double digit string (by adding a leading zero if needed)
func intDateToNormalString(i int) string {
	if i < 10 {
		return fmt.Sprintf("0%d", i)
	}

	return strconv.Itoa(i)
}

// DateString return year/month/day/hour as seperate strings and also a normalized string of datetime that is 14
// characters. It is intended to be used to partition PUTs on transport sinks.
func (rt RealTime) DateString() (
	year string, month string, day string, hour string, full string) {
	now := time.Now()
	yearInt, timeMonth, dayInt := now.Date()
	monthInt := int(timeMonth)

	hourInt := now.Hour()

	fullFormat := now.Format("20060102150405")

	return strconv.Itoa(yearInt),
		intDateToNormalString(monthInt),
		intDateToNormalString(dayInt),
		intDateToNormalString(hourInt),
		fullFormat
}
