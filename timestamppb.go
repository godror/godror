//go:build timestamppb
// +build timestamppb

// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// pbTimestamp is google.golang.org/protobuf/types/known/timestamppb#Timestamp.
type pbTimestamp = timestamppb.Timestamp

func setPbTimestamp(ts *pbTimestamp, t time.Time) {
	*ts = *timestamppb.New(t)
}
