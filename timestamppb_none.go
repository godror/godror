//go:build !timestamppb
// +build !timestamppb

// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import "time"

// pbTimestamp is a placeholder for google.golang.org/protobuf/types/known/timestamppb#Timestamp.
type pbTimestamp struct{}

func (ts *pbTimestamp) Reset() {
	panic("build with -tags=timestamppb")
}
func (ts *pbTimestamp) AsTime() time.Time {
	panic("build with -tags=timestamppb")
}
func (ts *pbTimestamp) IsValid() bool {
	panic("build with -tags=timestamppb")
}

func setPbTimestamp(ts *pbTimestamp, t time.Time) {
	panic("build with -tags=timestamppb")
}
