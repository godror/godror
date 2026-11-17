// Copyright 2020, 2026 Tamás Gulácsi
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package analyze

import (
    "math"
    "sort"
    "time"
)

// Returns outlier indices, total drift %, and skewness [-1, 1].
func Analyze(ds []time.Duration) (outliers []int, drift, skew float64) {
    n := len(ds)
    if n < 4 { return }

    v := make([]float64, n)
    for i, d := range ds { v[i] = float64(d) }

    s := append([]float64(nil), v...)
    sort.Float64s(s)
    med := (s[(n-1)/2] + s[n/2]) / 2

    // Quartile skewness.
    q1, q3 := s[n/4], s[3*n/4]
    if q3 > q1 { skew = (q3 + q1 - 2*med) / (q3 - q1) }

    // MAD outlier detection, with a 5% minimum tolerance.
    dev := make([]float64, n)
    for i, x := range v { dev[i] = math.Abs(x - med) }
    sort.Float64s(dev)
    mad := (dev[(n-1)/2] + dev[n/2]) / 2
    limit := math.Max(3 * 1.4826 * mad, 0.05 * math.Abs(med))

    // Regression excluding outliers.
    var sx, sy, sxx, sxy, count float64
    for i, y := range v {
        if math.Abs(y-med) > limit {
            outliers = append(outliers, i)
            continue
        }
        x := float64(i)
        sx += x; sy += y
        sxx += x*x; sxy += x*y
        count++
    }

    den := count*sxx - sx*sx
    if den != 0 && med != 0 {
        slope := (count*sxy - sx*sy) / den
        drift = 100 * slope * float64(n-1) / med
    }
    return
}
