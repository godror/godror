#!/bin/bash
set -euo pipefail
echo "# Run tests, print only failed or long (>10s) ones" >&2
go test -json -count=1 . | jq -r --unbuffered 'select(
      (.Test != null and (.Action == "pass" or .Action == "fail") and (.Action == "fail" or .Elapsed >= 10))
      or (.Test == null and .Action == "fail")
    )
    | if .Test == null then ["FAIL", (.Elapsed | floor), .Package, "(package)"]
      elif .Action == "fail" then ["FAIL", (.Elapsed | floor), .Package, .Test]
      else ["SLOW", (.Elapsed | floor), .Package, .Test]
      end
    | @tsv'
