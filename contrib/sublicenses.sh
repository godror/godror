#!/bin/sh
set -eu
sed -e '/^END OF TERMS AND CONDITIONS$/,$d' LICENSE.md
echo 'END OF TERMS AND CONDITIONS'
echo ''
go mod tidy
go mod download -json $(awk '/	/{print $1}' go.mod) | jq -r .Zip | while read -r z; do
	echo ''
	echo "============================================================";
	echo "$z" | sed -e 's,^.*/cache/download/,,;s,/@v/, ,;s/[.]zip$//';
	echo "============================================================";
	unzip -p "$z" "$(unzip -l "$z" | awk '/LICENSE/{print $4}' | head -n1)";
done
