#!/bin/sh
set -eu
git checkout gh-pages
trap 'git checkout main' EXIT 
git pull -s recursive -X theirs origin gh-pages
version="$(git tag | sort -Vr|head -n1)"
set -x
files=$(git show "$version":./doc | sed -ne '/[.]md$/ s,^,./doc/,p'; git show "$version":./ | grep '[.]md$')
git checkout "$version" -- $files
git add $files
if git status --porcelain | grep -q '^[ MAD]'; then
	git commit -am "Update gh-pages to $version"
	git push origin gh-pages
fi
