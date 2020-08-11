#!/bin/sh
set -eu
git checkout gh-pages
trap 'git checkout master' EXIT 
version="$(git tag | sort -Vr|head -n1)"
set -x
git checkout "$version" -- $(git show "$version":./doc | sed -ne '/[.]md$/ s,^,./doc/,p'; git show "$version":./ | grep '[.]md$')
if git status --porcelain | grep -q '^[ MAD]'; then
	git commit -am "Update gh-pages to $version"
	git push origin gh-pages
fi
