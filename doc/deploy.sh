#!/bin/sh
set -eu
git checkout gh-pages
trap EXIT 'git checkout master'
version="$(git tag | sort -Vr|head -n1)"
git checkout "$version" -- $({ git show "$version":./doc; git show "$version":./; } | grep '[.]md$')
if git status --porcelain | grep -q '^[ MAD]'; then
	git commit -am "Update gh-pages to $version"
	git push origin gh-pages
fi
