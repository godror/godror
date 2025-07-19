#!/bin/bash
set -euo pipefail
if ! git worktree list | grep -F -q '[gh-pages]'; then
	git worktree add --checkout gh-page gh-pages
fi
(
cd gh-pages 
git pull -s recursive -X theirs origin gh-pages
read -r hsh tag < <(git log --format='%H %(decorate:tag=)' main | head -n1)
version="$(echo "$tag" | sed -ne '/^(v/ { s/[()]//g; p; }')"
if [[ -z "$version" ]]; then
	version="$hsh"
fi

declare -a files
while read -r fn; do
	files+=("$fn")
done < <(git show "$hsh":./doc | sed -ne '/[.]md$/ s,^,./doc/,p'; git show "$hsh":./ | grep '[.]md$')
git checkout "$version" -- "${files[@]}"
git add "${files[@]}"
if git status --porcelain | grep -q '^[ MAD]'; then
	git commit -am "Update gh-pages to $version" -n
	git push origin gh-pages
fi
)
