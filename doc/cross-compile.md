# Cross-compilation
Go has native crosscompilation - you can 

    GOOS=windows GOARCH=386 go build -o x.exe

from a linux/amd64 host, and everything works, you get a windows/386 `x.exe`!

**But** godror has to use cgo to capitalize OCI, the Oracle supported connection library.

And cgo is not go.

So for cross-compilation, you have to have the proper cross-compiler on your host machine,
which compiles to the target architecture.
This is troublesome, but @karalabe helps us with github.com/karalabe/xgo, 
which utilizes Docker containers for this cross-compilation.

## xgo

	cd /tmp; go get github.com/karalabe/xgo

then

	xgo --targets=linux/amd64,windows/amd64,darwin/amd64 github.com/godror/godror

produces

    godror-linux-amd64
	godror-windows-4.0-amd64.exe
	godror-darwin-10.6-amd64

For details, see https://github.com/karalabe/xgo !

### Podman
`xgo` uses docker, but you can make it use podman with this little shim:

	#!/bin/sh
	# $HOME/bin/docker
	#  or anything that's on your PATH
	exec podman "$@"

and add "docker.io" to unqualified-search-registries in `/etc/containers/registries.conf`:

    unqualified-search-registries = ["docker.io"]


## zig
[Zig](https://ziglang.org) is an interesting project with an interesting tool: a clang C compiler
with a lot of target host std libs included!

For example, see [contrib/cross-compile.sh](../contrib/cross-compile.sh)!
	
