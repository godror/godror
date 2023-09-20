module github.com/godror/testdata/benchmem

go 1.19

require (
	github.com/godror/godror v0.40.1
	github.com/prometheus/procfs v0.11.1
	gonum.org/v1/gonum v0.14.0
)

require (
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/godror/knownpb v0.1.1 // indirect
	golang.org/x/exp v0.0.0-20230817173708-d852ddb80c63 // indirect
	golang.org/x/sys v0.11.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)

replace github.com/godror/godror => ../../

replace github.com/godror/godror/knownpb => ../../knownpb
