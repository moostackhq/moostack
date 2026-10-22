// Package controlplanev1 is version 1 of what a node needs of a control
// plane, generated from controlplane.proto, with the join token and the
// protocol names its connections use.
package controlplanev1

//go:generate sh -c "cd ../../.. && protoc --plugin=protoc-gen-go=$(go tool -n protoc-gen-go) --plugin=protoc-gen-go-grpc=$(go tool -n protoc-gen-go-grpc) --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/controlplane/v1/controlplane.proto"
