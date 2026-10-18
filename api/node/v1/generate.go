// Package nodev1 is version 1 of the node API, generated from node.proto.
package nodev1

//go:generate sh -c "cd ../../.. && protoc --plugin=protoc-gen-go=$(go tool -n protoc-gen-go) --plugin=protoc-gen-go-grpc=$(go tool -n protoc-gen-go-grpc) --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/node/v1/node.proto"
