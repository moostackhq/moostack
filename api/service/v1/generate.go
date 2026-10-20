// Package servicev1 is what every cluster service's API has in common,
// generated from service.proto, and how the agent tells a service who
// called.
package servicev1

//go:generate sh -c "cd ../../.. && protoc --plugin=protoc-gen-go=$(go tool -n protoc-gen-go) --plugin=protoc-gen-go-grpc=$(go tool -n protoc-gen-go-grpc) --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/service/v1/service.proto"
