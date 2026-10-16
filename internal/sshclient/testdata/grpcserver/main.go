// Command grpcserver serves gRPC health checks on a root-only unix socket,
// for the tests that forward to one.
package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: grpcserver <socket>")
	}
	l, err := net.Listen("unix", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	if err := os.Chmod(os.Args[1], 0o600); err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	healthpb.RegisterHealthServer(s, health.NewServer())
	log.Fatal(s.Serve(l))
}
