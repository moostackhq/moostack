// Command fixtureservice is a cluster service for nodetest's tests only:
// it serves the API whose gRPC service name is its argument, as another
// service does, but lets every caller kind call each of its methods,
// Ping too, so that a test can tell which of the two the agent asked
// what each method allows. Its Echo and Ping answer with an empty
// message, and its Stream answers each message of the call with an
// empty one. It prints "serving <API>" once it listens.
package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	servicev1 "github.com/moostackhq/moostack/api/service/v1"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: fixtureservice <API>")
		os.Exit(2)
	}
	if err := serve(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// serve serves api on the agent's socket until SIGTERM or SIGINT.
func serve(api string) error {
	socket := filepath.Join(servicev1.SocketDir, servicev1.APISocket)
	os.Remove(socket)
	l, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	fmt.Println("serving", api)
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		return handle(api, stream)
	}))
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		<-signals
		server.Stop()
	}()
	return server.Serve(l)
}

// handle answers a call of one of api's methods.
func handle(api string, stream grpc.ServerStream) error {
	full, _ := grpc.MethodFromServerStream(stream)
	method, ok := strings.CutPrefix(full, "/"+api+"/")
	if !ok {
		return status.Errorf(codes.Unimplemented, "%s isn't a method of %s", full, api)
	}
	var res proto.Message
	switch method {
	case "Health":
		res = &servicev1.HealthResponse{}
	case "Describe":
		every := []servicev1.CallerKind{
			servicev1.CallerKind_CALLER_KIND_OPERATOR, servicev1.CallerKind_CALLER_KIND_CONTROL_PLANE,
			servicev1.CallerKind_CALLER_KIND_PEER_NODE, servicev1.CallerKind_CALLER_KIND_LOCAL_SERVICE,
		}
		d := &servicev1.DescribeResponse{}
		for _, name := range []string{"Health", "Describe", "Echo", "Ping", "Stream"} {
			d.Methods = append(d.Methods, &servicev1.Method{Name: name, Callers: every})
		}
		res = d
	case "Echo", "Ping":
		res = &emptypb.Empty{}
	case "Stream":
		for {
			if err := stream.RecvMsg(&emptypb.Empty{}); err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
				return err
			}
		}
	default:
		return status.Errorf(codes.Unimplemented, "%s has no method %s", api, method)
	}
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		return err
	}
	return stream.SendMsg(res)
}
