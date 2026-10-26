// Command fixtureservice is a cluster service for nodetest's tests only:
// it serves the API whose gRPC service name -api gives, as another
// service does, but lets every caller kind call each of its methods,
// Ping too, so that a test can tell which of the two the agent asked
// what each method allows. Its Echo and Ping answer with an empty
// message, and its Stream answers each message of the call with an
// empty one. It prints "serving <API>" once it listens. -health-delay
// has Health take that long to answer. Without -api, it is an app,
// which serves nothing, and with -exit <code> it exits at once, or with
// -exit-after too, once that long has passed.
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	servicev1 "github.com/moostackhq/moostack/api/service/v1"
)

func main() {
	api := flag.String("api", "", "the gRPC service name of the API to serve, none for an app")
	exit := flag.Int("exit", -1, "the code to exit with, if any")
	exitAfter := flag.Duration("exit-after", 0, "how long to run before it exits with -exit's code")
	flag.DurationVar(&healthDelay, "health-delay", 0, "how long Health takes to answer")
	flag.Parse()
	switch {
	case *exit >= 0:
		time.Sleep(*exitAfter)
		fmt.Println("exiting with", *exit)
		os.Exit(*exit)
	case *api == "":
		// As an app, it serves nothing: it says who it runs as and waits
		// for SIGTERM.
		fmt.Printf("running as %d:%d\n", os.Getuid(), os.Getgid())
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		<-signals
		return
	}
	if err := serve(*api); err != nil {
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

// healthDelay is how long Health takes to answer.
var healthDelay time.Duration

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
		time.Sleep(healthDelay)
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
