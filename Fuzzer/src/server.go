package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"golang.org/x/net/context"

	pb "./fuzzer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

var (
	tls      = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile = flag.String("cert_file", "", "The TLS cert file")
	keyFile  = flag.String("key_file", "", "The TLS key file")
	port     = flag.Int("port", 10000, "The server port")
)

type echoServer struct{}

func (s *echoServer) Echo(ctx context.Context, address *pb.Address) (*pb.Address, error) {
	return address, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if *tls {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterEchoServer(grpcServer, &echoServer{})
	// Register reflection service (used by grpc_cli ls ...)
	reflection.Register(grpcServer)
	grpcServer.Serve(lis)
}
