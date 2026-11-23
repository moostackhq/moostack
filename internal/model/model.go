// Package model holds the types every port speaks.
package model

import "net/netip"

// App is an app to run: its containers, or its own manifests or chart.
type App struct {
	Name       string
	Containers []Container
	// Manifests and Chart are directories.
	Manifests string
	Chart     string
	// Image is the image built for the manifests or the chart.
	Image string
	// Secrets are values, by variable.
	Secrets map[string]string
}

type Container struct {
	Name string
	// Image is the image as nodes pull it.
	Image    string
	Command  []string
	Port     int
	Host     string
	Replicas int
}

// AppState is an app as the cluster runs it.
type AppState struct {
	Name        string
	Ready, Want int
	Images      []string
	Hosts       []string
}

// Running is a running container. App is empty for the cluster's own.
type Running struct {
	App, Replica, Container, Image string
}

// Node is a node of a cluster, as the cluster directory holds it.
type Node struct {
	Name           string
	ControlPlane   bool
	PrivateAddress netip.Addr
	Interface      string
	PublicAddress  netip.Addr
	Architecture   string
	MemoryMiB      int
	GOGC           string
	GOMEMLIMIT     string
}

// NodeState is a node as the cluster has it.
type NodeState struct {
	Name, Address, Architecture, Version string
	MemoryMiB                            int
	ControlPlane                         bool
	// Ready is "yes", or why not.
	Ready string
}
