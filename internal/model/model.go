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
	// Images are the images built for the manifests or the chart, by
	// the name each is built under.
	Images map[string]string
	// Secrets are the app's secrets' values, by name.
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
	// Env are the container's environment values, by variable.
	Env map[string]Value
	// Volumes are the directories of the container that outlive its
	// replicas.
	Volumes []Volume
	// PreStart runs before each replica starts, with the container's
	// image, Env and Volumes; nil if nothing does.
	PreStart *Command
	// Cron are the commands that run on a schedule, each with the
	// container's image, Env and Volumes.
	Cron []Cron
	// Healthcheck is what must answer before a replica takes requests;
	// nil if its Port accepting a connection is enough.
	Healthcheck *Healthcheck
	// Rolling says a container with Volumes is replaced as one without
	// is: its new replica starts before the old one stops.
	Rolling bool
	// StopDelay is the seconds a replica waits before it is stopped.
	StopDelay int
}

// Volume is a directory, at Path in its container, that outlives the
// container's replicas.
type Volume struct {
	Name, Path string
}

// Command is a command to run, as User if one is given.
type Command struct {
	Command []string
	User    *User
}

// User is a user's ID, and a group's if one is given.
type User struct {
	ID    int64
	Group *int64
}

// Cron is a command that runs on Schedule, a crontab expression of five
// fields, and not while its last run is going.
type Cron struct {
	Name     string
	Schedule string
	Command
}

// Healthcheck is a check of a replica: a GET of Path on its container's
// Port, or Command run in it. One is set.
type Healthcheck struct {
	Path    string
	Command []string
}

// Value is an environment value: Value, or the one of the app's
// Secrets called Secret.
type Value struct {
	Value  string
	Secret string
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
	App, Instance, Container, Image string
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
