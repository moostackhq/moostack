// Package objects makes the Kubernetes objects of an app: its namespace
// and Secret, and the objects of a deploy file's containers, manifests
// or chart.
package objects

// AppLabel is the label that marks an app's namespace and objects with
// its name.
const AppLabel = "moostack.dev/app"
