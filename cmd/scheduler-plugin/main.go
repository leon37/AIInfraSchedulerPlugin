package main

import (
	"github.com/leon37/AIInfraSchedulerPlugin/pkg/plugin"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

func main() {
	r := make(runtime.Registry)
	r.Register(plugin.Name, plugin.NewNodeLabelScore)
}
