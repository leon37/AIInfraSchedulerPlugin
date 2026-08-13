package main

import (
	"os"

	"github.com/leon37/AIInfraSchedulerPlugin/pkg/plugin"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	command := app.NewSchedulerCommand(app.WithPlugin(plugin.TracerPluginName, plugin.New))
	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
