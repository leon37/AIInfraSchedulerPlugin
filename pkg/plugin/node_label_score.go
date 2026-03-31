package plugin

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "NodeLabelScore"

type NodeLabelScore struct {
}

func (n *NodeLabelScore) Name() string {
	return Name
}

func (n *NodeLabelScore) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	return 0, nil
}

func (n *NodeLabelScore) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func NewNodeLabelScore(_ context.Context, _ runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	return &NodeLabelScore{}, nil
}
