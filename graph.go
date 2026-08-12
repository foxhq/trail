package trail

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Graph describes the transition graphs for one or more workflows.
type Graph struct {
	Flows []FlowGraph `json:"flows"`
}

// FlowGraph describes one workflow's declared transition graph.
type FlowGraph struct {
	Type        FlowType    `json:"type"`
	DataVersion int         `json:"dataVersion"`
	States      []FlowState `json:"states"`
	Edges       []GraphEdge `json:"edges"`
}

// GraphEdge describes one allowed transition.
type GraphEdge struct {
	From   FlowState  `json:"from"`
	Action ActionType `json:"action"`
	To     FlowState  `json:"to"`
}

// GraphOf returns the declared transition graph for one spec.
func GraphOf[D any](spec Spec[D]) (FlowGraph, error) {
	if spec == nil || isNilSpec(spec) {
		return FlowGraph{}, fmt.Errorf("%w: spec is nil", ErrInvalidFlow)
	}

	var transitions Transitions
	if transitionSpec, ok := spec.(TransitionSpec); ok {
		transitions = transitionSpec.Transitions()
	}
	return flowGraph(spec.Type(), spec.DataVersion(), transitions), nil
}

// GraphOfRegistry returns the declared transition graphs for all specs in a
// registry.
func GraphOfRegistry(registry *SpecRegistry) (Graph, error) {
	if registry == nil {
		return Graph{}, fmt.Errorf("%w: registry is nil", ErrInvalidFlow)
	}

	registry.mu.RLock()
	defer registry.mu.RUnlock()

	flows := make([]FlowGraph, 0, len(registry.specs))
	for _, spec := range registry.specs {
		flows = append(flows, flowGraph(spec.flowType(), spec.dataVersion(), spec.transitions()))
	}
	sort.Slice(flows, func(i, j int) bool {
		return flows[i].Type < flows[j].Type
	})

	return Graph{Flows: flows}, nil
}

func flowGraph(flowType FlowType, dataVersion int, transitions Transitions) FlowGraph {
	stateSet := make(map[FlowState]struct{})
	edges := make([]GraphEdge, 0)

	for from, actions := range transitions {
		if from != BeginState {
			stateSet[from] = struct{}{}
		}
		for action, targets := range actions {
			for _, to := range targets {
				if to != BeginState {
					stateSet[to] = struct{}{}
				}
				edges = append(edges, GraphEdge{
					From:   from,
					Action: action,
					To:     to,
				})
			}
		}
	}

	states := make([]FlowState, 0, len(stateSet))
	for state := range stateSet {
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i] < states[j]
	})
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From != edges[j].From {
			return edges[i].From < edges[j].From
		}
		if edges[i].Action != edges[j].Action {
			return edges[i].Action < edges[j].Action
		}
		return edges[i].To < edges[j].To
	})

	return FlowGraph{
		Type:        flowType,
		DataVersion: dataVersion,
		States:      states,
		Edges:       edges,
	}
}

// Mermaid renders workflow graphs as a Mermaid state diagram.
func Mermaid(graph Graph) string {
	var b strings.Builder
	b.WriteString("stateDiagram-v2\n")

	if len(graph.Flows) == 1 {
		writeMermaidFlow(&b, graph.Flows[0], "    ", "")
		return b.String()
	}

	for _, flow := range graph.Flows {
		flowID := mermaidID("flow_" + string(flow.Type))
		b.WriteString(fmt.Sprintf("    state %q as %s {\n", string(flow.Type), flowID))
		writeMermaidFlow(&b, flow, "        ", string(flow.Type)+"_")
		b.WriteString("    }\n")
	}
	return b.String()
}

func writeMermaidFlow(b *strings.Builder, flow FlowGraph, indent string, prefix string) {
	for _, state := range flow.States {
		b.WriteString(fmt.Sprintf("%sstate %q as %s\n", indent, string(state), mermaidID(prefix+string(state))))
	}
	for _, edge := range flow.Edges {
		from := mermaidID(prefix + string(edge.From))
		if edge.From == BeginState {
			from = "[*]"
		}
		to := mermaidID(prefix + string(edge.To))

		b.WriteString(fmt.Sprintf("%s%s --> %s", indent, from, to))
		if edge.Action != "" && edge.Action != BeginAction {
			b.WriteString(fmt.Sprintf(": %s", edge.Action))
		}
		b.WriteString("\n")
	}
}

var nonMermaidID = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func mermaidID(value string) string {
	value = nonMermaidID.ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return "state"
	}
	if value[0] >= '0' && value[0] <= '9' {
		return "state_" + value
	}
	return value
}
