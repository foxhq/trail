package trail

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneSnapshot(in Snapshot) Snapshot {
	in.Data = cloneBytes(in.Data)
	in.ViewData = cloneBytes(in.ViewData)
	in.Metadata = cloneStringMap(in.Metadata)
	return in
}

func cloneResult(in Result) Result {
	in.View = cloneBytes(in.View)
	return in
}

func cloneTransitions(in Transitions) Transitions {
	if in == nil {
		return nil
	}
	out := make(Transitions, len(in))
	for from, actions := range in {
		outActions := make(ActionTransitions, len(actions))
		for action, targets := range actions {
			outActions[action] = append([]FlowState(nil), targets...)
		}
		out[from] = outActions
	}
	return out
}
