package trail

import (
	"encoding/json"
	"fmt"
)

func viewForTransition(previous []byte, transition interface{ viewValue() (any, bool) }) ([]byte, error) {
	view, set := transition.viewValue()
	if !set {
		return cloneBytes(previous), nil
	}
	if view == nil {
		return nil, nil
	}
	raw, err := json.Marshal(view)
	if err != nil {
		return nil, fmt.Errorf("%w: encode view: %v", ErrInvalidSnapshot, err)
	}
	return raw, nil
}

func viewVersionForTransition(previous int, transition interface{ viewValue() (any, bool) }) int {
	_, set := transition.viewValue()
	if !set {
		return previous
	}
	return previous + 1
}

func resultFromSnapshot(snap Snapshot) Result {
	return Result{
		ID: snap.ID, Type: snap.Type, State: snap.State, Completed: snap.Completed,
		Cancelled: snap.Cancelled, ExpiresAt: snap.ExpiresAt,
		View: json.RawMessage(cloneBytes(snap.ViewData)), Revision: snap.Revision,
	}
}

// ViewAs decodes a client-safe Result view into V. An absent or cleared view
// decodes to V's zero value.
func ViewAs[V any](result Result) (V, error) {
	var view V
	if len(result.View) == 0 || string(result.View) == "null" {
		return view, nil
	}
	if err := json.Unmarshal(result.View, &view); err != nil {
		return view, fmt.Errorf("%w: decode result view: %v", ErrInvalidSnapshot, err)
	}
	return view, nil
}
