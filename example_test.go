package trail_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/foxhq/trail"
)

const verifyAddress trail.FlowType = "verify_address"

type verifyAddressData struct {
	UserID      string    `json:"userId"`
	AddressID   string    `json:"addressId"`
	Code        string    `json:"code"`
	ResendAfter time.Time `json:"resendAfter"`
}

type verifyAddressInput struct {
	AddressID string
	Code      string
}

type verifyCode struct {
	Code string
}

func (verifyCode) Type() trail.ActionType { return "verify_code" }

func newVerifyAddressFlow() *trail.Definition[verifyAddressData, verifyAddressInput] {
	spec := trail.Define(
		verifyAddress,
		func(_ context.Context, begin trail.BeginContext, in verifyAddressInput) (*trail.Transition[verifyAddressData], error) {
			data := verifyAddressData{
				UserID:      string(begin.SubjectID),
				AddressID:   in.AddressID,
				Code:        in.Code,
				ResendAfter: begin.Now.Add(time.Minute),
			}
			return trail.To("code_required", data).WithView(map[string]any{
				"state": "code_required",
			}), nil
		},
	)

	spec.Start().MustGoTo("code_required")
	spec.When("code_required",
		func(_ context.Context, data verifyAddressData, action verifyCode) (*trail.Transition[verifyAddressData], error) {
			if action.Code != data.Code {
				return nil, errors.New("invalid code")
			}
			return trail.Done("verified", data).
				WithView(map[string]any{
					"state": "verified",
				}), nil
		},
	).MustGoTo("verified")
	return spec
}

func Example() {
	ctx := context.Background()

	registry := trail.NewRegistry()
	_ = trail.Register(registry, newVerifyAddressFlow())

	engine := trail.NewEngine(trail.NewMemoryStore(), registry)

	started, _ := engine.Begin(ctx, trail.BeginRequest{
		Type:      verifyAddress,
		SubjectID: "user_123",
		Input: verifyAddressInput{
			AddressID: "addr_123",
			Code:      "123456",
		},
	})

	done, _ := engine.Submit(ctx, trail.SubmitRequest{
		FlowID: started.ID,
		Action: verifyCode{Code: "123456"},
	})

	fmt.Println(started.State)
	fmt.Println(done.State)
	fmt.Println(done.Completed)
	// Output:
	// code_required
	// verified
	// true
}
