package trail_test

import (
	"testing"

	"github.com/foxhq/trail"
	"github.com/foxhq/trail/storetest"
)

func TestMemoryStoreContract(t *testing.T) {
	storetest.Contract(t, func(testing.TB) trail.Store {
		return trail.NewMemoryStore()
	})
}
