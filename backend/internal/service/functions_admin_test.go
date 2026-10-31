package service

import (
	"context"
	"testing"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestFunctionAdminUpdateIsOwnerScopedAndBounded(t *testing.T) {
	repository := newFunctionMemory()
	service := NewFunctionService(repository, FunctionOptions{})
	function, err := service.Register(context.Background(), "app_1", RegisterFunction{Name: "calculate", TimeoutSeconds: 5})
	if err != nil {
		t.Fatal(err)
	}
	enabled := false
	updated, err := service.Update(context.Background(), "app_1", function.ID, UpdateFunction{Name: stringPtr("calculate-v2"), TimeoutSeconds: intPtr(10), Enabled: &enabled})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Name != "calculate-v2" || updated.TimeoutSeconds != 10 || updated.Enabled {
		t.Fatalf("updated=%+v", updated)
	}
	if _, err := service.Update(context.Background(), "other-app", function.ID, UpdateFunction{Name: stringPtr("nope")}); err != ErrNotFound {
		t.Fatalf("cross-owner update error=%v, want ErrNotFound", err)
	}
	if _, err := service.Update(context.Background(), "app_1", function.ID, UpdateFunction{TimeoutSeconds: intPtr(31)}); err != ErrInvalidInput {
		t.Fatalf("invalid timeout error=%v, want ErrInvalidInput", err)
	}
}

func stringPtr(value string) *string { return &value }
func intPtr(value int) *int          { return &value }

func (m *functionMemory) UpdateFunction(_ context.Context, owner string, function domain.Function) (domain.Function, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.functions[function.ID]
	if !ok || current.AppID != owner {
		return domain.Function{}, store.ErrNotFound
	}
	for id, old := range m.functions {
		if id != function.ID && old.AppID == owner && old.Name == function.Name {
			return domain.Function{}, store.ErrConflict
		}
	}
	m.functions[function.ID] = function
	return function, nil
}
