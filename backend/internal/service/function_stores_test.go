package service

import (
	"context"
	"errors"
	"testing"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type catalogOnly struct{ functions map[string]domain.Function }

func (catalog *catalogOnly) CreateFunction(_ context.Context, function domain.Function) error {
	if catalog.functions == nil {
		catalog.functions = map[string]domain.Function{}
	}
	catalog.functions[function.ID] = function
	return nil
}
func (catalog *catalogOnly) GetFunction(_ context.Context, id string) (domain.Function, error) {
	function, ok := catalog.functions[id]
	if !ok {
		return domain.Function{}, store.ErrNotFound
	}
	return function, nil
}
func (catalog *catalogOnly) ListFunctions(_ context.Context, owner string) ([]domain.Function, error) {
	items := []domain.Function{}
	for _, function := range catalog.functions {
		if function.AppID == owner {
			items = append(items, function)
		}
	}
	return items, nil
}
func (catalog *catalogOnly) DeleteFunction(_ context.Context, owner, id string) error {
	function, ok := catalog.functions[id]
	if !ok || function.AppID != owner {
		return store.ErrNotFound
	}
	delete(catalog.functions, id)
	return nil
}

func TestFunctionCatalogOperatesWithoutInvocationStore(t *testing.T) {
	service := NewFunctionServiceStores(&catalogOnly{}, nil, FunctionOptions{})
	function, err := service.Register(context.Background(), "app_owner", RegisterFunction{Name: "calculate", TimeoutSeconds: 5})
	if err != nil {
		t.Fatal(err)
	}
	items, err := service.List(context.Background(), "app_owner")
	if err != nil || len(items) != 1 || items[0].ID != function.ID {
		t.Fatalf("List() = %#v, %v", items, err)
	}
	if _, _, err := service.Invoke(context.Background(), "app_caller", function.ID, "key", []byte(`{}`)); !errors.Is(err, ErrFunctionUnavailable) {
		t.Fatalf("Invoke() error = %v", err)
	}
}
