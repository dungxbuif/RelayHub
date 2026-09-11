package domain

import (
	"encoding/json"
	"testing"
)

func TestRPCHandlerErrorRejectsAmbiguousKeys(t *testing.T) {
	for _, raw := range []string{
		`{"Code":"bad","message":"failure"}`,
		`{"code":"bad","Message":"failure"}`,
		`{"code":"bad","code":"other","message":"failure"}`,
		`{"code":"bad","message":"failure","message":null}`,
		`{"code":"bad","code":null,"message":"failure"}`,
		`{"code":"bad","message":null}`,
	} {
		t.Run(raw, func(t *testing.T) {
			if ValidRPCResult(RPCResult{InvocationID: "inv_test", Error: json.RawMessage(raw)}) {
				t.Fatal("ambiguous handler error accepted")
			}
		})
	}
}
