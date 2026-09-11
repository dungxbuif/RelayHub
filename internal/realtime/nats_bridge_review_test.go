package realtime

import (
	"encoding/json"
	"testing"

	gonats "github.com/nats-io/nats.go"
)

func TestNATSAcceptanceSuccessSurvivesRejections(t *testing.T) {
	pending := &pendingInvocationAcceptance{invocationID: "inv_one", updates: make(chan struct{}, 1)}
	bridge := &NATSBridge{pending: map[string]*pendingInvocationAcceptance{"request": pending}}
	for _, accepted := range []bool{false, false, true, false} {
		raw, _ := json.Marshal(natsInvocationAcceptance{RequestID: "request", InvocationID: "inv_one", Accepted: accepted})
		bridge.handleAcceptance(&gonats.Msg{Data: raw})
	}
	<-pending.updates
	if !pending.accepted {
		t.Fatal("successful acceptance was lost behind a rejection")
	}
}

func TestNATSAcceptanceIgnoresAnotherInvocation(t *testing.T) {
	pending := &pendingInvocationAcceptance{invocationID: "inv_one", updates: make(chan struct{}, 1)}
	bridge := &NATSBridge{pending: map[string]*pendingInvocationAcceptance{"request": pending}}
	raw, _ := json.Marshal(natsInvocationAcceptance{RequestID: "request", InvocationID: "inv_other", Accepted: true})
	bridge.handleAcceptance(&gonats.Msg{Data: raw})
	if pending.accepted {
		t.Fatal("another invocation accepted this request")
	}
	select {
	case <-pending.updates:
		t.Fatal("another invocation woke this request")
	default:
	}
}
