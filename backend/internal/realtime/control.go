package realtime

import (
	"context"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/redisstate"
)

var ErrConnectionNotFound = errors.New("realtime connection not found")

type Control struct {
	connections *redisstate.RealtimeConnectionStore
	commands    *NATSBridge
}

func NewControl(connections *redisstate.RealtimeConnectionStore, commands *NATSBridge) *Control {
	return &Control{connections: connections, commands: commands}
}

func (control *Control) List(ctx context.Context, appID string, limit int64) ([]redisstate.RealtimeConnection, error) {
	if control == nil || control.connections == nil {
		return nil, redisstate.ErrUnavailable
	}
	return control.connections.List(ctx, appID, time.Now(), limit)
}

func (control *Control) Disconnect(ctx context.Context, appID, connectionID string) error {
	if control == nil || control.connections == nil || control.commands == nil {
		return redisstate.ErrUnavailable
	}
	connection, err := control.connections.Get(ctx, appID, connectionID)
	if errors.Is(err, redisstate.ErrNotFound) {
		return ErrConnectionNotFound
	}
	if err != nil {
		return err
	}
	return control.commands.DisconnectRealtimeV2(ctx, connection.InstanceID, appID, connectionID)
}
