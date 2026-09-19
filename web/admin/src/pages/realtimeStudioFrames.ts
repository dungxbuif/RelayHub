type Audience = {type: string; connection_id?: string; client_id?: string};

function boundedHistoryLimit(limit: number): number {
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new TypeError("invalid history limit");
  return limit;
}

export const historyFrame = (channel: string, limit: number) => ({
  type: "history.get",
  channel,
  limit: boundedHistoryLimit(limit),
});

export const rewindSubscribeFrame = (channel: string, limit: number) => ({
  type: "subscribe",
  channels: [channel],
  rewind: {limit: boundedHistoryLimit(limit)},
});

export const batchPublishFrame = (id: string, channel: string, data: Record<string, unknown>, audience: Audience) => ({
  type: "channel.publish.batch",
  items: [{id, channel, audience, data}],
});
