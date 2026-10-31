import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useAuth } from "../auth/AuthProvider";
import { PermissionGate } from "../auth/PermissionGate";
import {
  bindAdminPushDevice,
  deleteAdminPushDevice,
  disconnectAdminRealtimeConnection,
  downloadAdminRealtimeFile,
  getAdminRealtimeFile,
  listAdminPushDevices,
  listAdminRealtimeConnections,
  listAdminRealtimeHistory,
  listApps,
  publishAdminPushNotification,
  registerAdminPushDevice,
  type PushDevice,
  type RealtimeConnection,
} from "../api/controlPlane";
import { safeEventPayload } from "../api/adminReads";
import { AsyncState } from "../components/AsyncState";
import { ConfirmationDialog } from "../components/ConfirmationDialog";
import { JsonEditor, validateJson } from "../components/JsonEditor";
import { MutationFeedback, useAdminMutationFeedback } from "../components/MutationFeedback";

export function RealtimeResourcesPage() {
  const { client } = useAuth();
  const cache = useQueryClient();
  const [appID, setAppID] = useState("");
  const [channel, setChannel] = useState("room");
  const [fileID, setFileID] = useState("");
  const [fileInput, setFileInput] = useState("");
  const [pushToken, setPushToken] = useState("");
  const [pushProvider, setPushProvider] = useState<"fcm" | "apns">("fcm");
  const [confirmation, setConfirmation] = useState<{ kind: "connection" | "device"; connection?: RealtimeConnection; device?: PushDevice }>();
  const feedback = useAdminMutationFeedback();
  const apps = useQuery({ queryKey: ["admin-apps"], queryFn: ({ signal }) => listApps(client, signal) });
  const enabled = Boolean(appID && channel);
  const connections = useQuery({ queryKey: ["admin-realtime-resources-connections", appID], queryFn: ({ signal }) => listAdminRealtimeConnections(client, appID, signal), enabled: Boolean(appID), refetchInterval: 10_000, refetchIntervalInBackground: false });
  const history = useQuery({ queryKey: ["admin-realtime-history", appID, channel], queryFn: ({ signal }) => listAdminRealtimeHistory(client, appID, channel, "", signal), enabled, refetchInterval: false });
  const devices = useQuery({ queryKey: ["admin-push-devices", appID, channel], queryFn: ({ signal }) => listAdminPushDevices(client, appID, channel, signal), enabled, refetchInterval: false });
  const file = useQuery({ queryKey: ["admin-realtime-file", appID, fileID], queryFn: ({ signal }) => getAdminRealtimeFile(client, appID, fileID, signal), enabled: Boolean(appID && fileID) });
  const refresh = async () => { await Promise.all([cache.invalidateQueries({ queryKey: ["admin-realtime-resources-connections", appID] }), cache.invalidateQueries({ queryKey: ["admin-push-devices", appID, channel] })]); };
  const disconnect = useMutation({ mutationFn: (connectionID: string) => disconnectAdminRealtimeConnection(client, appID, connectionID), onSuccess: async () => { feedback.succeed("Connection disconnected."); setConfirmation(undefined); await refresh(); }, onError: feedback.fail });
  const register = useMutation({ mutationFn: () => registerAdminPushDevice(client, appID, { provider: pushProvider, token: pushToken }), onSuccess: async () => { setPushToken(""); feedback.succeed("Push device registered."); await refresh(); }, onError: feedback.fail });
  const deleteDevice = useMutation({ mutationFn: (deviceID: string) => deleteAdminPushDevice(client, appID, deviceID), onSuccess: async () => { feedback.succeed("Push device deleted."); setConfirmation(undefined); await refresh(); }, onError: feedback.fail });
  const bindDevice = useMutation({ mutationFn: ({ deviceID, bind }: { deviceID: string; bind: boolean }) => bindAdminPushDevice(client, appID, channel, deviceID, bind), onSuccess: async (_, variables) => { feedback.succeed(variables.bind ? "Push device bound." : "Push device unbound."); await refresh(); }, onError: feedback.fail });
  const publish = useMutation({ mutationFn: (input: { title: string; body: string; data?: Record<string, unknown> }) => publishAdminPushNotification(client, appID, channel, input), onSuccess: (result) => feedback.succeed(`${result.outcomes.length} push outcome${result.outcomes.length === 1 ? "" : "s"} recorded.`), onError: feedback.fail });
  const confirmAction = () => { if (confirmation?.kind === "connection" && confirmation.connection) disconnect.mutate(confirmation.connection.connection_id); if (confirmation?.kind === "device" && confirmation.device) deleteDevice.mutate(confirmation.device.id); };

  return <main className="page" id="main-content"><p className="eyebrow">REALTIME RESOURCES</p><h1 tabIndex={-1}>Realtime Resources</h1><p className="page-lede">Inspect app-scoped connections, bounded channel history, file metadata and push-device state. Credentials and provider tokens are never displayed.</p>
    <section className="studio-controls"><label>Application<select value={appID} onChange={(event) => setAppID(event.target.value)}><option value="">Select app</option>{apps.data?.filter((app) => app.enabled).map((app) => <option key={app.id} value={app.id}>{app.name}</option>)}</select></label><label>Channel<input value={channel} onChange={(event) => setChannel(event.target.value)} placeholder="room" /></label><button className="quiet-button" type="button" disabled={!appID} onClick={() => { void connections.refetch(); void history.refetch(); void devices.refetch(); }}>Refresh resources</button></section>
    <MutationFeedback feedback={feedback.feedback} />
    {connections.isError && <AsyncState title="Connections unavailable" role="alert">{connections.error.message}</AsyncState>}
    {appID && <section><div className="studio-log-toolbar"><h2>Active connections</h2><span>{connections.data?.connections.length ?? 0} visible</span></div><div className="table-wrap"><table><caption>Realtime connections</caption><thead><tr><th>Client</th><th>Connection</th><th>Protocol</th><th>Channels</th><th>Last seen</th><th>Action</th></tr></thead><tbody>{connections.data?.connections.map((item) => <tr key={item.connection_id}><td>{item.client_id}</td><td><code>{item.connection_id}</code></td><td>{item.protocol}</td><td>{item.channels.join(", ") || "—"}</td><td>{new Date(item.last_seen_at).toLocaleString()}</td><td><PermissionGate mutation><button className="quiet-button" type="button" onClick={() => setConfirmation({ kind: "connection", connection: item })}>Disconnect {item.connection_id}</button></PermissionGate></td></tr>)}</tbody></table></div></section>}
    {enabled && <section><h2>Channel history</h2>{history.isError && <p className="form-error" role="alert">{history.error.message}</p>}<div className="table-wrap"><table><caption>Bounded history for {channel}</caption><thead><tr><th>Message</th><th>Cursor</th><th>Data</th></tr></thead><tbody>{history.data?.items.slice(0, 100).map((item) => <tr key={`${item.message_id}:${item.cursor ?? ""}`}><td><code>{item.message_id}</code></td><td><code>{item.cursor ?? "—"}</code></td><td><code>{JSON.stringify(safeEventPayload(item.data ?? item))}</code></td></tr>)}{history.data?.items.length === 0 && <tr><td colSpan={3}>No history entries.</td></tr>}</tbody></table></div></section>}
    {enabled && <PushSection devices={devices.data?.devices ?? []} token={pushToken} setToken={setPushToken} provider={pushProvider} setProvider={setPushProvider} onRegister={() => register.mutate()} onBind={(deviceID, bind) => bindDevice.mutate({ deviceID, bind })} onDelete={(device) => setConfirmation({ kind: "device", device })} onPublish={(input) => publish.mutate(input)} pending={register.isPending || bindDevice.isPending || deleteDevice.isPending || publish.isPending} />}
    {appID && <section className="control-form"><h2>File metadata</h2><label>File ID<input value={fileInput} onChange={(event) => setFileInput(event.target.value)} placeholder="file_…" /></label><button className="quiet-button" type="button" disabled={!fileInput} onClick={() => setFileID(fileInput)}>Inspect file</button>{file.isError && <p className="form-error" role="alert">{file.error.message}</p>}{file.data && <div className="detail-grid"><div><span>Name</span><strong>{file.data.name}</strong></div><div><span>Status</span><strong>{file.data.status}</strong></div><div><span>Type</span><strong>{file.data.mime_type}</strong></div><div><span>Size</span><strong>{file.data.size_bytes} bytes</strong></div><div><span>SHA-256</span><code>{file.data.sha256}</code></div><div><PermissionGate mutation><button className="quiet-button" type="button" onClick={() => void downloadFile(client, appID, file.data!.id, file.data!.name, feedback)}>Download file</button></PermissionGate></div></div>}</section>}
    {confirmation && <ConfirmationDialog title={confirmation.kind === "connection" ? "Confirm connection disconnect" : "Confirm push-device deletion"} confirmLabel={confirmation.kind === "connection" ? "Disconnect connection" : "Delete push device"} pending={disconnect.isPending || deleteDevice.isPending} error={feedback.feedback?.kind === "error" ? feedback.feedback.message : undefined} onCancel={() => setConfirmation(undefined)} onConfirm={confirmAction}><p>This control-plane action is scoped to the selected application and is recorded in the audit log.</p><code>{confirmation.connection?.connection_id ?? confirmation.device?.id}</code></ConfirmationDialog>}
  </main>;
}

function PushSection({ devices, token, setToken, provider, setProvider, onRegister, onBind, onDelete, onPublish, pending }: { devices: PushDevice[]; token: string; setToken(value: string): void; provider: "fcm" | "apns"; setProvider(value: "fcm" | "apns"): void; onRegister(): void; onBind(deviceID: string, bind: boolean): void; onDelete(device: PushDevice): void; onPublish(input: { title: string; body: string; data?: Record<string, unknown> }): void; pending: boolean }) {
  const [title, setTitle] = useState(""); const [body, setBody] = useState(""); const [data, setData] = useState("{}");
  return <section><h2>Push devices</h2><PermissionGate mutation><form className="control-form" onSubmit={(event) => { event.preventDefault(); onRegister(); }}><label>Provider<select value={provider} onChange={(event) => setProvider(event.target.value as "fcm" | "apns")}><option value="fcm">FCM</option><option value="apns">APNs</option></select></label><label>Device token<input aria-label="Device token" value={token} onChange={(event) => setToken(event.target.value)} autoComplete="off" /></label><button className="primary-button" disabled={pending || token.length < 16}>Register device</button></form></PermissionGate><div className="table-wrap"><table><caption>Registered push devices</caption><thead><tr><th>ID</th><th>Provider</th><th>Created</th><th>Actions</th></tr></thead><tbody>{devices.map((device) => <tr key={device.id}><td><code>{device.id}</code></td><td>{device.provider}</td><td>{new Date(device.created_at).toLocaleString()}</td><td><PermissionGate mutation><button className="quiet-button" type="button" onClick={() => onBind(device.id, true)}>Bind</button><button className="quiet-button" type="button" onClick={() => onBind(device.id, false)}>Unbind</button><button className="danger-button" type="button" onClick={() => onDelete(device)}>Delete</button></PermissionGate></td></tr>)}</tbody></table></div><PermissionGate mutation><form className="control-form" onSubmit={(event) => { event.preventDefault(); const result = validateJson(data, 4_096); if (!result.valid) return; onPublish({ title, body, data: JSON.parse(data) }); }}><h3>Send notification</h3><label>Title<input value={title} onChange={(event) => setTitle(event.target.value)} required maxLength={100} /></label><label>Body<textarea value={body} onChange={(event) => setBody(event.target.value)} required maxLength={500} /></label><JsonEditor label="Notification data" value={data} onChange={setData} maxBytes={4_096} /><button className="quiet-button" type="submit" disabled={pending}>Publish notification</button></form></PermissionGate></section>;
}

async function downloadFile(client: Parameters<typeof downloadAdminRealtimeFile>[0], appID: string, fileID: string, name: string, feedback: ReturnType<typeof useAdminMutationFeedback>) { try { const blob = await downloadAdminRealtimeFile(client, appID, fileID); if (typeof URL.createObjectURL !== "function") return; const url = URL.createObjectURL(blob); const link = document.createElement("a"); link.href = url; link.download = name; link.click(); URL.revokeObjectURL(url); feedback.succeed("File download started."); } catch (error) { feedback.fail(error); } }
