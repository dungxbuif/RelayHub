import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRef, useState } from "react";
import { createAdminUser, disableAdminUser, listAdminUsers, rotateAdminUserPassword, updateAdminUser, type AdminUser } from "../api/controlPlane";
import { useAuth } from "../auth/AuthProvider";
import { PermissionGate } from "../auth/PermissionGate";
import { AsyncState } from "../components/AsyncState";

export function AdminUsersPage() {
  const { client } = useAuth();
  const cache = useQueryClient();
  const formRef = useRef<HTMLFormElement>(null);
  const [message, setMessage] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<AdminUser["role"]>("user");
  const users = useQuery({ queryKey: ["admin-users"], queryFn: ({ signal }) => listAdminUsers(client, signal) });
  const refresh = () => cache.invalidateQueries({ queryKey: ["admin-users"] });
  const create = useMutation({ mutationFn: () => createAdminUser(client, { email: email.trim(), password, role }), onSuccess: async () => { setPassword(""); setEmail(""); formRef.current?.reset(); setMessage("User created."); await refresh(); } });
  const update = useMutation({ mutationFn: ({ id, input }: { id: string; input: Partial<Pick<AdminUser, "role" | "enabled">> }) => updateAdminUser(client, id, input), onSuccess: refresh });
  const disable = useMutation({ mutationFn: (id: string) => disableAdminUser(client, id), onSuccess: refresh });
  const rotate = useMutation({ mutationFn: ({ id, nextPassword }: { id: string; nextPassword: string }) => rotateAdminUserPassword(client, id, nextPassword), onSuccess: async () => { setMessage("Password rotated and existing sessions revoked."); await refresh(); } });
  return <main className="page" id="main-content"><p className="eyebrow">ACCESS CONTROL</p><h1 tabIndex={-1}>Admin users</h1><p className="page-lede">Manage operator accounts. Passwords are write-only and are never returned or retained after a successful action.</p>
    <PermissionGate mutation fallback={<p className="status-card" role="status">This session is read-only. You can inspect users but cannot change access.</p>}><form ref={formRef} className="control-form" onSubmit={(event) => { event.preventDefault(); create.mutate(); }}><h2>Create user</h2><label>Email<input aria-label="Email" type="email" value={email} onChange={(event) => setEmail(event.target.value)} required /></label><label>Temporary password<input aria-label="Temporary password" type="password" value={password} onChange={(event) => setPassword(event.target.value)} required autoComplete="new-password" /></label><label>Role<select aria-label="Role" value={role} onChange={(event) => setRole(event.target.value as AdminUser["role"])}><option value="user">User (read-only)</option><option value="admin">Admin</option></select></label><button className="primary-button" disabled={create.isPending}>{create.isPending ? "Creating…" : "Create user"}</button></form></PermissionGate>
    {message && <p className="success-banner" role="status">{message}</p>}{create.error && <p className="form-error" role="alert">{create.error.message}</p>}{users.isPending && <AsyncState title="Loading users">Reading access records…</AsyncState>}{users.isError && <AsyncState title="Users unavailable" role="alert">{users.error.message}</AsyncState>}
    {users.data && <UserTable users={users.data} onRoleChange={(id, nextRole) => update.mutate({ id, input: { role: nextRole } })} onDisable={(id) => disable.mutate(id)} onRotate={(id, nextPassword) => rotate.mutate({ id, nextPassword })} />}
  </main>;
}

function UserTable({ users, onRoleChange, onDisable, onRotate }: { users: AdminUser[]; onRoleChange(id: string, role: AdminUser["role"]): void; onDisable(id: string): void; onRotate(id: string, password: string): void }) {
  const [passwords, setPasswords] = useState<Record<string, string>>({});
  return <div className="table-wrap"><table><caption>Admin users</caption><thead><tr><th scope="col">Email</th><th scope="col">Role</th><th scope="col">Status</th><th scope="col">Access actions</th></tr></thead><tbody>{users.map((user) => <tr key={user.id}><td>{user.email}</td><td><PermissionGate mutation fallback={user.role}><select aria-label={`Role for ${user.email}`} value={user.role} onChange={(event) => onRoleChange(user.id, event.target.value as AdminUser["role"])}><option value="user">User</option><option value="admin">Admin</option></select></PermissionGate></td><td>{user.enabled ? "Enabled" : "Disabled"}</td><td className="action-cell"><PermissionGate mutation fallback={null}><button className="quiet-button" type="button" disabled={!user.enabled} onClick={() => onDisable(user.id)}>Disable {user.email}</button><input aria-label={`New password for ${user.email}`} type="password" autoComplete="new-password" value={passwords[user.id] ?? ""} onChange={(event) => setPasswords((current) => ({ ...current, [user.id]: event.target.value }))} /><button className="quiet-button" type="button" disabled={!passwords[user.id]} onClick={() => { const next = passwords[user.id] ?? ""; setPasswords((current) => ({ ...current, [user.id]: "" })); onRotate(user.id, next); }}>Rotate password</button></PermissionGate></td></tr>)}</tbody></table></div>;
}
