import { useState } from "react";
import type { Credentials } from "../api/controlPlane";

export function CredentialsDialog({ credentials, onClose }: { credentials: Credentials; onClose(): void }) {
  const [acknowledged, setAcknowledged] = useState(false); const snippet = `RELAYHUB_APP_ID=${credentials.app_id}\nRELAYHUB_API_KEY=${credentials.api_key}\nRELAYHUB_HMAC_SECRET=${credentials.hmac_secret}`;
  return <div className="dialog-backdrop"><section className="confirmation-dialog credentials-dialog" role="dialog" aria-modal="true" aria-labelledby="credentials-title"><p className="eyebrow">DISPLAYED ONCE</p><h2 id="credentials-title">Store application credentials</h2><p>Copy these values into an approved secret manager. They cannot be retrieved later.</p><pre>{snippet}</pre><button className="quiet-button" type="button" onClick={() => navigator.clipboard?.writeText(snippet)}>Copy environment snippet</button><label className="acknowledgement"><input type="checkbox" checked={acknowledged} onChange={(event) => setAcknowledged(event.target.checked)} /> I stored these credentials securely.</label><button className="primary-button" type="button" disabled={!acknowledged} onClick={onClose}>Close credentials</button></section></div>;
}
