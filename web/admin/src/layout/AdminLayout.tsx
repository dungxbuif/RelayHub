import { useEffect, useRef, useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import { useAuth } from "../auth/AuthProvider";
import { Navigation } from "./Navigation";

export function AdminLayout() {
  const { logout } = useAuth();
  const [open, setOpen] = useState(false);
  const location = useLocation();
  const mounted = useRef(false);
  useEffect(() => {
    if (!mounted.current) { mounted.current = true; return; }
    document.querySelector<HTMLElement>("#main-content h1")?.focus();
  }, [location.pathname]);
  return <div className="admin-shell"><a className="skip-link" href="#main-content">Skip to content</a>
    <header className="admin-topbar"><button className="menu-button" type="button" aria-expanded={open} aria-controls="admin-sidebar" onClick={() => setOpen((value) => !value)}>Menu</button>
      <a className="brand" href="/admin/" aria-label="RelayHub Admin home"><span className="brand-mark" aria-hidden="true">RH</span><span>RelayHub Admin</span></a>
      <button className="quiet-button" type="button" onClick={() => void logout()}>Sign out</button></header>
    <aside id="admin-sidebar" className={open ? "sidebar sidebar-open" : "sidebar"}><div className="sidebar-label">Workspace</div><Navigation close={() => setOpen(false)} /><a className="docs-link" href="/docs/">Official docs <span aria-hidden="true">↗</span></a></aside>
    <div className="page-area"><Outlet /></div></div>;
}
