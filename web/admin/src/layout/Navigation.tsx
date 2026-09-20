import { NavLink } from "react-router-dom";

export const navigationItems = [["Overview", ""], ["Events", "events"], ["Dead Letters", "dead-letters"], ["Queue v2", "queue"], ["Apps", "apps"], ["Routing Rules", "routing-rules"], ["Realtime Studio", "realtime-studio"], ["Audit Logs", "audit-logs"], ["System", "system"]] as const;

export function Navigation({ close }: { close(): void }) {
  return <nav className="navigation" aria-label="Admin sections">{navigationItems.map(([label, path]) => (
    <NavLink key={label} to={path} end={path === ""} onClick={close}><span aria-hidden="true" className="nav-dot" />{label}</NavLink>
  ))}</nav>;
}
