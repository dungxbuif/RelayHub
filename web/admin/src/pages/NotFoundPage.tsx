import { Link } from "react-router-dom";
export function NotFoundPage() { return <main className="page" id="main-content"><p className="eyebrow">404</p><h1 tabIndex={-1}>Page not found</h1><p><Link to="/">Return to Overview</Link></p></main>; }
