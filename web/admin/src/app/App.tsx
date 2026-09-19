export function App() {
  return (
    <div className="app-shell">
      <header className="topbar">
        <a className="brand" href="/admin/" aria-label="RelayHub Admin home">
          <span className="brand-mark" aria-hidden="true">RH</span>
          <span>RelayHub Admin</span>
        </a>
        <span className="environment-badge">Control plane</span>
      </header>
      <main className="welcome" id="main-content">
        <p className="eyebrow">OPERATIONS, WITHOUT THE GUESSWORK</p>
        <h1>RelayHub Admin</h1>
        <p className="lede">
          The embedded control plane is ready for secure session authentication and live operational modules.
        </p>
        <section className="status-card" aria-labelledby="foundation-status">
          <div className="status-indicator" aria-hidden="true" />
          <div>
            <h2 id="foundation-status">Admin foundation</h2>
            <p>Local assets loaded. No external runtime dependencies.</p>
          </div>
        </section>
      </main>
    </div>
  );
}
