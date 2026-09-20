import { getDashboard } from "../api/adminReads";
import { useAuth } from "../auth/AuthProvider";
import { AsyncState } from "../components/AsyncState";
import { MetricCard } from "../components/MetricCard";
import { TimeSeriesChart } from "../components/TimeSeriesChart";
import { usePausableRefresh } from "../hooks/usePausableRefresh";

export function OverviewPage() {
  const { client } = useAuth();
  const dashboard = usePausableRefresh(["admin-dashboard", "15m", "1m"], (signal) => getDashboard(client, signal));
  return <main className="page" id="main-content"><p className="eyebrow">LIVE OPERATIONS</p><div className="page-heading"><div><h1 tabIndex={-1}>Overview</h1><p className="page-lede">Cluster rolling traffic and durable delivery state, refreshed every five seconds.</p></div><button className="quiet-button" type="button" onClick={dashboard.togglePaused}>{dashboard.paused ? "Resume live refresh" : "Pause live refresh"}</button></div>
    {dashboard.isPending && <AsyncState title="Loading dashboard">Reading current cluster state…</AsyncState>}
    {dashboard.isError && <AsyncState title="Dashboard unavailable" role="alert">{dashboard.error.message}</AsyncState>}
    {dashboard.data && <DashboardContent data={dashboard.data} paused={dashboard.paused} />}
  </main>;
}

function DashboardContent({ data, paused }: { data: Awaited<ReturnType<typeof getDashboard>>; paused: boolean }) {
  const totals = data.series.reduce((sum, point) => ({ requests: sum.requests + (point.values.request_total ?? 0), errors: sum.errors + (point.values.status_4xx ?? 0) + (point.values.status_5xx ?? 0) }), { requests: 0, errors: 0 });
  const rate = data.window_seconds > 0 ? totals.requests / data.window_seconds : 0;
  const errorRate = totals.requests > 0 ? totals.errors / totals.requests * 100 : 0;
  const healthyNATS = data.instances.filter((instance) => instance.nats_connected).length;
  return <><p className="freshness" role="status">{paused ? "Live refresh paused" : `Updated ${new Date(data.generated_at).toLocaleTimeString()}`}</p>
    {data.degraded_components.length > 0 && <AsyncState title="Partial data" role="alert">Unavailable: {data.degraded_components.join(", ")}.</AsyncState>}
    <section className="metric-grid" aria-label="Dashboard metrics">
      <MetricCard label="Request rate" value={`${rate.toFixed(2)}/s`} detail="15 minute rolling window" />
      <MetricCard label="HTTP error rate" value={`${errorRate.toFixed(1)}%`} detail="4xx and 5xx responses" tone={errorRate > 5 ? "warning" : "good"} />
      <MetricCard label="Active WebSockets" value={String(data.active_connections)} detail={`${data.instances.length} live API replicas`} />
      <MetricCard label="NATS connected" value={`${healthyNATS}/${data.instances.length}`} detail="Live API instance state" tone={healthyNATS === data.instances.length && healthyNATS > 0 ? "good" : "warning"} />
      <MetricCard label="Pending / retrying" value={`${data.durable.pending} / ${data.durable.retrying}`} detail="PostgreSQL durable truth" />
      <MetricCard label="Dead letters" value={String(data.durable.dead_letter)} detail={oldestDetail(data.durable.oldest_pending_at)} tone={data.durable.dead_letter > 0 ? "warning" : "good"} />
    </section>
    <div className="chart-grid">
      <TimeSeriesChart title="Request rate" points={data.series} fields={[{ key: "request_total", label: "Requests" }]} />
      <TimeSeriesChart title="HTTP status breakdown" points={data.series} fields={[{ key: "status_2xx", label: "2xx" }, { key: "status_3xx", label: "3xx" }, { key: "status_4xx", label: "4xx" }, { key: "status_5xx", label: "5xx" }]} />
      <TimeSeriesChart title="Event outcomes" points={data.series} fields={[{ key: "event_published", label: "Published" }, { key: "event_replayed", label: "Replayed" }, { key: "event_rejected", label: "Rejected" }, { key: "event_store_error", label: "Store error" }]} />
      <TimeSeriesChart title="Delivery outcomes" points={data.series} fields={[{ key: "delivery_delivered", label: "Delivered" }, { key: "delivery_pending", label: "Pending" }, { key: "delivery_dead_letter", label: "Dead letter" }]} />
      <TimeSeriesChart title="NATS state changes" points={data.series} fields={[{ key: "nats_disconnected", label: "Disconnected" }, { key: "nats_reconnected", label: "Reconnected" }]} />
      <section className="chart-card"><h2>Delivery latency</h2>{data.durable.delivery_latency_p50_ms === undefined
        ? <p className="muted">No completed durable deliveries are available yet.</p>
        : <div className="latency-grid" aria-label="Persisted delivery latency percentiles"><MetricCard label="p50" value={formatLatency(data.durable.delivery_latency_p50_ms)} detail="Median" /><MetricCard label="p95" value={formatLatency(data.durable.delivery_latency_p95_ms)} detail="95th percentile" /><MetricCard label="p99" value={formatLatency(data.durable.delivery_latency_p99_ms)} detail="99th percentile" /></div>}</section>
    </div>
  </>;
}

function formatLatency(value?: number) {
  if (value === undefined) return "—";
  return value >= 1_000 ? `${(value / 1_000).toFixed(2)}s` : `${Math.round(value)}ms`;
}

function oldestDetail(value?: string) {
  if (!value) return "No pending delivery age";
  return `Oldest pending ${Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1000))}s`;
}
