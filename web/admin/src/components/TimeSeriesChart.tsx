import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { MetricPoint } from "../api/adminReads";

const colors = ["#2563eb", "#0d9488", "#d97706", "#dc2626"];
export function TimeSeriesChart({ title, points, fields }: { title: string; points: MetricPoint[]; fields: Array<{ key: string; label: string }> }) {
  const data = points.map((point) => ({ at: new Date(point.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }), ...point.values }));
  return <section className="chart-card" aria-labelledby={`chart-${slug(title)}`}><h2 id={`chart-${slug(title)}`}>{title}</h2>
    {data.length === 0 ? <p className="muted">No rolling data is available for this window.</p> : <div className="chart-frame"><ResponsiveContainer width="100%" height="100%"><LineChart data={data} accessibilityLayer margin={{ top: 8, right: 12, bottom: 4, left: -12 }}>
      <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" /><XAxis dataKey="at" tick={{ fontSize: 12 }} /><YAxis allowDecimals={false} tick={{ fontSize: 12 }} /><Tooltip /><Legend />
      {fields.map((field, index) => <Line key={field.key} type="monotone" dataKey={field.key} name={field.label} stroke={colors[index % colors.length]} strokeWidth={2} dot={false} isAnimationActive={false} />)}
    </LineChart></ResponsiveContainer></div>}
  </section>;
}
function slug(value: string) { return value.toLowerCase().replace(/[^a-z0-9]+/g, "-"); }
