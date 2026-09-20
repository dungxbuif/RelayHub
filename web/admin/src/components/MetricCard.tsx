export function MetricCard({ label, value, detail, tone = "neutral" }: { label: string; value: string; detail: string; tone?: "neutral" | "good" | "warning" }) {
  return <article className={`metric-card metric-${tone}`}><p>{label}</p><strong>{value}</strong><span>{detail}</span></article>;
}
