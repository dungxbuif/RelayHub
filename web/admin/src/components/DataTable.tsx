import type { ReactNode } from "react";
export type Column<T> = { key: string; label: string; render(item: T): ReactNode };
export function DataTable<T>({ caption, columns, rows, rowKey, emptyTitle = "No results", emptyDescription = "Try changing the filters or wait for new operational data." }: { caption: string; columns: Column<T>[]; rows: T[]; rowKey(item: T): string; emptyTitle?: string; emptyDescription?: string }) {
  if (rows.length === 0) return <section className="empty-card" aria-live="polite"><h2>{emptyTitle}</h2><p>{emptyDescription}</p></section>;
  return <div className="table-wrap"><table><caption>{caption}</caption><thead><tr>{columns.map((column) => <th key={column.key} scope="col">{column.label}</th>)}</tr></thead><tbody>{rows.map((row) => <tr key={rowKey(row)}>{columns.map((column) => <td key={column.key}>{column.render(row)}</td>)}</tr>)}</tbody></table></div>;
}
