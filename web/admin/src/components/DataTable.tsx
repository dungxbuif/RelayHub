import type { ReactNode } from "react";
export type Column<T> = { key: string; label: string; render(item: T): ReactNode };
export function DataTable<T>({ caption, columns, rows, rowKey }: { caption: string; columns: Column<T>[]; rows: T[]; rowKey(item: T): string }) {
  if (rows.length === 0) return <section className="empty-card"><h2>No results</h2><p>Try changing the filters or wait for new operational data.</p></section>;
  return <div className="table-wrap"><table><caption>{caption}</caption><thead><tr>{columns.map((column) => <th key={column.key} scope="col">{column.label}</th>)}</tr></thead><tbody>{rows.map((row) => <tr key={rowKey(row)}>{columns.map((column) => <td key={column.key}>{column.render(row)}</td>)}</tr>)}</tbody></table></div>;
}
