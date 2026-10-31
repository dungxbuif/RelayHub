export type ResourceOption = { id: string; label: string };

export function ResourceSelector({ label, value, options, onChange, placeholder = "Select a resource" }: { label: string; value: string; options: ResourceOption[]; onChange(value: string): void; placeholder?: string }) {
  return <label>{label}<select aria-label={label} value={value} onChange={(event) => onChange(event.target.value)}>
    <option value="">{placeholder}</option>
    {options.slice(0, 100).map((option) => <option key={option.id} value={option.id}>{option.label}</option>)}
  </select></label>;
}
