import { useEffect, useRef } from "react";
export function FeatureBoundaryPage({ title, description }: { title: string; description: string }) {
  const heading = useRef<HTMLHeadingElement>(null);
  useEffect(() => { heading.current?.focus(); }, [title]);
  return <main className="page" id="main-content"><p className="eyebrow">ADMIN MODULE</p><h1 ref={heading} tabIndex={-1}>{title}</h1><p className="page-lede">{description}</p>
    <section className="boundary-card"><span className="boundary-badge">Foundation ready</span><h2>Live data connects in the next platform phase</h2><p>This route is intentionally honest: it does not display sample metrics or invented production state.</p></section></main>;
}
