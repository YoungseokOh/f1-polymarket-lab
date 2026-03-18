import type { PropsWithChildren, ReactNode } from "react";

export function Panel(
  props: PropsWithChildren<{ title: string; eyebrow?: string }>,
) {
  return (
    <section className="rounded-3xl border border-white/10 bg-slate-950/70 p-6 shadow-2xl shadow-slate-950/40">
      <div className="mb-4">
        {props.eyebrow ? (
          <p className="text-xs uppercase tracking-[0.35em] text-cyan-300/80">
            {props.eyebrow}
          </p>
        ) : null}
        <h2 className="mt-2 text-xl font-semibold text-white">{props.title}</h2>
      </div>
      {props.children}
    </section>
  );
}

export function StatCard(props: {
  label: string;
  value: ReactNode;
  hint?: string;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
      <p className="text-xs uppercase tracking-[0.3em] text-slate-400">
        {props.label}
      </p>
      <p className="mt-3 text-3xl font-semibold text-white">{props.value}</p>
      {props.hint ? (
        <p className="mt-2 text-sm text-slate-300">{props.hint}</p>
      ) : null}
    </div>
  );
}

export function Badge(props: {
  children: ReactNode;
  tone?: "default" | "good" | "warn";
}) {
  const toneClass =
    props.tone === "good"
      ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-200"
      : props.tone === "warn"
        ? "border-amber-400/20 bg-amber-400/10 text-amber-200"
        : "border-cyan-400/20 bg-cyan-400/10 text-cyan-100";

  return (
    <span
      className={`inline-flex rounded-full border px-3 py-1 text-xs font-medium ${toneClass}`}
    >
      {props.children}
    </span>
  );
}
