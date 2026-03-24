import type { PropsWithChildren, ReactNode } from "react";

export function Panel(
  props: PropsWithChildren<{ title: string; eyebrow?: string }>,
) {
  return (
    <section className="group/panel relative overflow-hidden rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] p-6 shadow-xl shadow-black/30 transition-all duration-300 hover:border-white/[0.1] hover:shadow-2xl hover:shadow-black/40">
      {/* Top highlight edge */}
      <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-white/[0.08] to-transparent" />
      {/* Left accent bar with glow */}
      <div className="absolute left-0 top-0 h-full w-[3px] bg-[#e10600]" />
      <div className="absolute left-0 top-0 h-full w-[6px] bg-[#e10600]/20 blur-sm" />
      <div className="mb-4">
        {props.eyebrow ? (
          <p className="text-[10px] font-bold uppercase tracking-[0.35em] text-[#e10600]">
            {props.eyebrow}
          </p>
        ) : null}
        <h2 className="mt-1 text-lg font-semibold text-white">{props.title}</h2>
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
    <div className="group relative overflow-hidden rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] p-5 shadow-lg shadow-black/20 transition-all duration-300 hover:-translate-y-0.5 hover:border-[#e10600]/30 hover:shadow-xl hover:shadow-[#e10600]/[0.06]">
      {/* Top highlight edge */}
      <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-white/[0.06] to-transparent" />
      {/* Hover glow */}
      <div className="pointer-events-none absolute -right-8 -top-8 h-24 w-24 rounded-full bg-[#e10600]/0 blur-2xl transition-all duration-500 group-hover:bg-[#e10600]/[0.07]" />
      <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#9ca3af]">
        {props.label}
      </p>
      <p className="mt-2 text-3xl font-bold tabular-nums text-white drop-shadow-sm">
        {props.value}
      </p>
      {props.hint ? (
        <p className="mt-1.5 text-xs text-[#6b7280]">{props.hint}</p>
      ) : null}
    </div>
  );
}

export function Badge(props: {
  children: ReactNode;
  tone?: "default" | "good" | "warn" | "live";
}) {
  const toneClass =
    props.tone === "good"
      ? "border-emerald-500/20 bg-emerald-500/10 text-emerald-300 shadow-emerald-500/5"
      : props.tone === "warn"
        ? "border-amber-500/20 bg-amber-500/10 text-amber-300 shadow-amber-500/5"
        : props.tone === "live"
          ? "border-[#e10600]/30 bg-[#e10600]/10 text-[#ff4d4d] shadow-[#e10600]/5"
          : "border-white/10 bg-white/5 text-[#d1d5db] shadow-white/5";

  return (
    <span
      className={`inline-flex items-center rounded-md border px-2.5 py-0.5 text-[11px] font-semibold shadow-sm backdrop-blur-sm ${toneClass}`}
    >
      {props.children}
    </span>
  );
}
