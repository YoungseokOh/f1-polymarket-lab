export default function Loading() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)] flex-col items-center justify-center gap-8">
      {/* Pulsing red glow */}
      <div className="animate-f1-glow absolute h-48 w-48 rounded-full bg-[#e10600]/40" />

      {/* Title */}
      <div className="relative flex flex-col items-center gap-3">
        <h1 className="text-4xl font-bold tracking-tight text-white">
          F1 <span className="text-[#e10600]">LAB</span>
        </h1>

        {/* Sweeping line */}
        <div className="animate-f1-sweep h-[2px] w-40 bg-gradient-to-r from-transparent via-[#e10600] to-transparent" />

        {/* Subtitle */}
        <p className="animate-f1-fade-up text-sm text-[#6b7280]">
          Loading prediction data…
        </p>
      </div>
    </div>
  );
}
