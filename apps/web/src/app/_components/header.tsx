import { sdk } from "@f1/ts-sdk";
import { Badge } from "@f1/ui";

export async function Header() {
  const [health, meetings] = await Promise.all([
    sdk.health().catch(() => ({
      service: "api",
      status: "offline",
      now: new Date().toISOString(),
    })),
    sdk.meetings().catch(() => []),
  ]);

  const now = new Date();
  const sorted = [...meetings].sort((a, b) =>
    (a.startDateUtc ?? "").localeCompare(b.startDateUtc ?? ""),
  );
  const upcoming = sorted.filter(
    (m) => m.startDateUtc && new Date(m.endDateUtc ?? m.startDateUtc) >= now,
  );
  const currentGP = upcoming[0] ?? sorted.at(-1);

  return (
    <header className="hidden h-16 items-center justify-between border-b border-white/[0.06] bg-f1-dark/80 px-6 backdrop-blur-md lg:flex">
      <div className="flex items-center gap-4">
        {currentGP ? (
          <>
            <h1 className="text-sm font-semibold text-white">
              {currentGP.meetingName}
            </h1>
            {currentGP.location && (
              <span className="text-xs text-[#6b7280]">
                {currentGP.location}, {currentGP.countryName}
              </span>
            )}
          </>
        ) : (
          <h1 className="text-sm font-semibold text-white">
            F1 Polymarket Lab
          </h1>
        )}
      </div>

      <div className="flex items-center gap-3">
        <Badge tone={health.status === "ok" ? "good" : "warn"}>
          API {health.status}
        </Badge>
        <span className="text-[11px] tabular-nums text-[#6b7280]">
          {new Date(health.now).toLocaleTimeString("en-US", {
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
          })}
        </span>
      </div>
    </header>
  );
}
