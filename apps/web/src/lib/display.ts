import type { DataQualityResult, MarketTaxonomy } from "@f1/shared-types";

const SESSION_CODE_LABELS: Record<string, string> = {
  FP1: "Practice 1",
  FP2: "Practice 2",
  FP3: "Practice 3",
  SQ: "Sprint Qualifying",
  S: "Sprint",
  Q: "Qualifying",
  R: "Race",
};

const TAXONOMY_LABELS: Record<string, string> = {
  head_to_head_session: "Head-to-head matchup",
  head_to_head_practice: "Practice head-to-head",
  driver_pole_position: "Pole position",
  constructor_pole_position: "Constructor pole",
  race_winner: "Race winner",
  sprint_winner: "Sprint winner",
  qualifying_winner: "Qualifying winner",
  driver_podium: "Podium finish",
  constructor_scores_first: "First constructor to score",
  constructor_fastest_lap_practice: "Constructor fastest lap",
  constructor_fastest_lap_session: "Constructor fastest lap",
  driver_fastest_lap_practice: "Driver fastest lap",
  driver_fastest_lap_session: "Driver fastest lap",
  drivers_champion: "Drivers' championship",
  constructors_champion: "Constructors' championship",
  red_flag: "Red flag",
  safety_car: "Safety car",
  other: "Other",
  q_pole: "Qualifying pole position",
  pole_position: "Pole position",
};

const TAXONOMY_SUMMARIES: Record<string, string> = {
  head_to_head_session:
    "Pick which driver beats the other in a qualifying, sprint, or race session.",
  head_to_head_practice:
    "Practice-session matchups built from the fastest timing or lap result.",
  driver_pole_position:
    "Markets for who starts first on the grid after qualifying.",
  constructor_pole_position:
    "Constructor-level markets based on who claims pole position.",
  race_winner: "Outright winner markets settled from the Grand Prix race.",
  sprint_winner: "Winner markets settled from the sprint session.",
  qualifying_winner: "Who tops the qualifying classification.",
  driver_podium: "Finish-on-the-podium markets for individual drivers.",
  constructor_scores_first:
    "Which team gets on the scoreboard before the others do.",
  constructor_fastest_lap_practice:
    "Fastest-lap markets derived from practice sessions.",
  constructor_fastest_lap_session:
    "Fastest-lap markets derived from sprint or race sessions.",
  driver_fastest_lap_practice:
    "Driver fastest-lap markets from practice sessions.",
  driver_fastest_lap_session:
    "Driver fastest-lap markets from sprint or race sessions.",
  drivers_champion: "Season-long championship outrights for drivers.",
  constructors_champion: "Season-long championship outrights for teams.",
  red_flag: "Special markets around red-flag incidents.",
  safety_car: "Special markets around safety-car incidents.",
};

type QualityDatasetInfo = {
  label: string;
  impact: string;
  optional: boolean;
};

const QUALITY_DATASET_INFO: Record<string, QualityDatasetInfo> = {
  source_fetch_log: {
    label: "Connector freshness log",
    impact:
      "Used to confirm when upstream sources were refreshed most recently.",
    optional: false,
  },
  polymarket_ws_message_manifest: {
    label: "Live Polymarket websocket capture",
    impact:
      "Used for live-only weekend monitoring and live paper-trading capture. Historical market, prediction, and backtest views still work without it.",
    optional: true,
  },
  f1_telemetry_index: {
    label: "F1 telemetry coverage",
    impact:
      "Supports higher-detail session analysis and telemetry-driven feature snapshots.",
    optional: false,
  },
};

const GP_NAME_LABELS: Record<string, string> = {
  australia: "Australian Grand Prix",
  bahrain: "Bahrain Grand Prix",
  china: "Chinese Grand Prix",
  japan: "Japanese Grand Prix",
  saudi_arabia: "Saudi Arabian Grand Prix",
  miami: "Miami Grand Prix",
  emilia_romagna: "Emilia Romagna Grand Prix",
  monaco: "Monaco Grand Prix",
  spain: "Spanish Grand Prix",
  canada: "Canadian Grand Prix",
  austria: "Austrian Grand Prix",
  great_britain: "British Grand Prix",
  belgium: "Belgian Grand Prix",
  hungary: "Hungarian Grand Prix",
  netherlands: "Dutch Grand Prix",
  italy: "Italian Grand Prix",
  azerbaijan: "Azerbaijan Grand Prix",
  singapore: "Singapore Grand Prix",
  united_states: "United States Grand Prix",
  mexico: "Mexico City Grand Prix",
  brazil: "Sao Paulo Grand Prix",
  las_vegas: "Las Vegas Grand Prix",
  qatar: "Qatar Grand Prix",
  abu_dhabi: "Abu Dhabi Grand Prix",
};

const STAGE_CODE_SET = new Set(
  Object.keys(SESSION_CODE_LABELS).map((code) => code.toLowerCase()),
);

function titleCaseWord(word: string): string {
  if (word === "f1") {
    return "F1";
  }
  return word.charAt(0).toUpperCase() + word.slice(1);
}

export function humanizeIdentifier(value: string): string {
  return value.split(/[_-]+/).filter(Boolean).map(titleCaseWord).join(" ");
}

export function formatSessionCodeLabel(
  code: string | null | undefined,
): string {
  if (!code) {
    return "Unlabeled session";
  }
  return SESSION_CODE_LABELS[code.toUpperCase()] ?? code;
}

export function formatTaxonomyLabel(
  taxonomy: MarketTaxonomy | string | null | undefined,
): string {
  if (!taxonomy) {
    return "Other";
  }
  return TAXONOMY_LABELS[taxonomy] ?? humanizeIdentifier(taxonomy);
}

export function formatTaxonomySummary(
  taxonomy: MarketTaxonomy | string | null | undefined,
): string {
  if (!taxonomy) {
    return "Unclassified market family.";
  }
  return (
    TAXONOMY_SUMMARIES[taxonomy] ??
    `${formatTaxonomyLabel(taxonomy)} markets linked to the current F1 workflow.`
  );
}

export function formatPriceCents(value: number | null | undefined): string {
  if (value == null) {
    return "—";
  }
  return `${(value * 100).toFixed(1)}¢`;
}

export function formatUsd(value: number | null | undefined): string {
  if (value == null) {
    return "—";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

export function formatCompactUsd(value: number | null | undefined): string {
  if (value == null) {
    return "—";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: value >= 1000 ? 1 : 0,
  }).format(value);
}

export function formatProbability(
  value: number | null | undefined,
  digits = 1,
): string {
  if (value == null) {
    return "—";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

export function formatPercentValue(
  value: number | null | undefined,
  digits = 1,
): string {
  if (value == null) {
    return "—";
  }
  return `${value.toFixed(digits)}%`;
}

export function formatDateTimeShort(value: string | null | undefined): string {
  if (!value) {
    return "—";
  }
  return new Date(value).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

export function formatDateRangeShort(
  start: string | null | undefined,
  end: string | null | undefined,
): string {
  if (!start && !end) {
    return "Dates unavailable";
  }
  if (start && !end) {
    return new Date(start).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  }
  if (!start && end) {
    return new Date(end).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  }

  const startDate = new Date(start as string);
  const endDate = new Date(end as string);
  const sameYear = startDate.getFullYear() === endDate.getFullYear();
  const startLabel = startDate.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    ...(sameYear ? {} : { year: "numeric" }),
  });
  const endLabel = endDate.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
  return `${startLabel} – ${endLabel}`;
}

export function describeQualityDataset(dataset: string): QualityDatasetInfo {
  return (
    QUALITY_DATASET_INFO[dataset] ?? {
      label: humanizeIdentifier(dataset),
      impact: "Check this dataset if related dashboards or actions look stale.",
      optional: false,
    }
  );
}

export function describeQualityAlert(result: DataQualityResult): string {
  const info = describeQualityDataset(result.dataset);
  const rowCount =
    typeof result.metricsJson?.row_count === "number"
      ? result.metricsJson.row_count
      : null;

  if (result.status === "pass") {
    return `${info.label} is healthy.`;
  }
  if (rowCount === 0 && info.optional) {
    return `${info.label} has no captured rows yet. This mainly affects live-only monitoring, not the main research pages.`;
  }
  if (rowCount === 0) {
    return `${info.label} has no rows in the latest quality check.`;
  }
  return `${info.label} needs follow-up. ${info.impact}`;
}

function formatGpLabel(slug: string): string {
  return GP_NAME_LABELS[slug] ?? humanizeIdentifier(slug);
}

type StageDescriptor = {
  label: string;
  context: string | null;
};

export function describeStage(
  stage: string | null | undefined,
): StageDescriptor {
  if (!stage) {
    return { label: "Unlabeled stage", context: null };
  }

  let base = stage.toLowerCase();
  let suffix: "quicktest" | "snapshot" | "backtest" | null = null;
  for (const candidate of ["_quicktest", "_snapshot", "_backtest"] as const) {
    if (base.endsWith(candidate)) {
      base = base.slice(0, -candidate.length);
      suffix =
        candidate === "_quicktest"
          ? "quicktest"
          : candidate === "_snapshot"
            ? "snapshot"
            : "backtest";
      break;
    }
  }

  if (base.includes("_to_")) {
    const [left, right] = base.split("_to_");
    const leftTokens = left.split("_").filter(Boolean);
    const sourceCode = leftTokens.at(-1) ?? "";
    const gpSlug = leftTokens.slice(0, -1).join("_");
    const gpLabel = gpSlug ? formatGpLabel(gpSlug) : null;
    const sourceLabel = formatSessionCodeLabel(sourceCode.toUpperCase());
    const targetLabel = formatTaxonomyLabel(right);

    return {
      label: gpLabel
        ? `${gpLabel} · ${sourceLabel} to ${targetLabel}`
        : `${sourceLabel} to ${targetLabel}`,
      context:
        suffix === "snapshot"
          ? "Feature snapshot"
          : suffix === "backtest"
            ? "Backtest stage"
            : "Forecast stage",
    };
  }

  const tokens = base.split("_").filter(Boolean);
  if (tokens.length >= 3 && STAGE_CODE_SET.has(tokens[1] ?? "")) {
    const gpLabel = formatGpLabel(tokens[0] ?? "");
    const sourceLabel = formatSessionCodeLabel((tokens[1] ?? "").toUpperCase());
    const targetLabel = formatTaxonomyLabel(tokens.slice(2).join("_"));
    return {
      label: `${gpLabel} · ${sourceLabel} to ${targetLabel}`,
      context:
        suffix === "snapshot"
          ? "Feature snapshot"
          : suffix === "backtest"
            ? "Backtest stage"
            : "Forecast stage",
    };
  }

  return {
    label:
      suffix === "backtest"
        ? `${humanizeIdentifier(base)} backtest`
        : humanizeIdentifier(base),
    context:
      suffix === "snapshot"
        ? "Feature snapshot"
        : suffix === "quicktest"
          ? "Quick test"
          : suffix === "backtest"
            ? "Backtest stage"
            : null,
  };
}

export function describePredictionSignal(
  probabilityYes: number | null | undefined,
): { label: string; tone: "default" | "good" | "warn" } {
  if (probabilityYes == null) {
    return { label: "No signal", tone: "default" };
  }
  if (probabilityYes >= 0.75) {
    return { label: "Strong YES", tone: "good" };
  }
  if (probabilityYes >= 0.55) {
    return { label: "Lean YES", tone: "good" };
  }
  if (probabilityYes <= 0.25) {
    return { label: "Strong NO", tone: "warn" };
  }
  if (probabilityYes <= 0.45) {
    return { label: "Lean NO", tone: "warn" };
  }
  return { label: "Near even", tone: "default" };
}
