"use client";

import type { PricePoint } from "@f1/shared-types";
import ReactEChartsCore from "echarts-for-react/lib/core";
import { LineChart } from "echarts/charts";
import {
  GridComponent,
  LegendComponent,
  TooltipComponent,
} from "echarts/components";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([
  LineChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  CanvasRenderer,
]);

type PriceLineChartProps = {
  data: PricePoint[];
  height?: number;
};

export function PriceLineChart({ data, height = 320 }: PriceLineChartProps) {
  if (data.length === 0) {
    return (
      <div
        className="flex items-center justify-center text-sm text-[#6b7280]"
        style={{ height }}
      >
        No price data available
      </div>
    );
  }

  const times = data.map((d) => d.observedAtUtc);
  const prices = data.map((d) => d.price ?? d.midpoint ?? null);
  const bids = data.map((d) => d.bestBid);
  const asks = data.map((d) => d.bestAsk);

  const option: echarts.EChartsCoreOption = {
    backgroundColor: "transparent",
    grid: { top: 40, right: 16, bottom: 32, left: 48, containLabel: false },
    tooltip: {
      trigger: "axis",
      backgroundColor: "#1e1e2e",
      borderColor: "rgba(255,255,255,0.08)",
      textStyle: { color: "#d1d5db", fontSize: 11 },
    },
    legend: {
      top: 4,
      right: 0,
      textStyle: { color: "#9ca3af", fontSize: 10 },
      itemWidth: 12,
      itemHeight: 2,
    },
    xAxis: {
      type: "category",
      data: times,
      axisLabel: {
        color: "#6b7280",
        fontSize: 9,
        formatter: (v: string) => {
          const d = new Date(v);
          return `${d.getMonth() + 1}/${d.getDate()} ${d.getHours()}:${String(d.getMinutes()).padStart(2, "0")}`;
        },
      },
      axisLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } },
      splitLine: { show: false },
    },
    yAxis: {
      type: "value",
      min: 0,
      max: 1,
      axisLabel: {
        color: "#6b7280",
        fontSize: 9,
        formatter: (v: number) => `${(v * 100).toFixed(0)}¢`,
      },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.04)" } },
    },
    series: [
      {
        name: "Price",
        type: "line",
        data: prices,
        smooth: true,
        symbol: "none",
        lineStyle: { width: 2, color: "#e10600" },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: "rgba(225,6,0,0.15)" },
            { offset: 1, color: "rgba(225,6,0,0)" },
          ]),
        },
      },
      {
        name: "Bid",
        type: "line",
        data: bids,
        smooth: true,
        symbol: "none",
        lineStyle: { width: 1, color: "#00d747", type: "dashed" },
      },
      {
        name: "Ask",
        type: "line",
        data: asks,
        smooth: true,
        symbol: "none",
        lineStyle: { width: 1, color: "#ffd600", type: "dashed" },
      },
    ],
  };

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={option}
      style={{ height, width: "100%" }}
      notMerge
      lazyUpdate
    />
  );
}
