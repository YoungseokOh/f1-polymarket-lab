"use client";

import ReactEChartsCore from "echarts-for-react/lib/core";
import { LineChart, ScatterChart } from "echarts/charts";
import { GridComponent, TooltipComponent } from "echarts/components";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";

import type { CalibrationPoint } from "../../../lib/calibration";

echarts.use([
  ScatterChart,
  LineChart,
  GridComponent,
  TooltipComponent,
  CanvasRenderer,
]);

type CalibrationChartProps = {
  points: CalibrationPoint[];
  height?: number;
};

export function CalibrationChart({
  points,
  height = 280,
}: CalibrationChartProps) {
  if (points.length === 0) {
    return (
      <div
        className="flex items-center justify-center text-sm text-[#6b7280]"
        style={{ height }}
      >
        No calibration data
      </div>
    );
  }

  const option: echarts.EChartsCoreOption = {
    backgroundColor: "transparent",
    grid: { top: 24, right: 16, bottom: 32, left: 44, containLabel: false },
    tooltip: {
      trigger: "item",
      backgroundColor: "#1e1e2e",
      borderColor: "rgba(255,255,255,0.08)",
      textStyle: { color: "#d1d5db", fontSize: 11 },
      formatter: (p: { data: [number, number, number, string] }) =>
        `Bucket: ${p.data[3]}<br/>Predicted: ${(p.data[0] * 100).toFixed(1)}%<br/>Actual: ${(p.data[1] * 100).toFixed(1)}%<br/>Samples: ${p.data[2]}`,
    },
    xAxis: {
      type: "value",
      name: "Predicted",
      nameTextStyle: { color: "#6b7280", fontSize: 9 },
      min: 0,
      max: 1,
      axisLabel: {
        color: "#6b7280",
        fontSize: 9,
        formatter: (v: number) => `${(v * 100).toFixed(0)}%`,
      },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.04)" } },
    },
    yAxis: {
      type: "value",
      name: "Actual",
      nameTextStyle: { color: "#6b7280", fontSize: 9 },
      min: 0,
      max: 1,
      axisLabel: {
        color: "#6b7280",
        fontSize: 9,
        formatter: (v: number) => `${(v * 100).toFixed(0)}%`,
      },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.04)" } },
    },
    series: [
      {
        type: "line",
        data: points.map((point) => [point.predicted, point.actual]),
        symbol: "none",
        lineStyle: {
          width: 2,
          color: "#f97316",
        },
        silent: true,
      },
      {
        type: "scatter",
        data: points.map((point) => [
          point.predicted,
          point.actual,
          point.count,
          point.bucketLabel,
        ]),
        symbolSize: (value: number[]) => {
          const count = value[2] ?? 1;
          return Math.max(8, Math.min(22, 8 + Math.log2(count + 1) * 4));
        },
        itemStyle: { color: "#e10600" },
      },
      {
        type: "line",
        data: [
          [0, 0],
          [1, 1],
        ],
        symbol: "none",
        lineStyle: {
          width: 1,
          color: "rgba(255,255,255,0.12)",
          type: "dashed",
        },
        silent: true,
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
