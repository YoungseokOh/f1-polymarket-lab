"use client";

import ReactEChartsCore from "echarts-for-react/lib/core";
import { LineChart } from "echarts/charts";
import { GridComponent, TooltipComponent } from "echarts/components";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([LineChart, GridComponent, TooltipComponent, CanvasRenderer]);

type PnlAreaChartProps = {
  labels: string[];
  values: number[];
  height?: number;
};

export function PnlAreaChart({
  labels,
  values,
  height = 280,
}: PnlAreaChartProps) {
  if (values.length === 0) {
    return (
      <div
        className="flex items-center justify-center text-sm text-[#6b7280]"
        style={{ height }}
      >
        No PnL data available
      </div>
    );
  }

  const option: echarts.EChartsCoreOption = {
    backgroundColor: "transparent",
    grid: { top: 24, right: 16, bottom: 28, left: 52, containLabel: false },
    tooltip: {
      trigger: "axis",
      backgroundColor: "#1e1e2e",
      borderColor: "rgba(255,255,255,0.08)",
      textStyle: { color: "#d1d5db", fontSize: 11 },
    },
    xAxis: {
      type: "category",
      data: labels,
      axisLabel: { color: "#6b7280", fontSize: 9 },
      axisLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } },
    },
    yAxis: {
      type: "value",
      axisLabel: {
        color: "#6b7280",
        fontSize: 9,
        formatter: (v: number) => `$${v.toFixed(0)}`,
      },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.04)" } },
    },
    series: [
      {
        type: "line",
        data: values,
        smooth: true,
        symbol: "none",
        lineStyle: { width: 2, color: "#00d747" },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: "rgba(0,215,71,0.12)" },
            { offset: 1, color: "rgba(0,215,71,0)" },
          ]),
        },
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
