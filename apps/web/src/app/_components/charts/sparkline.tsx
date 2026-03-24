"use client";

import ReactEChartsCore from "echarts-for-react/lib/core";
import { LineChart } from "echarts/charts";
import { GridComponent } from "echarts/components";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([LineChart, GridComponent, CanvasRenderer]);

type SparklineProps = {
  values: number[];
  width?: number;
  height?: number;
  color?: string;
};

export function Sparkline({
  values,
  width = 80,
  height = 24,
  color = "#e10600",
}: SparklineProps) {
  if (values.length < 2) return null;

  const option: echarts.EChartsCoreOption = {
    backgroundColor: "transparent",
    grid: { top: 0, right: 0, bottom: 0, left: 0 },
    xAxis: { type: "category", show: false },
    yAxis: { type: "value", show: false, min: "dataMin", max: "dataMax" },
    series: [
      {
        type: "line",
        data: values,
        symbol: "none",
        smooth: true,
        lineStyle: { width: 1.5, color },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: `${color}20` },
            { offset: 1, color: `${color}00` },
          ]),
        },
      },
    ],
  };

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={option}
      style={{ width, height }}
      notMerge
      lazyUpdate
    />
  );
}
