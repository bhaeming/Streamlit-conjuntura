"use client";

import dynamic from "next/dynamic";
import type { EChartsOption } from "echarts";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

export function ChartCard({
  title,
  subtitle,
  option,
  tall = false,
}: {
  title: string;
  subtitle: string;
  option: EChartsOption;
  tall?: boolean;
}) {
  return (
    <section className="chart-card">
      <div className="chart-title"><div><h2>{title}</h2><p>{subtitle}</p></div></div>
      <ReactECharts option={option} style={{ height: tall ? 440 : 350 }} notMerge lazyUpdate />
    </section>
  );
}
