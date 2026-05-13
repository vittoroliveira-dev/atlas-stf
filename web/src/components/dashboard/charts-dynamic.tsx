"use client";

import dynamic from "next/dynamic";

import { ChartSkeleton } from "./chart-skeleton";

export const DailyAreaChart = dynamic(
  () => import("./charts").then((m) => m.DailyAreaChart),
  { ssr: false, loading: () => <ChartSkeleton /> },
);

export const DistributionBars = dynamic(
  () => import("./charts").then((m) => m.DistributionBars),
  { ssr: false, loading: () => <ChartSkeleton /> },
);

export const DistributionDonut = dynamic(
  () => import("./charts").then((m) => m.DistributionDonut),
  { ssr: false, loading: () => <ChartSkeleton /> },
);

export const SegmentBarChart = dynamic(
  () => import("./charts").then((m) => m.SegmentBarChart),
  { ssr: false, loading: () => <ChartSkeleton /> },
);
