"use client";

import { useEffect, useRef, useState } from "react";

import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type Row = Record<string, number | string>;

const PIE_COLORS = ["#007D30", "#946300", "#002776", "#7C3AED", "#CC9A00", "#009C3B"];

function ChartFrame({ children }: { children: React.ReactNode }) {
  return <div className="h-72 min-h-[18rem] w-full min-w-0">{children}</div>;
}

function MeasuredChart({ render }: { render: (size: { width: number; height: number }) => React.ReactNode }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });

  useEffect(() => {
    const element = containerRef.current;
    if (!element) {
      return;
    }

    const updateSize = () => {
      const nextWidth = Math.floor(element.clientWidth);
      const nextHeight = Math.floor(element.clientHeight);
      setSize((current) =>
        current.width === nextWidth && current.height === nextHeight
          ? current
          : { width: nextWidth, height: nextHeight },
      );
    };

    updateSize();
    const observer = new ResizeObserver(updateSize);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const hasSize = size.width > 0 && size.height > 0;

  return (
    <div ref={containerRef} className="h-full w-full min-w-0">
      {hasSize ? (
        render(size)
      ) : (
        <div className="h-full w-full rounded-[20px] bg-slate-100/70" aria-hidden="true" />
      )}
    </div>
  );
}

function CardFrame({ title, subtitle, children }: { title: string; subtitle: string; children: React.ReactNode }) {
  return (
    <section className="rounded-[28px] border border-white/60 bg-white/80 p-5 shadow-[0_18px_60px_rgba(15,23,42,0.08)] backdrop-blur-xl">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <h3 className="font-mono text-sm font-semibold uppercase tracking-[0.24em] text-slate-500">{title}</h3>
          <p className="mt-1 text-sm text-slate-600">{subtitle}</p>
        </div>
      </div>
      <ChartFrame>{children}</ChartFrame>
    </section>
  );
}

export function DailyAreaChart({ data }: { data: Row[] }) {
  return (
    <CardFrame title="Série diária" subtitle="Eventos observados versus média histórica por dia ativo.">
      <MeasuredChart
        render={({ width, height }) => (
        <AreaChart width={width} height={height} data={data} margin={{ top: 8, right: 8, left: -12, bottom: 0 }}>
          <defs>
            <linearGradient id="eventsGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#007D30" stopOpacity={0.45} />
              <stop offset="95%" stopColor="#007D30" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke="#E2E8F0" strokeDasharray="3 3" vertical={false} />
          <XAxis dataKey="date" stroke="#64748B" tickLine={false} axisLine={false} />
          <YAxis stroke="#64748B" tickLine={false} axisLine={false} allowDecimals={false} />
          <Tooltip contentStyle={{ borderRadius: 16, borderColor: "#CBD5E1" }} />
          <Area type="monotone" dataKey="mediaHistorica" stroke="#94A3B8" strokeWidth={2} fill="transparent" />
          <Area type="monotone" dataKey="eventos" stroke="#007D30" strokeWidth={3} fill="url(#eventsGradient)" />
        </AreaChart>
        )}
      />
    </CardFrame>
  );
}

export function DistributionBars({ title, subtitle, data, valueLabel }: { title: string; subtitle: string; data: Row[]; valueLabel: string }) {
  return (
    <CardFrame title={title} subtitle={subtitle}>
      <MeasuredChart
        render={({ width, height }) => (
        <BarChart width={width} height={height} data={data} layout="vertical" margin={{ top: 4, right: 8, left: 24, bottom: 4 }}>
          <CartesianGrid stroke="#E2E8F0" strokeDasharray="3 3" horizontal={false} />
          <XAxis type="number" stroke="#64748B" tickLine={false} axisLine={false} allowDecimals={false} />
          <YAxis dataKey="name" type="category" width={120} stroke="#475569" tickLine={false} axisLine={false} />
          <Tooltip formatter={(value) => [value, valueLabel]} contentStyle={{ borderRadius: 16, borderColor: "#CBD5E1" }} />
          <Bar dataKey="value" radius={[0, 14, 14, 0]} fill="#002776" />
        </BarChart>
        )}
      />
    </CardFrame>
  );
}

export function DistributionDonut({ title, subtitle, data }: { title: string; subtitle: string; data: Row[] }) {
  return (
    <CardFrame title={title} subtitle={subtitle}>
      <MeasuredChart
        render={({ width, height }) => (
        <PieChart width={width} height={height}>
          <Pie data={data} dataKey="value" nameKey="name" innerRadius={68} outerRadius={104} paddingAngle={3}>
            {data.map((entry, index) => (
              <Cell key={String(entry.name)} fill={PIE_COLORS[index % PIE_COLORS.length]} />
            ))}
          </Pie>
          <Tooltip formatter={(value) => [value, "eventos"]} contentStyle={{ borderRadius: 16, borderColor: "#CBD5E1" }} />
        </PieChart>
        )}
      />
    </CardFrame>
  );
}

export function SegmentBarChart({ title, subtitle, data }: { title: string; subtitle: string; data: Row[] }) {
  return (
    <CardFrame title={title} subtitle={subtitle}>
      <MeasuredChart
        render={({ width, height }) => (
        <BarChart width={width} height={height} data={data} margin={{ top: 8, right: 8, left: -12, bottom: 8 }}>
          <CartesianGrid stroke="#E2E8F0" strokeDasharray="3 3" vertical={false} />
          <XAxis dataKey="name" stroke="#64748B" tickLine={false} axisLine={false} interval={0} angle={-20} textAnchor="end" height={72} />
          <YAxis stroke="#64748B" tickLine={false} axisLine={false} allowDecimals={false} />
          <Tooltip contentStyle={{ borderRadius: 16, borderColor: "#CBD5E1" }} />
          <Bar dataKey="value" radius={[14, 14, 0, 0]} fill="#946300" />
        </BarChart>
        )}
      />
    </CardFrame>
  );
}
