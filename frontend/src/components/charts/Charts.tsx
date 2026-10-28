import React from 'react';
import { formatNumber, truncate } from '../../analysis/format';

/** Small dependency-free SVG charts. Each one is an image with a text title; the data is also shown
 *  in a table next to it, so nothing is available only visually. */

const AXIS = 'stroke-slate-300 dark:stroke-slate-700';
const TEXT = 'fill-slate-600 dark:fill-slate-300';
const BAR = 'fill-sky-500 dark:fill-sky-400';

export interface ChartDatum {
  label: string;
  value: number | null;
}

// ---------------------------------------------------------------------------- bar chart

const BAR_ROW = 26;
const BAR_LEFT = 190;
const BAR_RIGHT = 90;
const BAR_WIDTH = 720;

export const BarChart: React.FC<{ data: ChartDatum[]; title: string }> = ({ data, title }) => {
  const values = data.map((d) => d.value).filter((v): v is number => v !== null);
  const low = Math.min(0, ...values);
  const high = Math.max(0, ...values);
  const span = high - low || 1;
  const plot = BAR_WIDTH - BAR_LEFT - BAR_RIGHT;
  const x = (value: number) => BAR_LEFT + ((value - low) / span) * plot;
  const zero = x(0);
  const height = data.length * BAR_ROW + 16;

  return (
    <svg
      role="img"
      aria-label={title}
      viewBox={`0 0 ${BAR_WIDTH} ${height}`}
      className="h-auto w-full"
      data-chart="bar"
    >
      <title>{title}</title>
      <line x1={zero} x2={zero} y1={4} y2={height - 4} className={AXIS} />
      {data.map((datum, index) => {
        const y = 8 + index * BAR_ROW;
        const value = datum.value;
        const barX = value === null ? zero : Math.min(zero, x(value));
        const barW = value === null ? 0 : Math.abs(x(value) - zero);
        return (
          <g key={`${datum.label}-${index}`}>
            <text x={BAR_LEFT - 8} y={y + 14} textAnchor="end" className={`${TEXT} text-[11px]`}>
              {truncate(datum.label, 28)}
              <title>{datum.label}</title>
            </text>
            <rect x={barX} y={y + 2} width={Math.max(barW, value === 0 ? 1 : 0)} height={16} rx={3} className={BAR} />
            <text
              x={value !== null && value < 0 ? barX - 6 : barX + barW + 6}
              y={y + 14}
              textAnchor={value !== null && value < 0 ? 'end' : 'start'}
              className={`${TEXT} text-[11px] font-semibold`}
            >
              {value === null ? '—' : formatNumber(value)}
            </text>
          </g>
        );
      })}
    </svg>
  );
};

// --------------------------------------------------------------------------- line chart

const LINE_W = 720;
const LINE_H = 280;
const PAD = { left: 64, right: 20, top: 16, bottom: 44 };

export const LineChart: React.FC<{ data: ChartDatum[]; title: string }> = ({ data, title }) => {
  const values = data.map((d) => d.value).filter((v): v is number => v !== null);
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 1;
  const low = min === max ? min - 1 : min;
  const high = min === max ? max + 1 : max;
  const plotW = LINE_W - PAD.left - PAD.right;
  const plotH = LINE_H - PAD.top - PAD.bottom;
  const px = (i: number) => PAD.left + (data.length <= 1 ? plotW / 2 : (i / (data.length - 1)) * plotW);
  const py = (v: number) => PAD.top + plotH - ((v - low) / (high - low)) * plotH;

  // Break the line at missing values instead of drawing through them.
  const segments: string[] = [];
  let current: string[] = [];
  data.forEach((d, i) => {
    if (d.value === null) {
      if (current.length > 1) segments.push(current.join(' '));
      current = [];
    } else {
      current.push(`${px(i)},${py(d.value)}`);
    }
  });
  if (current.length > 1) segments.push(current.join(' '));

  const labelStep = Math.max(1, Math.ceil(data.length / 8));
  const ticks = [low, (low + high) / 2, high];

  return (
    <svg role="img" aria-label={title} viewBox={`0 0 ${LINE_W} ${LINE_H}`} className="h-auto w-full" data-chart="line">
      <title>{title}</title>
      {ticks.map((tick) => (
        <g key={tick}>
          <line x1={PAD.left} x2={LINE_W - PAD.right} y1={py(tick)} y2={py(tick)} className={AXIS} strokeDasharray="3 3" />
          <text x={PAD.left - 8} y={py(tick) + 4} textAnchor="end" className={`${TEXT} text-[11px]`}>
            {formatNumber(Number(tick.toPrecision(5)))}
          </text>
        </g>
      ))}
      {segments.map((points) => (
        <polyline key={points} points={points} fill="none" strokeWidth={2} className="stroke-sky-500 dark:stroke-sky-400" />
      ))}
      {data.map((d, i) =>
        d.value === null ? null : (
          <circle key={`${d.label}-${i}`} cx={px(i)} cy={py(d.value)} r={3.5} className={BAR}>
            <title>{`${d.label}: ${formatNumber(d.value)}`}</title>
          </circle>
        ),
      )}
      {data.map((d, i) =>
        i % labelStep === 0 ? (
          <text key={`x-${d.label}-${i}`} x={px(i)} y={LINE_H - 20} textAnchor="middle" className={`${TEXT} text-[11px]`}>
            {truncate(d.label, 10)}
          </text>
        ) : null,
      )}
    </svg>
  );
};

// ----------------------------------------------------------------------------- histogram

const HIST_W = 720;
const HIST_H = 240;

export const HistogramChart: React.FC<{
  bins: { from: number; to: number; count: number }[];
  title: string;
}> = ({ bins, title }) => {
  const max = Math.max(1, ...bins.map((b) => b.count));
  const plotW = HIST_W - 60;
  const plotH = HIST_H - 56;
  const barW = bins.length ? plotW / bins.length : plotW;
  return (
    <svg role="img" aria-label={title} viewBox={`0 0 ${HIST_W} ${HIST_H}`} className="h-auto w-full" data-chart="histogram">
      <title>{title}</title>
      <line x1={40} x2={HIST_W - 20} y1={plotH + 16} y2={plotH + 16} className={AXIS} />
      {bins.map((bin, i) => {
        const h = (bin.count / max) * plotH;
        return (
          <g key={`${bin.from}-${i}`}>
            <rect x={40 + i * barW + 2} y={plotH + 16 - h} width={Math.max(barW - 4, 1)} height={h} rx={2} className={BAR}>
              <title>{`${formatNumber(Number(bin.from.toPrecision(5)))} to ${formatNumber(Number(bin.to.toPrecision(5)))}: ${bin.count}`}</title>
            </rect>
            {bin.count > 0 && (
              <text x={40 + i * barW + barW / 2} y={plotH + 12 - h} textAnchor="middle" className={`${TEXT} text-[10px]`}>
                {bin.count}
              </text>
            )}
          </g>
        );
      })}
      {bins.length > 0 && (
        <>
          <text x={40} y={HIST_H - 14} className={`${TEXT} text-[11px]`}>
            {formatNumber(Number(bins[0].from.toPrecision(5)))}
          </text>
          <text x={HIST_W - 20} y={HIST_H - 14} textAnchor="end" className={`${TEXT} text-[11px]`}>
            {formatNumber(Number(bins[bins.length - 1].to.toPrecision(5)))}
          </text>
        </>
      )}
    </svg>
  );
};
