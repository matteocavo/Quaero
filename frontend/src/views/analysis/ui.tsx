import React from 'react';
import type { ColumnKind } from '../../analysis/types';

export const inputClass =
  'w-full rounded-lg border border-slate-300 bg-white px-2.5 py-1.5 text-xs text-slate-900 ' +
  'focus:border-sky-500 focus:outline-none focus:ring-1 focus:ring-sky-500 ' +
  'disabled:cursor-not-allowed disabled:opacity-60 ' +
  'dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100';

export const primaryButton =
  'inline-flex items-center justify-center gap-1.5 rounded-xl bg-sky-600 px-4 py-2 text-xs font-bold text-white ' +
  'shadow-xs transition hover:bg-sky-500 disabled:cursor-not-allowed disabled:opacity-50 ' +
  'dark:bg-sky-500 dark:hover:bg-sky-400';

export const secondaryButton =
  'inline-flex items-center justify-center gap-1.5 rounded-xl border border-slate-300 bg-white px-3 py-1.5 ' +
  'text-xs font-semibold text-slate-800 transition hover:bg-slate-50 disabled:cursor-not-allowed ' +
  'disabled:opacity-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100 dark:hover:bg-slate-700';

export const Card: React.FC<{ title?: string; subtitle?: string; children: React.ReactNode; className?: string }> = ({
  title,
  subtitle,
  children,
  className = '',
}) => (
  <section
    className={`rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900 ${className}`}
  >
    {title && (
      <header className="mb-3">
        <h3 className="text-sm font-bold text-slate-900 dark:text-white">{title}</h3>
        {subtitle && <p className="mt-0.5 text-xs text-slate-500 dark:text-slate-400">{subtitle}</p>}
      </header>
    )}
    {children}
  </section>
);

export const Field: React.FC<{ label: string; htmlFor: string; children: React.ReactNode; hint?: string }> = ({
  label,
  htmlFor,
  children,
  hint,
}) => (
  <div className="space-y-1">
    <label htmlFor={htmlFor} className="block text-[11px] font-semibold text-slate-600 dark:text-slate-300">
      {label}
    </label>
    {children}
    {hint && <p className="text-[11px] text-slate-500 dark:text-slate-400">{hint}</p>}
  </div>
);

const KIND_STYLE: Record<ColumnKind, string> = {
  numeric: 'bg-sky-100 text-sky-900 dark:bg-sky-950 dark:text-sky-200',
  categorical: 'bg-violet-100 text-violet-900 dark:bg-violet-950 dark:text-violet-200',
  date: 'bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200',
  boolean: 'bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200',
};

export const KindBadge: React.FC<{ kind: ColumnKind }> = ({ kind }) => (
  <span className={`rounded px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wide ${KIND_STYLE[kind]}`}>
    {kind}
  </span>
);

export const Notice: React.FC<{ tone?: 'info' | 'warning' | 'error'; children: React.ReactNode }> = ({
  tone = 'info',
  children,
}) => {
  const style =
    tone === 'error'
      ? 'border-rose-200 bg-rose-50 text-rose-900 dark:border-rose-900 dark:bg-rose-950/40 dark:text-rose-200'
      : tone === 'warning'
        ? 'border-amber-200 bg-amber-50 text-amber-900 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-200'
        : 'border-sky-200 bg-sky-50 text-sky-900 dark:border-sky-900 dark:bg-sky-950/40 dark:text-sky-200';
  return (
    <div role={tone === 'error' ? 'alert' : undefined} className={`rounded-xl border p-3 text-xs ${style}`}>
      {children}
    </div>
  );
};

export const Stat: React.FC<{ label: string; value: React.ReactNode; hint?: string }> = ({ label, value, hint }) => (
  <div className="rounded-xl border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900">
    <div className="text-[11px] font-semibold text-slate-500 dark:text-slate-400">{label}</div>
    <div className="mt-0.5 text-lg font-bold text-slate-900 dark:text-white">{value}</div>
    {hint && <div className="text-[11px] text-slate-500 dark:text-slate-400">{hint}</div>}
  </div>
);
