import React, { useMemo } from 'react';
import { AlertTriangle, Info } from 'lucide-react';
import { adviseDataset, type ColumnAdvice, type Suggestion } from '../../analysis/advisor';
import { useApp } from '../../context/AppContext';
import { Card, KindBadge, Notice, primaryButton, secondaryButton } from './ui';

const CHART_LABEL: Record<Suggestion['chart'], string> = {
  bar: 'bar chart',
  line: 'line chart',
  histogram: 'histogram',
  table: 'table',
};

const ACTION: Record<Suggestion['kind'], string> = {
  query: 'Run this analysis',
  distribution: 'Open the distribution',
  outliers: 'Show the unusual values',
};

const ROLE_LABEL: Record<ColumnAdvice['role'], string> = {
  metric: 'measure',
  category: 'grouping',
  label: 'label',
  time: 'time',
  identifier: 'identifier',
  unused: 'not used',
};

/** Columns that the rules flag, with the reason in plain words. Used in Data Quality and in the suggestions. */
export const ColumnHealthCard: React.FC = () => {
  const { activeDataset: dataset } = useApp();
  const advice = useMemo(() => (dataset ? adviseDataset(dataset) : null), [dataset]);
  if (!dataset || !advice) return null;

  const flagged = advice.columns.filter((c) => c.concerns.length > 0);
  const left = flagged.filter((c) => c.concerns.some((x) => x.severity === 'warning'));
  const notes = flagged.filter((c) => !left.includes(c));

  return (
    <Card
      title="Columns to treat with care"
      subtitle="Constant, nearly constant, mostly empty or key-like columns say little, so they are left out of the suggestions. The rules are fixed and listed in the README."
    >
      {flagged.length === 0 ? (
        <Notice>No column is constant, nearly constant, mostly empty or key-like. Some may still have a few missing values: see above.</Notice>
      ) : (
        <ul className="space-y-2" data-testid="column-health">
          {[...left, ...notes].map((c) => (
            <li key={c.name} className="rounded-lg border border-slate-200 p-2.5 text-xs dark:border-slate-800">
              <div className="flex flex-wrap items-center gap-2">
                <span className="font-mono font-semibold text-slate-900 dark:text-slate-100">{c.name}</span>
                <KindBadge kind={c.kind} />
                <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wide text-slate-700 dark:bg-slate-800 dark:text-slate-200">
                  {ROLE_LABEL[c.role]}
                </span>
              </div>
              <ul className="mt-1 list-disc pl-5 text-slate-700 dark:text-slate-200">
                {c.concerns.map((x) => (
                  <li key={x.code}>{x.text}</li>
                ))}
              </ul>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
};

/** Ready-made analyses for the loaded data, each with its reason, its exact calculation and its limits. */
export const Suggestions: React.FC = () => {
  const { activeDataset: dataset, applySpec, setWorkspaceTab, setDistributionColumn, draftSpec } = useApp();
  const advice = useMemo(() => (dataset ? adviseDataset(dataset) : null), [dataset]);
  if (!dataset || !advice) return null;

  const left = advice.columns.filter((c) => c.concerns.some((x) => x.severity === 'warning'));
  const filters = draftSpec.filters.length;

  const apply = (s: Suggestion) => {
    if (s.kind === 'query' && s.spec) {
      if (applySpec(s.spec)) setWorkspaceTab('results');
      return;
    }
    if (s.column) {
      setDistributionColumn(s.column);
      setWorkspaceTab('explore');
    }
  };

  return (
    <Card
      title="Suggested analyses"
      subtitle="Worked out from your columns by fixed rules, in this browser tab. No AI is used and the Python pipeline is not involved."
    >
      <div className="space-y-3">
        {advice.nothingBecause && <Notice tone="warning">{advice.nothingBecause}</Notice>}

        {advice.suggestions.length > 0 && (
          <ol className="space-y-3" data-testid="suggestions">
            {advice.suggestions.map((s) => (
              <li key={s.id} data-testid="suggestion" className="rounded-xl border border-slate-200 p-3 text-xs dark:border-slate-800">
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <div className="min-w-0">
                    <h4 className="break-words text-sm font-bold text-slate-900 dark:text-white">{s.title}</h4>
                    <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">{CHART_LABEL[s.chart]}</span>
                  </div>
                  <button
                    type="button"
                    className={s.kind === 'query' ? primaryButton : secondaryButton}
                    aria-label={`${ACTION[s.kind]}: ${s.title}`}
                    onClick={() => apply(s)}
                  >
                    {ACTION[s.kind]}
                  </button>
                </div>
                <dl className="mt-2 space-y-1.5 text-slate-700 dark:text-slate-200">
                  <div>
                    <dt className="font-semibold text-slate-900 dark:text-white">Why</dt>
                    <dd>
                      <ul className="list-disc pl-5">
                        {s.because.map((b) => (
                          <li key={b}>{b}</li>
                        ))}
                      </ul>
                    </dd>
                  </div>
                  <div>
                    <dt className="font-semibold text-slate-900 dark:text-white">What is calculated</dt>
                    <dd>{s.calculation}</dd>
                  </div>
                  <div>
                    <dt className="font-semibold text-slate-900 dark:text-white">Limits</dt>
                    <dd>
                      <ul className="list-disc pl-5">
                        {s.limits.map((l) => (
                          <li key={l}>{l}</li>
                        ))}
                      </ul>
                    </dd>
                  </div>
                </dl>
              </li>
            ))}
          </ol>
        )}

        {advice.hidden > 0 && (
          <p className="flex items-center gap-1.5 text-[11px] text-slate-500 dark:text-slate-400">
            <Info className="h-3.5 w-3.5" /> {advice.hidden} more suggestion{advice.hidden === 1 ? '' : 's'} not shown: up to 3 numeric columns, in file order.
          </p>
        )}
        {filters > 0 && advice.suggestions.some((s) => s.kind === 'query') && (
          <p className="text-[11px] text-slate-500 dark:text-slate-400">
            Your {filters} filter{filters === 1 ? ' stays' : 's stay'} applied when you run a suggestion; the reasons above describe the whole file.
          </p>
        )}
        {left.length > 0 && (
          <p className="flex items-start gap-1.5 text-[11px] text-amber-800 dark:text-amber-300">
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            <span>
              Left out of the suggestions: {left.map((c) => c.name).join(', ')}. See Data Quality for the reason.
            </span>
          </p>
        )}
      </div>
    </Card>
  );
};
