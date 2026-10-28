import React from 'react';
import { X } from 'lucide-react';
import { MAX_DIMENSIONS, MAX_MEASURES, MAX_RESULT_ROWS } from '../../analysis/limits';
import { dimensionKey, measureKey, specToSentence, validateSpec } from '../../analysis/query';
import type {
  AggregateFn,
  AnalysisSpec,
  Dataset,
  DatasetColumn,
  DateBin,
  Dimension,
  Measure,
} from '../../analysis/types';
import { useApp } from '../../context/AppContext';
import { Card, Field, Notice, inputClass, primaryButton, secondaryButton } from './ui';

const FUNCTIONS: { fn: AggregateFn; label: string }[] = [
  { fn: 'count', label: 'Count' },
  { fn: 'count_distinct', label: 'Count distinct' },
  { fn: 'sum', label: 'Sum' },
  { fn: 'avg', label: 'Average' },
  { fn: 'min', label: 'Minimum' },
  { fn: 'max', label: 'Maximum' },
];

const BINS: { value: DateBin | ''; label: string }[] = [
  { value: '', label: 'exact date' },
  { value: 'day', label: 'per day' },
  { value: 'month', label: 'per month' },
  { value: 'year', label: 'per year' },
];

function columnsFor(dataset: Dataset, fn: AggregateFn): DatasetColumn[] {
  if (fn === 'sum' || fn === 'avg') return dataset.columns.filter((c) => c.kind === 'numeric');
  if (fn === 'min' || fn === 'max') return dataset.columns.filter((c) => c.kind === 'numeric' || c.kind === 'date');
  return dataset.columns;
}

// ------------------------------------------------------------------------------ patterns

interface Pattern {
  id: string;
  title: string;
  description: string;
  /** Null when the dataset has what the pattern needs; otherwise why it is not available. */
  unavailable: string | null;
  apply: () => Partial<AnalysisSpec>;
}

function pickGroupColumn(dataset: Dataset): DatasetColumn | undefined {
  const candidates = dataset.columns.filter((c) => c.kind === 'categorical' || c.kind === 'boolean');
  return (
    candidates.find((c) => c.distinctCount >= 2 && c.distinctCount <= 30) ??
    candidates.find((c) => c.distinctCount >= 2) ??
    candidates[0]
  );
}

function pickMetric(dataset: Dataset): DatasetColumn | undefined {
  const numeric = dataset.columns.filter((c) => c.kind === 'numeric');
  // Skip identifier-like columns (a different value in every row) when there is a better choice.
  return numeric.find((c) => c.distinctCount < dataset.rows.length) ?? numeric[0];
}

function pickBin(column: DatasetColumn): DateBin {
  const first = Number(String(column.min ?? '').slice(0, 4));
  const last = Number(String(column.max ?? '').slice(0, 4));
  if (last - first >= 3) return 'year';
  const months = (last - first) * 12 + Number(String(column.max ?? '').slice(5, 7)) - Number(String(column.min ?? '').slice(5, 7));
  return months >= 2 ? 'month' : 'day';
}

function buildPatterns(dataset: Dataset): Pattern[] {
  const group = pickGroupColumn(dataset);
  const metric = pickMetric(dataset);
  const date = dataset.columns.find((c) => c.kind === 'date');
  const needsGroup = group ? null : 'needs a text or yes/no column to group by';
  const needsMetric = metric ? null : 'needs a numeric column';
  const needsDate = date ? null : 'needs a date column';

  return [
    {
      id: 'compare',
      title: 'Compare a value across groups',
      description: group && metric ? `Average of ${metric.name} for each ${group.name}` : 'Average of a number for each group',
      unavailable: needsGroup ?? needsMetric,
      apply: () => ({
        dimensions: [{ column: group!.name }],
        measures: [{ fn: 'avg', column: metric!.name }],
        sort: { key: `avg(${metric!.name})`, direction: 'desc' },
        limit: undefined,
      }),
    },
    {
      id: 'time',
      title: 'See how a value changes over time',
      description: date && metric ? `Sum of ${metric.name} per period of ${date.name}` : 'A number summed per period',
      unavailable: needsDate ?? needsMetric,
      apply: () => ({
        dimensions: [{ column: date!.name, bin: pickBin(date!) }],
        measures: [{ fn: 'sum', column: metric!.name }],
        sort: undefined,
        limit: undefined,
      }),
    },
    {
      id: 'top',
      title: 'Rank the top groups',
      description: group && metric ? `Top 10 ${group.name} by sum of ${metric.name}` : 'Top 10 groups by a total',
      unavailable: needsGroup ?? needsMetric,
      apply: () => ({
        dimensions: [{ column: group!.name }],
        measures: [{ fn: 'sum', column: metric!.name }],
        sort: { key: `sum(${metric!.name})`, direction: 'desc' },
        limit: 10,
      }),
    },
    {
      id: 'count',
      title: 'Count records per group',
      description: group ? `Number of rows for each ${group.name}` : 'Number of rows for each group',
      unavailable: needsGroup,
      apply: () => ({
        dimensions: [{ column: group!.name }],
        measures: [{ fn: 'count' }],
        sort: { key: 'count', direction: 'desc' },
        limit: undefined,
      }),
    },
    {
      id: 'totals',
      title: 'Summarize everything',
      description: metric ? `Row count, sum and average of ${metric.name}` : 'Row count',
      unavailable: null,
      apply: () => ({
        dimensions: [],
        measures: metric
          ? [{ fn: 'count' }, { fn: 'sum', column: metric.name }, { fn: 'avg', column: metric.name }]
          : [{ fn: 'count' }],
        sort: undefined,
        limit: undefined,
      }),
    },
  ];
}

// -------------------------------------------------------------------------------- tab

export const BuildTab: React.FC = () => {
  const { activeDataset: dataset, draftSpec, setDraftSpec, runDraftAnalysis, analysisError, setWorkspaceTab } = useApp();
  if (!dataset) return null;

  const commit = (next: AnalysisSpec) => {
    const keys = new Set([...next.dimensions.map(dimensionKey), ...next.measures.map(measureKey)]);
    setDraftSpec({ ...next, sort: next.sort && keys.has(next.sort.key) ? next.sort : undefined });
  };
  const patch = (changes: Partial<AnalysisSpec>) => commit({ ...draftSpec, ...changes });

  const errors = validateSpec(dataset, draftSpec);
  const sortKeys = [...draftSpec.dimensions.map(dimensionKey), ...draftSpec.measures.map(measureKey)];
  const patterns = buildPatterns(dataset);

  const setDimension = (index: number, dimension: Dimension) =>
    patch({ dimensions: draftSpec.dimensions.map((d, i) => (i === index ? dimension : d)) });
  const setMeasure = (index: number, measure: Measure) =>
    patch({ measures: draftSpec.measures.map((m, i) => (i === index ? measure : m)) });

  const run = () => {
    if (runDraftAnalysis()) setWorkspaceTab('results');
  };

  return (
    <div className="space-y-4">
      <Notice>
        Quaero Web does not interpret free-text questions in the browser. Build the question from the columns of
        your file: pick a pattern or choose the grouping, the calculation and the order yourself.
      </Notice>

      <Card title="1. Your question (optional)" subtitle="Saved in the analysis report in your own words. It is not interpreted.">
        <Field label="Question" htmlFor="analysis-question">
          <textarea
            id="analysis-question"
            rows={2}
            maxLength={500}
            className={inputClass}
            value={draftSpec.question ?? ''}
            placeholder="For example: which region sold the most last quarter?"
            onChange={(e) => patch({ question: e.target.value })}
          />
        </Field>
      </Card>

      <Card title="2. Start from a pattern" subtitle="Each pattern uses columns that exist in your file. You can change everything afterwards.">
        <ul className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          {patterns.map((pattern) => (
            <li key={pattern.id}>
              <button
                type="button"
                disabled={pattern.unavailable !== null}
                onClick={() => patch(pattern.apply())}
                className="flex h-full w-full flex-col items-start rounded-xl border border-slate-200 p-3 text-left transition hover:border-sky-400 hover:bg-sky-50/50 disabled:cursor-not-allowed disabled:opacity-60 dark:border-slate-800 dark:hover:bg-sky-950/20"
              >
                <span className="text-xs font-bold text-slate-900 dark:text-slate-100">{pattern.title}</span>
                <span className="mt-0.5 text-[11px] text-slate-600 dark:text-slate-300">{pattern.description}</span>
                {pattern.unavailable && (
                  <span className="mt-1 text-[11px] text-amber-700 dark:text-amber-300">
                    Not available: this file {pattern.unavailable}.
                  </span>
                )}
              </button>
            </li>
          ))}
        </ul>
      </Card>

      <Card title="3. Group by" subtitle={`Optional. Up to ${MAX_DIMENSIONS} columns; dates can be grouped per day, month or year.`}>
        {draftSpec.dimensions.length === 0 && (
          <p className="mb-2 text-xs text-slate-500 dark:text-slate-400">No grouping: you will get one row of totals.</p>
        )}
        <ul className="space-y-2">
          {draftSpec.dimensions.map((dimension, index) => {
            const column = dataset.columns.find((c) => c.name === dimension.column);
            return (
              <li key={index} className="flex flex-wrap items-end gap-2">
                <Field label={`Group ${index + 1}: column`} htmlFor={`group-${index}-column`}>
                  <select
                    id={`group-${index}-column`}
                    className={`${inputClass} w-auto min-w-40`}
                    value={dimension.column}
                    onChange={(e) => {
                      const next = dataset.columns.find((c) => c.name === e.target.value);
                      setDimension(index, { column: e.target.value, bin: next?.kind === 'date' ? 'month' : undefined });
                    }}
                  >
                    {dataset.columns.map((c) => (
                      <option key={c.name} value={c.name}>
                        {c.name} ({c.kind})
                      </option>
                    ))}
                  </select>
                </Field>
                {column?.kind === 'date' && (
                  <Field label="Date detail" htmlFor={`group-${index}-bin`}>
                    <select
                      id={`group-${index}-bin`}
                      className={`${inputClass} w-auto`}
                      value={dimension.bin ?? ''}
                      onChange={(e) => setDimension(index, { column: dimension.column, bin: (e.target.value || undefined) as DateBin | undefined })}
                    >
                      {BINS.map((b) => (
                        <option key={b.value} value={b.value}>
                          {b.label}
                        </option>
                      ))}
                    </select>
                  </Field>
                )}
                <button
                  type="button"
                  aria-label={`Remove group ${index + 1}`}
                  className="rounded-lg p-1.5 text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800"
                  onClick={() => patch({ dimensions: draftSpec.dimensions.filter((_, i) => i !== index) })}
                >
                  <X className="h-4 w-4" />
                </button>
              </li>
            );
          })}
        </ul>
        <button
          type="button"
          className={`${secondaryButton} mt-3`}
          disabled={draftSpec.dimensions.length >= MAX_DIMENSIONS}
          onClick={() => {
            const column = pickGroupColumn(dataset) ?? dataset.columns[0];
            patch({ dimensions: [...draftSpec.dimensions, { column: column.name, bin: column.kind === 'date' ? 'month' : undefined }] });
          }}
        >
          Add grouping
        </button>
      </Card>

      <Card title="4. Calculate" subtitle={`Up to ${MAX_MEASURES} calculations. Missing values are ignored.`}>
        <ul className="space-y-2">
          {draftSpec.measures.map((measure, index) => {
            const options = columnsFor(dataset, measure.fn);
            return (
              <li key={index} className="flex flex-wrap items-end gap-2">
                <Field label={`Calculation ${index + 1}`} htmlFor={`measure-${index}-fn`}>
                  <select
                    id={`measure-${index}-fn`}
                    className={`${inputClass} w-auto`}
                    value={measure.fn}
                    onChange={(e) => {
                      const fn = e.target.value as AggregateFn;
                      const allowed = columnsFor(dataset, fn);
                      const keep = measure.column && allowed.some((c) => c.name === measure.column) ? measure.column : undefined;
                      setMeasure(index, { fn, column: fn === 'count' ? keep : (keep ?? allowed[0]?.name) });
                    }}
                  >
                    {FUNCTIONS.map((f) => (
                      <option key={f.fn} value={f.fn}>
                        {f.label}
                      </option>
                    ))}
                  </select>
                </Field>
                <Field label="of column" htmlFor={`measure-${index}-column`}>
                  <select
                    id={`measure-${index}-column`}
                    className={`${inputClass} w-auto min-w-40`}
                    value={measure.column ?? ''}
                    onChange={(e) => setMeasure(index, { fn: measure.fn, column: e.target.value || undefined })}
                  >
                    {measure.fn === 'count' && <option value="">(all rows)</option>}
                    {options.map((c) => (
                      <option key={c.name} value={c.name}>
                        {c.name}
                      </option>
                    ))}
                  </select>
                </Field>
                <button
                  type="button"
                  aria-label={`Remove calculation ${index + 1}`}
                  className="rounded-lg p-1.5 text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800"
                  onClick={() => patch({ measures: draftSpec.measures.filter((_, i) => i !== index) })}
                >
                  <X className="h-4 w-4" />
                </button>
              </li>
            );
          })}
        </ul>
        <button
          type="button"
          className={`${secondaryButton} mt-3`}
          disabled={draftSpec.measures.length >= MAX_MEASURES}
          onClick={() => {
            const metric = pickMetric(dataset);
            const used = new Set(draftSpec.measures.map(measureKey));
            const candidate: Measure = metric ? { fn: 'sum', column: metric.name } : { fn: 'count' };
            patch({ measures: [...draftSpec.measures, used.has(measureKey(candidate)) ? { fn: 'avg', column: metric?.name } : candidate] });
          }}
        >
          Add calculation
        </button>
      </Card>

      <Card title="5. Order and size">
        <div className="flex flex-wrap items-end gap-3">
          <Field label="Sort by" htmlFor="sort-key">
            <select
              id="sort-key"
              className={`${inputClass} w-auto min-w-44`}
              value={draftSpec.sort?.key ?? ''}
              onChange={(e) => patch({ sort: e.target.value ? { key: e.target.value, direction: draftSpec.sort?.direction ?? 'desc' } : undefined })}
            >
              <option value="">Default order</option>
              {sortKeys.map((key) => (
                <option key={key} value={key}>
                  {key}
                </option>
              ))}
            </select>
          </Field>
          <Field label="Direction" htmlFor="sort-direction">
            <select
              id="sort-direction"
              className={`${inputClass} w-auto`}
              disabled={!draftSpec.sort}
              value={draftSpec.sort?.direction ?? 'desc'}
              onChange={(e) => draftSpec.sort && patch({ sort: { key: draftSpec.sort.key, direction: e.target.value as 'asc' | 'desc' } })}
            >
              <option value="desc">Highest first</option>
              <option value="asc">Lowest first</option>
            </select>
          </Field>
          <Field label="Show only the top" htmlFor="result-limit" hint={`1 to ${MAX_RESULT_ROWS}; empty shows every group`}>
            <input
              id="result-limit"
              type="number"
              min={1}
              max={MAX_RESULT_ROWS}
              className={`${inputClass} w-28`}
              value={draftSpec.limit ?? ''}
              onChange={(e) => patch({ limit: e.target.value === '' ? undefined : Number(e.target.value) })}
            />
          </Field>
        </div>
      </Card>

      <Card title="6. Filters">
        {draftSpec.filters.length === 0 ? (
          <p className="text-xs text-slate-500 dark:text-slate-400">No filters: every row is used.</p>
        ) : (
          <p className="text-xs text-slate-700 dark:text-slate-200">{draftSpec.filters.length} filter(s) set in Explore &amp; Filter.</p>
        )}
        <button type="button" className={`${secondaryButton} mt-2`} onClick={() => setWorkspaceTab('explore')}>
          Edit filters
        </button>
      </Card>

      <Card title="This analysis will calculate">
        <p className="text-sm font-semibold text-slate-900 dark:text-white" aria-live="polite">
          {specToSentence(draftSpec)}
        </p>
        {errors.length > 0 && (
          <ul className="mt-3 list-disc space-y-1 pl-5 text-xs text-amber-800 dark:text-amber-300">
            {errors.map((error) => (
              <li key={error}>{error}</li>
            ))}
          </ul>
        )}
        {analysisError && errors.length === 0 && (
          <div className="mt-3">
            <Notice tone="error">{analysisError}</Notice>
          </div>
        )}
        <div className="mt-4">
          <button type="button" className={primaryButton} disabled={errors.length > 0} onClick={run}>
            Run analysis
          </button>
        </div>
      </Card>
    </div>
  );
};
