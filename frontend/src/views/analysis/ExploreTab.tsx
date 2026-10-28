import React, { useMemo, useState } from 'react';
import { X } from 'lucide-react';
import { BarChart, HistogramChart, LineChart } from '../../components/charts/Charts';
import { formatCell, formatNumber } from '../../analysis/format';
import { MAX_FILTERS } from '../../analysis/limits';
import { applyFilters, describeFilter, histogram, topValues, validateFilter } from '../../analysis/query';
import type { ColumnKind, Dataset, DatasetColumn, Filter, Row } from '../../analysis/types';
import { useApp } from '../../context/AppContext';
import { Card, Field, KindBadge, Notice, Stat, inputClass, primaryButton, secondaryButton } from './ui';

type FilterOp = Filter['op'];

const OPS_BY_KIND: Record<ColumnKind, { op: FilterOp; label: string }[]> = {
  categorical: [
    { op: 'in', label: 'is one of' },
    { op: 'contains', label: 'contains text' },
    { op: 'missing', label: 'is missing' },
    { op: 'present', label: 'has a value' },
  ],
  boolean: [
    { op: 'in', label: 'is one of' },
    { op: 'missing', label: 'is missing' },
    { op: 'present', label: 'has a value' },
  ],
  numeric: [
    { op: 'range', label: 'is between' },
    { op: 'in', label: 'is one of' },
    { op: 'missing', label: 'is missing' },
    { op: 'present', label: 'has a value' },
  ],
  date: [
    { op: 'dateRange', label: 'is between' },
    { op: 'missing', label: 'is missing' },
    { op: 'present', label: 'has a value' },
  ],
};

export function defaultFilter(column: DatasetColumn, op?: FilterOp): Filter {
  const chosen = op ?? OPS_BY_KIND[column.kind][0].op;
  switch (chosen) {
    case 'in':
      return { column: column.name, op: 'in', values: [] };
    case 'range':
      return { column: column.name, op: 'range' };
    case 'dateRange':
      return { column: column.name, op: 'dateRange' };
    case 'contains':
      return { column: column.name, op: 'contains', text: '' };
    case 'missing':
      return { column: column.name, op: 'missing' };
    default:
      return { column: column.name, op: 'present' };
  }
}

// ------------------------------------------------------------------------ filter editor

const ValuePicker: React.FC<{
  dataset: Dataset;
  filter: Extract<Filter, { op: 'in' }>;
  onChange: (filter: Filter) => void;
  id: string;
}> = ({ dataset, filter, onChange, id }) => {
  const [search, setSearch] = useState('');
  const values = useMemo(() => topValues(dataset.rows, filter.column, 200), [dataset, filter.column]);
  const shown = values.filter((v) => v.value.toLowerCase().includes(search.trim().toLowerCase()));
  const toggle = (value: string) =>
    onChange({
      ...filter,
      values: filter.values.includes(value) ? filter.values.filter((v) => v !== value) : [...filter.values, value],
    });

  return (
    <div className="space-y-2">
      <label htmlFor={`${id}-search`} className="sr-only">
        Search values
      </label>
      <input
        id={`${id}-search`}
        className={inputClass}
        placeholder="Search values…"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
      />
      <ul className="max-h-40 space-y-1 overflow-y-auto rounded-lg border border-slate-200 p-2 dark:border-slate-800">
        {shown.map((v) => (
          <li key={v.value}>
            <label className="flex cursor-pointer items-center justify-between gap-2 text-xs text-slate-800 dark:text-slate-100">
              <span className="flex min-w-0 items-center gap-2">
                <input type="checkbox" checked={filter.values.includes(v.value)} onChange={() => toggle(v.value)} />
                <span className="truncate">{v.value}</span>
              </span>
              <span className="shrink-0 text-slate-500 dark:text-slate-400">{v.count.toLocaleString('en-US')}</span>
            </label>
          </li>
        ))}
        {shown.length === 0 && <li className="text-xs text-slate-500">No value matches your search.</li>}
      </ul>
      {values.length === 200 && (
        <p className="text-[11px] text-slate-500 dark:text-slate-400">Showing the 200 most frequent values.</p>
      )}
    </div>
  );
};

const FilterEditor: React.FC<{
  index: number;
  dataset: Dataset;
  filter: Filter;
  onChange: (filter: Filter) => void;
  onRemove: () => void;
}> = ({ index, dataset, filter, onChange, onRemove }) => {
  const column = dataset.columns.find((c) => c.name === filter.column);
  const id = `filter-${index}`;
  const problems = validateFilter(dataset, filter);
  if (!column) return null;

  return (
    <li className="rounded-xl border border-slate-200 p-3 dark:border-slate-800">
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-mono text-xs font-bold text-slate-900 dark:text-slate-100">{column.name}</span>
        <KindBadge kind={column.kind} />
        <label htmlFor={`${id}-op`} className="sr-only">
          Condition for {column.name}
        </label>
        <select
          id={`${id}-op`}
          className={`${inputClass} w-auto`}
          value={filter.op}
          onChange={(e) => onChange(defaultFilter(column, e.target.value as FilterOp))}
        >
          {OPS_BY_KIND[column.kind].map((o) => (
            <option key={o.op} value={o.op}>
              {o.label}
            </option>
          ))}
        </select>
        <button
          type="button"
          aria-label={`Remove filter on ${column.name}`}
          className="ml-auto rounded-lg p-1 text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800"
          onClick={onRemove}
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="mt-2">
        {filter.op === 'in' && <ValuePicker dataset={dataset} filter={filter} onChange={onChange} id={id} />}
        {filter.op === 'range' && (
          <div className="grid grid-cols-2 gap-2">
            <Field label="Minimum" htmlFor={`${id}-min`} hint={column.min !== undefined ? `smallest: ${column.min}` : undefined}>
              <input
                id={`${id}-min`}
                type="number"
                step="any"
                className={inputClass}
                value={filter.min ?? ''}
                onChange={(e) => onChange({ ...filter, min: e.target.value === '' ? undefined : Number(e.target.value) })}
              />
            </Field>
            <Field label="Maximum" htmlFor={`${id}-max`} hint={column.max !== undefined ? `largest: ${column.max}` : undefined}>
              <input
                id={`${id}-max`}
                type="number"
                step="any"
                className={inputClass}
                value={filter.max ?? ''}
                onChange={(e) => onChange({ ...filter, max: e.target.value === '' ? undefined : Number(e.target.value) })}
              />
            </Field>
          </div>
        )}
        {filter.op === 'dateRange' && (
          <div className="grid grid-cols-2 gap-2">
            <Field label="From" htmlFor={`${id}-from`} hint={column.min ? `earliest: ${String(column.min).slice(0, 10)}` : undefined}>
              <input
                id={`${id}-from`}
                type="date"
                className={inputClass}
                value={filter.from ?? ''}
                onChange={(e) => onChange({ ...filter, from: e.target.value || undefined })}
              />
            </Field>
            <Field label="To" htmlFor={`${id}-to`} hint={column.max ? `latest: ${String(column.max).slice(0, 10)}` : undefined}>
              <input
                id={`${id}-to`}
                type="date"
                className={inputClass}
                value={filter.to ?? ''}
                onChange={(e) => onChange({ ...filter, to: e.target.value || undefined })}
              />
            </Field>
          </div>
        )}
        {filter.op === 'contains' && (
          <Field label="Text to look for (not case sensitive)" htmlFor={`${id}-text`}>
            <input
              id={`${id}-text`}
              className={inputClass}
              value={filter.text}
              onChange={(e) => onChange({ ...filter, text: e.target.value })}
            />
          </Field>
        )}
      </div>

      {problems.length > 0 && (
        <p className="mt-2 text-[11px] text-amber-700 dark:text-amber-300">Not applied yet: {problems[0]}</p>
      )}
    </li>
  );
};

// ------------------------------------------------------------------------- distribution

function monthCounts(rows: Row[], column: string) {
  const counts = new Map<string, number>();
  for (const row of rows) {
    const cell = row[column];
    if (typeof cell === 'string') counts.set(cell.slice(0, 7), (counts.get(cell.slice(0, 7)) ?? 0) + 1);
  }
  return [...counts.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([label, value]) => ({ label, value }));
}

const Distribution: React.FC<{ rows: Row[]; column: DatasetColumn }> = ({ rows, column }) => {
  if (rows.length === 0) return <Notice>No rows to show: the filters match nothing.</Notice>;
  if (column.kind === 'numeric') {
    const { bins, missing } = histogram(rows, column.name, 10);
    if (bins.length === 0) return <Notice>This column has no numeric values in the selected rows.</Notice>;
    return (
      <>
        <HistogramChart bins={bins} title={`Distribution of ${column.name} (${rows.length} rows)`} />
        {missing > 0 && <p className="mt-1 text-[11px] text-slate-500">{missing} rows have no value and are not drawn.</p>}
      </>
    );
  }
  if (column.kind === 'date') {
    const data = monthCounts(rows, column.name).slice(0, 400);
    if (data.length === 0) return <Notice>This column has no dates in the selected rows.</Notice>;
    return <LineChart data={data} title={`Rows per month by ${column.name}`} />;
  }
  const data = topValues(rows, column.name, 12).map((v) => ({ label: v.value, value: v.count }));
  return <BarChart data={data} title={`Most frequent values of ${column.name}`} />;
};

// ------------------------------------------------------------------------------- tab

export const ExploreTab: React.FC = () => {
  const { activeDataset: dataset, draftSpec, setDraftSpec } = useApp();
  const [newColumn, setNewColumn] = useState('');
  const [distColumn, setDistColumn] = useState('');

  const filters = draftSpec.filters;
  const usable = useMemo(
    () => (dataset ? filters.filter((f) => validateFilter(dataset, f).length === 0) : []),
    [dataset, filters],
  );
  const matching = useMemo(() => (dataset ? applyFilters(dataset.rows, usable) : []), [dataset, usable]);
  if (!dataset) return null;

  const setFilters = (next: Filter[]) => setDraftSpec({ ...draftSpec, filters: next });
  const addColumnName = newColumn && dataset.columns.some((c) => c.name === newColumn) ? newColumn : dataset.columns[0].name;
  const distName = distColumn && dataset.columns.some((c) => c.name === distColumn) ? distColumn : dataset.columns[0].name;
  const distribution = dataset.columns.find((c) => c.name === distName)!;
  const share = dataset.rows.length ? (matching.length / dataset.rows.length) * 100 : 0;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        <Stat label="Rows in the dataset" value={dataset.rows.length.toLocaleString('en-US')} />
        <Stat label="Rows that match" value={matching.length.toLocaleString('en-US')} hint={`${formatNumber(Number(share.toFixed(1)))}% of the rows`} />
        <Stat label="Filters applied" value={usable.length} hint={filters.length > usable.length ? `${filters.length - usable.length} incomplete` : 'all complete'} />
      </div>

      <Card
        title="Filters"
        subtitle="Rows must satisfy every filter. The same filters are used by Build Analysis and the filtered export."
      >
        <div className="flex flex-wrap items-end gap-2">
          <Field label="Column" htmlFor="new-filter-column">
            <select
              id="new-filter-column"
              className={`${inputClass} w-auto min-w-40`}
              value={addColumnName}
              onChange={(e) => setNewColumn(e.target.value)}
            >
              {dataset.columns.map((c) => (
                <option key={c.name} value={c.name}>
                  {c.name} ({c.kind})
                </option>
              ))}
            </select>
          </Field>
          <button
            type="button"
            className={primaryButton}
            disabled={filters.length >= MAX_FILTERS}
            onClick={() => {
              const column = dataset.columns.find((c) => c.name === addColumnName)!;
              setFilters([...filters, defaultFilter(column)]);
            }}
          >
            Add filter
          </button>
          {filters.length > 0 && (
            <button type="button" className={secondaryButton} onClick={() => setFilters([])}>
              Clear all filters
            </button>
          )}
        </div>

        {filters.length === 0 ? (
          <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">
            No filters: every row is used. Add one to narrow the data by values or by a range.
          </p>
        ) : (
          <ul className="mt-3 space-y-2">
            {filters.map((filter, index) => (
              <FilterEditor
                key={index}
                index={index}
                dataset={dataset}
                filter={filter}
                onChange={(next) => setFilters(filters.map((f, i) => (i === index ? next : f)))}
                onRemove={() => setFilters(filters.filter((_, i) => i !== index))}
              />
            ))}
          </ul>
        )}
        {usable.length > 0 && (
          <p className="mt-3 text-[11px] text-slate-500 dark:text-slate-400">
            Applied: {usable.map(describeFilter).join(' and ')}.
          </p>
        )}
      </Card>

      <Card title="Distribution of the matching rows">
        <Field label="Column" htmlFor="distribution-column">
          <select
            id="distribution-column"
            className={`${inputClass} w-auto min-w-40`}
            value={distName}
            onChange={(e) => setDistColumn(e.target.value)}
          >
            {dataset.columns.map((c) => (
              <option key={c.name} value={c.name}>
                {c.name} ({c.kind})
              </option>
            ))}
          </select>
        </Field>
        <div className="mt-3">
          <Distribution rows={matching} column={distribution} />
        </div>
      </Card>

      <Card title="Matching rows" subtitle={`First ${Math.min(25, matching.length)} of ${matching.length.toLocaleString('en-US')}.`}>
        {matching.length === 0 ? (
          <Notice tone="warning">No rows match the filters. Loosen or remove a filter.</Notice>
        ) : (
          <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-50 font-semibold text-slate-600 dark:bg-slate-800/60 dark:text-slate-300">
                <tr>
                  {dataset.columns.map((c) => (
                    <th key={c.name} className="whitespace-nowrap px-3 py-2">
                      {c.name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {matching.slice(0, 25).map((row, index) => (
                  <tr key={index} className="border-t border-slate-100 dark:border-slate-800">
                    {dataset.columns.map((c) => (
                      <td key={c.name} className={`whitespace-nowrap px-3 py-1.5 ${row[c.name] === null ? 'text-slate-400' : ''}`}>
                        {formatCell(row[c.name])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
};
