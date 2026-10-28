import React, { useMemo, useState } from 'react';
import { BarChart, LineChart, type ChartDatum } from '../../components/charts/Charts';
import { formatCell } from '../../analysis/format';
import { specToSentence } from '../../analysis/query';
import type { Cell } from '../../analysis/types';
import { useApp } from '../../context/AppContext';
import { Card, Field, Notice, Stat, inputClass, primaryButton } from './ui';

type ChartKind = 'bar' | 'line' | 'table';

const TABLE_ROW_LIMIT = 200;
const BAR_LIMIT = 30;
const LINE_LIMIT = 400;

export const ResultsTab: React.FC = () => {
  const { analysisResult: result, appliedSpec, setWorkspaceTab } = useApp();
  const [chartKind, setChartKind] = useState<ChartKind | ''>('');
  const [measureKey, setMeasureKey] = useState('');
  const [tableSort, setTableSort] = useState<{ key: string; direction: 'asc' | 'desc' } | null>(null);

  const rows = useMemo(() => {
    if (!result) return [];
    if (!tableSort) return result.rows;
    const direction = tableSort.direction === 'asc' ? 1 : -1;
    return [...result.rows].sort((a, b) => {
      const left = a[tableSort.key];
      const right = b[tableSort.key];
      if (left === null && right === null) return 0;
      if (left === null) return 1;
      if (right === null) return -1;
      if (typeof left === 'number' && typeof right === 'number') return (left - right) * direction;
      return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: 'base' }) * direction;
    });
  }, [result, tableSort]);

  if (!result || !appliedSpec) {
    return (
      <Card title="No results yet">
        <p className="text-xs text-slate-600 dark:text-slate-300">
          Build an analysis first: choose a pattern or the grouping and calculation, then press Run analysis.
        </p>
        <button type="button" className={`${primaryButton} mt-3`} onClick={() => setWorkspaceTab('build')}>
          Go to Build Analysis
        </button>
      </Card>
    );
  }

  const dimensions = result.columns.filter((c) => c.role === 'dimension');
  const measures = result.columns.filter((c) => c.role === 'measure');
  const charted = measures.find((m) => m.key === measureKey) ?? measures[0];
  const chartable = charted.kind === 'numeric';

  const available: ChartKind[] =
    dimensions.length === 0 ? ['table'] : dimensions.length === 1 && dimensions[0].kind === 'date' ? ['line', 'bar', 'table'] : ['bar', 'table'];
  const kind: ChartKind = chartKind && available.includes(chartKind) ? chartKind : available[0];

  const labelOf = (row: Record<string, Cell>) => dimensions.map((d) => formatCell(row[d.key])).join(' · ');
  const toDatum = (row: Record<string, Cell>): ChartDatum => ({
    label: labelOf(row),
    value: typeof row[charted.key] === 'number' ? (row[charted.key] as number) : null,
  });
  const barData = result.rows.slice(0, BAR_LIMIT).map(toDatum);
  const lineData = result.rows.slice(0, LINE_LIMIT).map(toDatum);
  const chartTitle = `${charted.label} by ${dimensions.map((d) => d.label).join(' and ')}`;

  return (
    <div className="space-y-4">
      <Card title="Analysis">
        <p className="text-sm font-semibold text-slate-900 dark:text-white">{specToSentence(appliedSpec)}</p>
        {appliedSpec.question?.trim() && (
          <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">Your question: “{appliedSpec.question.trim()}”</p>
        )}
      </Card>

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        <Stat label="Rows analyzed" value={result.filteredRows.toLocaleString('en-US')} hint={`of ${result.inputRows.toLocaleString('en-US')} in the file`} />
        <Stat
          label={dimensions.length ? 'Groups' : 'Result rows'}
          value={result.totalGroups.toLocaleString('en-US')}
          hint={result.truncated ? `showing the top ${result.rows.length}` : undefined}
        />
        <Stat label="Calculations" value={measures.length} />
      </div>

      {result.warnings.map((warning) => (
        <Notice key={warning} tone="warning">
          {warning}
        </Notice>
      ))}

      {dimensions.length === 0 && result.rows[0] && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          {measures.map((m) => (
            <Stat key={m.key} label={m.label} value={formatCell(result.rows[0][m.key])} />
          ))}
        </div>
      )}

      {result.rows.length === 0 ? (
        <Notice tone="warning">There are no groups to show. Check the filters in Explore &amp; Filter.</Notice>
      ) : (
        dimensions.length > 0 && (
          <Card title="Chart">
            <div className="mb-3 flex flex-wrap items-end gap-3">
              {measures.length > 1 && (
                <Field label="Calculation to draw" htmlFor="chart-measure">
                  <select
                    id="chart-measure"
                    className={`${inputClass} w-auto`}
                    value={charted.key}
                    onChange={(e) => setMeasureKey(e.target.value)}
                  >
                    {measures.map((m) => (
                      <option key={m.key} value={m.key}>
                        {m.label}
                      </option>
                    ))}
                  </select>
                </Field>
              )}
              <Field label="Chart type" htmlFor="chart-kind">
                <select id="chart-kind" className={`${inputClass} w-auto`} value={kind} onChange={(e) => setChartKind(e.target.value as ChartKind)}>
                  {available.map((k) => (
                    <option key={k} value={k}>
                      {k === 'bar' ? 'Bars' : k === 'line' ? 'Line over time' : 'Table only'}
                    </option>
                  ))}
                </select>
              </Field>
            </div>
            {kind === 'table' ? (
              <p className="text-xs text-slate-500 dark:text-slate-400">Showing the table below.</p>
            ) : !chartable ? (
              <Notice>{charted.label} is a date, so it is shown in the table and not drawn.</Notice>
            ) : kind === 'line' ? (
              <>
                <LineChart data={lineData} title={chartTitle} />
                {result.rows.length > LINE_LIMIT && <p className="mt-1 text-[11px] text-slate-500">Drawing the first {LINE_LIMIT} of {result.rows.length} points; the table and the export have all of them.</p>}
              </>
            ) : (
              <>
                <BarChart data={barData} title={chartTitle} />
                {result.rows.length > BAR_LIMIT && <p className="mt-1 text-[11px] text-slate-500">Drawing the first {BAR_LIMIT} of {result.rows.length} groups; the table and the export have all of them.</p>}
              </>
            )}
          </Card>
        )
      )}

      <Card title="Table" subtitle="Click a column heading to sort the table.">
        <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-50 font-semibold text-slate-600 dark:bg-slate-800/60 dark:text-slate-300">
              <tr>
                {result.columns.map((c) => (
                  <th
                    key={c.key}
                    className="whitespace-nowrap px-3 py-2"
                    aria-sort={tableSort?.key === c.key ? (tableSort.direction === 'asc' ? 'ascending' : 'descending') : 'none'}
                  >
                    <button
                      type="button"
                      className="font-semibold hover:underline"
                      onClick={() =>
                        setTableSort(
                          tableSort?.key === c.key && tableSort.direction === 'desc'
                            ? { key: c.key, direction: 'asc' }
                            : { key: c.key, direction: 'desc' },
                        )
                      }
                    >
                      {c.label}
                      {tableSort?.key === c.key ? (tableSort.direction === 'asc' ? ' ▲' : ' ▼') : ''}
                    </button>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, TABLE_ROW_LIMIT).map((row, index) => (
                <tr key={index} className="border-t border-slate-100 dark:border-slate-800">
                  {result.columns.map((c) => (
                    <td key={c.key} className={`whitespace-nowrap px-3 py-1.5 ${row[c.key] === null ? 'text-slate-400' : ''}`}>
                      {formatCell(row[c.key])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {rows.length > TABLE_ROW_LIMIT && (
          <p className="mt-2 text-[11px] text-slate-500 dark:text-slate-400">
            Showing the first {TABLE_ROW_LIMIT} of {rows.length.toLocaleString('en-US')} rows. The export contains all of them.
          </p>
        )}
      </Card>
    </div>
  );
};
