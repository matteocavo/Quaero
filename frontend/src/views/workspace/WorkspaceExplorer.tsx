import React, { useMemo } from 'react';
import { formatNumber, truncate } from '../../analysis/format';
import { summarizeDataset } from '../../analysis/summary';
import { useApp } from '../../context/AppContext';
import { Card, KindBadge, Stat, secondaryButton } from '../analysis/ui';
import { UnavailableCard } from './Unavailable';
import { WorkspaceHeader } from './WorkspaceHeader';

const DELIMITER_NAMES: Record<string, string> = { ',': 'comma', ';': 'semicolon', '\t': 'tab', '|': 'pipe' };

const Row: React.FC<{ label: string; children: React.ReactNode }> = ({ label, children }) => (
  <div className="flex flex-col gap-0.5 border-b border-slate-100 py-2 last:border-b-0 dark:border-slate-800 sm:flex-row sm:gap-4">
    <dt className="w-40 shrink-0 text-xs font-semibold text-slate-500 dark:text-slate-400">{label}</dt>
    <dd className="min-w-0 break-words text-xs text-slate-900 dark:text-slate-100">{children}</dd>
  </div>
);

/** Project Explorer while the Browser Workspace is active: where the data came from and what it contains. */
export const WorkspaceExplorer: React.FC = () => {
  const {
    activeDataset: dataset,
    activeDatasetProfile: profile,
    analysisHistory,
    setDraftSpec,
    setActiveTab,
    setWorkspaceTab,
  } = useApp();
  const summary = useMemo(() => (dataset && profile ? summarizeDataset(dataset, profile) : null), [dataset, profile]);
  if (!dataset || !profile || !summary) return null;

  const loadRun = (index: number) => {
    setDraftSpec(analysisHistory[index].spec);
    setWorkspaceTab('build');
    setActiveTab('analyze');
  };

  return (
    <div className="space-y-6">
      <WorkspaceHeader title="Project Explorer: Browser Workspace" />

      <Card title="Origin">
        <dl>
          <Row label="File name">
            <span className="font-mono">{dataset.fileName}</span>
          </Row>
          <Row label="Source">{dataset.origin === 'sample' ? 'Bundled sample, loaded into this tab' : 'File you selected on this device'}</Row>
          <Row label="Format">Delimited text, {DELIMITER_NAMES[profile.delimiter] ?? JSON.stringify(profile.delimiter)}-separated</Row>
          <Row label="Stored">In the memory of this browser tab only. Not saved, not uploaded. Reloading the page removes it.</Row>
          <Row label="Python pipeline">Never ran for this file: no marts, lineage, run history or anomaly framing exist.</Row>
        </dl>
      </Card>

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <Stat label="Rows" value={summary.rows.toLocaleString('en-US')} />
        <Stat label="Columns" value={summary.columns} />
        <Stat label="Cells" value={summary.cells.toLocaleString('en-US')} />
        <Stat label="Missing cells" value={summary.missingCells.toLocaleString('en-US')} hint={`${summary.completenessPct}% complete`} />
      </div>

      <Card title="Columns and statistics" subtitle="Numeric columns show min, mean and max; text and boolean columns their most frequent values.">
        <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-50 font-semibold text-slate-600 dark:bg-slate-800/60 dark:text-slate-300">
              <tr>
                <th className="px-3 py-2">Column</th>
                <th className="px-3 py-2">Type</th>
                <th className="px-3 py-2">Missing</th>
                <th className="px-3 py-2">Distinct</th>
                <th className="px-3 py-2">Statistics</th>
              </tr>
            </thead>
            <tbody>
              {summary.columnSummaries.map((c) => (
                <tr key={c.name} className="border-t border-slate-100 align-top dark:border-slate-800">
                  <td className="px-3 py-1.5 font-mono font-semibold text-slate-900 dark:text-slate-100">{c.name}</td>
                  <td className="px-3 py-1.5">
                    <KindBadge kind={c.kind} />
                  </td>
                  <td className="whitespace-nowrap px-3 py-1.5">{c.missing === 0 ? 'none' : `${c.missing.toLocaleString('en-US')} (${c.missingPct}%)`}</td>
                  <td className="px-3 py-1.5">{c.distinct.toLocaleString('en-US')}</td>
                  <td className="px-3 py-1.5">
                    {c.kind === 'numeric' && c.mean !== undefined
                      ? `min ${formatNumber(Number(c.min))} · mean ${formatNumber(c.mean)} · max ${formatNumber(Number(c.max))}`
                      : c.kind === 'date' && c.min !== undefined
                        ? `${c.min} to ${c.max}`
                        : c.top && c.top.length > 0
                          ? c.top.map((t) => `${truncate(t.value, 24)} (${t.count.toLocaleString('en-US')})`).join(', ')
                          : 'no values'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      <Card title="Analyses created" subtitle="Run in this session on this dataset. They disappear with the dataset.">
        {analysisHistory.length === 0 ? (
          <p className="text-xs text-slate-600 dark:text-slate-300">No analysis has been run yet.</p>
        ) : (
          <ol className="space-y-2">
            {analysisHistory.map((run, index) => (
              <li
                key={run.id}
                className="flex flex-col gap-2 rounded-lg border border-slate-200 p-2.5 text-xs dark:border-slate-800 sm:flex-row sm:items-center sm:justify-between"
              >
                <div className="min-w-0">
                  <div className="break-words font-semibold text-slate-900 dark:text-white">
                    {run.id}. {run.sentence}
                  </div>
                  <div className="text-[11px] text-slate-500 dark:text-slate-400">
                    {run.filteredRows.toLocaleString('en-US')} of {run.inputRows.toLocaleString('en-US')} rows, {run.groups.toLocaleString('en-US')} result row
                    {run.groups === 1 ? '' : 's'}, {run.spec.filters.length} filter{run.spec.filters.length === 1 ? '' : 's'}
                  </div>
                </div>
                <button type="button" className={`${secondaryButton} shrink-0`} onClick={() => loadRun(index)}>
                  Open in Build Analysis
                </button>
              </li>
            ))}
          </ol>
        )}
      </Card>

      <UnavailableCard />
    </div>
  );
};
