import React, { useMemo } from 'react';
import { formatCell, truncate } from '../../analysis/format';
import { applyFilters, describeFilter, specToSentence, validateFilter } from '../../analysis/query';
import { qualityFindings, summarizeDataset } from '../../analysis/summary';
import type { ColumnKind } from '../../analysis/types';
import { useApp, type WorkspaceTab } from '../../context/AppContext';
import { Card, KindBadge, Notice, Stat, primaryButton, secondaryButton } from '../analysis/ui';
import { UnavailableCard } from './Unavailable';
import { WorkspaceHeader } from './WorkspaceHeader';

const KIND_ORDER: ColumnKind[] = ['numeric', 'categorical', 'date', 'boolean'];

/** Overview Dashboard while the Browser Workspace is active: real numbers from the loaded rows only. */
export const WorkspaceOverview: React.FC = () => {
  const {
    activeDataset: dataset,
    activeDatasetProfile: profile,
    draftSpec,
    appliedSpec,
    analysisResult: result,
    setActiveTab,
    setWorkspaceTab,
  } = useApp();

  const summary = useMemo(() => (dataset && profile ? summarizeDataset(dataset, profile) : null), [dataset, profile]);
  const findings = useMemo(() => (dataset && profile ? qualityFindings(dataset, profile) : []), [dataset, profile]);
  const usableFilters = useMemo(
    () => (dataset ? draftSpec.filters.filter((f) => validateFilter(dataset, f).length === 0) : []),
    [dataset, draftSpec.filters],
  );
  const matching = useMemo(() => (dataset ? applyFilters(dataset.rows, usableFilters).length : 0), [dataset, usableFilters]);

  if (!dataset || !profile || !summary) return null;

  const open = (tab: WorkspaceTab) => {
    setWorkspaceTab(tab);
    setActiveTab('analyze');
  };
  const kindHint = KIND_ORDER.filter((k) => summary.kindCounts[k] > 0)
    .map((k) => `${summary.kindCounts[k]} ${k}`)
    .join(' · ');

  return (
    <div className="space-y-6">
      <WorkspaceHeader title="Overview: Browser Workspace" />

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <Stat label="Rows" value={summary.rows.toLocaleString('en-US')} hint={`${summary.cells.toLocaleString('en-US')} cells`} />
        <Stat label="Columns" value={summary.columns} hint={kindHint} />
        <Stat
          label="Completeness"
          value={`${summary.completenessPct}%`}
          hint={`${summary.missingCells.toLocaleString('en-US')} missing cell${summary.missingCells === 1 ? '' : 's'}`}
        />
        <Stat
          label="Data Quality score"
          value={profile.overallDqScore}
          hint={`${summary.duplicateRows.toLocaleString('en-US')} duplicate row${summary.duplicateRows === 1 ? '' : 's'}`}
        />
      </div>

      <Card title="Schema" subtitle="Detected from the values in your file.">
        <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-50 font-semibold text-slate-600 dark:bg-slate-800/60 dark:text-slate-300">
              <tr>
                <th className="px-3 py-2">Column</th>
                <th className="px-3 py-2">Type</th>
                <th className="px-3 py-2">Missing</th>
                <th className="px-3 py-2">Distinct</th>
              </tr>
            </thead>
            <tbody>
              {summary.columnSummaries.map((c) => (
                <tr key={c.name} className="border-t border-slate-100 dark:border-slate-800">
                  <td className="px-3 py-1.5 font-mono font-semibold text-slate-900 dark:text-slate-100">{c.name}</td>
                  <td className="px-3 py-1.5">
                    <KindBadge kind={c.kind} />
                  </td>
                  <td className="px-3 py-1.5">{c.missing === 0 ? 'none' : `${c.missing.toLocaleString('en-US')} (${c.missingPct}%)`}</td>
                  <td className="px-3 py-1.5">{c.distinct.toLocaleString('en-US')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      <Card title="Data quality" subtitle="Findings computed from your data; no thresholds are applied.">
        {findings.length === 0 ? (
          <Notice>No missing values, duplicates, unreadable values or outliers were found.</Notice>
        ) : (
          <ul className="list-disc space-y-1 pl-5 text-xs text-slate-700 dark:text-slate-200">
            {findings.slice(0, 6).map((text) => (
              <li key={text}>{text}</li>
            ))}
          </ul>
        )}
        <div className="mt-3 flex flex-wrap items-center gap-3">
          <button type="button" className={secondaryButton} onClick={() => open('quality')}>
            Open Data Quality
          </button>
          {findings.length > 6 && (
            <span className="text-[11px] text-slate-500 dark:text-slate-400">{findings.length - 6} more in Data Quality.</span>
          )}
        </div>
      </Card>

      <Card title="Filters and analysis" subtitle="What you have set up in Analyze Dataset. It is kept while you look at other screens.">
        <div className="space-y-4 text-xs text-slate-700 dark:text-slate-200">
          <div>
            <div className="font-bold text-slate-900 dark:text-white">Filters</div>
            {usableFilters.length === 0 ? (
              <p className="mt-1">No filters: every row is used.</p>
            ) : (
              <>
                <ul className="mt-1 list-disc space-y-0.5 pl-5">
                  {usableFilters.map((f) => (
                    <li key={describeFilter(f)}>{describeFilter(f)}</li>
                  ))}
                </ul>
                <p className="mt-1">
                  {matching.toLocaleString('en-US')} of {summary.rows.toLocaleString('en-US')} rows match.
                </p>
              </>
            )}
          </div>

          <div>
            <div className="font-bold text-slate-900 dark:text-white">Latest analysis</div>
            {!result || !appliedSpec ? (
              <p className="mt-1">No analysis has been run yet.</p>
            ) : (
              <div className="mt-1 space-y-2">
                <p className="font-semibold">{specToSentence(appliedSpec)}</p>
                <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
                  <table className="w-full text-left text-xs">
                    <thead className="bg-slate-50 font-semibold text-slate-600 dark:bg-slate-800/60 dark:text-slate-300">
                      <tr>
                        {result.columns.map((c) => (
                          <th key={c.key} className="whitespace-nowrap px-3 py-2">
                            {c.label}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.rows.slice(0, 5).map((row, i) => (
                        <tr key={i} className="border-t border-slate-100 dark:border-slate-800">
                          {result.columns.map((c) => (
                            <td key={c.key} className="whitespace-nowrap px-3 py-1.5">
                              {truncate(formatCell(row[c.key]), 40)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <p className="text-[11px] text-slate-500 dark:text-slate-400">
                  Calculated on {result.filteredRows.toLocaleString('en-US')} of {result.inputRows.toLocaleString('en-US')} rows.
                  {result.totalGroups > 5 ? ` First 5 of ${result.totalGroups.toLocaleString('en-US')} groups shown.` : ''}
                </p>
              </div>
            )}
          </div>

          <div className="flex flex-wrap gap-2">
            <button type="button" className={primaryButton} onClick={() => open(result ? 'results' : 'build')}>
              {result ? 'Open results' : 'Build an analysis'}
            </button>
            <button type="button" className={secondaryButton} onClick={() => open('explore')}>
              Explore &amp; Filter
            </button>
            <button type="button" className={secondaryButton} onClick={() => open('export')}>
              Export
            </button>
          </div>
        </div>
      </Card>

      <UnavailableCard />
    </div>
  );
};
