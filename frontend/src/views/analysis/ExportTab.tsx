import React, { useMemo, useState } from 'react';
import { Download } from 'lucide-react';
import { buildReport, filteredDatasetToCsv, resultToCsv } from '../../analysis/report';
import { applyFilters, describeFilter, validateFilter } from '../../analysis/query';
import { useApp } from '../../context/AppContext';
import { downloadTextFile, fileStem } from '../../utils/download';
import { Card, Notice, primaryButton } from './ui';

export const ExportTab: React.FC = () => {
  const {
    activeDataset: dataset,
    activeDatasetProfile: profile,
    draftSpec,
    appliedSpec,
    analysisResult: result,
    setWorkspaceTab,
  } = useApp();
  const [done, setDone] = useState<string>('');

  const usableFilters = useMemo(
    () => (dataset ? draftSpec.filters.filter((f) => validateFilter(dataset, f).length === 0) : []),
    [dataset, draftSpec.filters],
  );
  const matching = useMemo(() => (dataset ? applyFilters(dataset.rows, usableFilters).length : 0), [dataset, usableFilters]);
  if (!dataset || !profile) return null;

  const stem = fileStem(dataset.fileName);
  const finish = (name: string) => setDone(`Downloaded ${name}`);

  const exportResult = () => {
    if (!result) return;
    const name = `${stem}_analysis.csv`;
    downloadTextFile(name, resultToCsv(result), 'text/csv;charset=utf-8;');
    finish(name);
  };
  const exportFiltered = () => {
    const name = `${stem}_filtered.csv`;
    downloadTextFile(name, filteredDatasetToCsv(dataset, profile, usableFilters).csv, 'text/csv;charset=utf-8;');
    finish(name);
  };
  const exportReport = () => {
    if (!result || !appliedSpec) return;
    const name = `${stem}_analysis_report.json`;
    downloadTextFile(name, JSON.stringify(buildReport({ dataset, profile, spec: appliedSpec, result }), null, 2), 'application/json');
    finish(name);
  };

  return (
    <div className="space-y-4">
      <Notice>Files are created in your browser from the rows you loaded. Nothing is uploaded.</Notice>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <Card title="Aggregated results (CSV)" subtitle="The table from Results & Charts, with every group.">
          <p className="text-xs text-slate-600 dark:text-slate-300">
            {result ? `${result.totalGroups.toLocaleString('en-US')} row(s), ${result.columns.length} column(s).` : 'Run an analysis to enable this export.'}
          </p>
          <div className="mt-3">
            <button type="button" className={primaryButton} disabled={!result} onClick={exportResult}>
              <Download className="h-3.5 w-3.5" /> Download results
            </button>
            {!result && (
              <button type="button" className="ml-2 text-xs font-semibold text-sky-700 underline dark:text-sky-300" onClick={() => setWorkspaceTab('build')}>
                Build an analysis
              </button>
            )}
          </div>
        </Card>

        <Card title="Filtered dataset (CSV)" subtitle="The rows that pass your filters, exactly as they were in your file.">
          <p className="text-xs text-slate-600 dark:text-slate-300">
            {matching.toLocaleString('en-US')} of {dataset.rows.length.toLocaleString('en-US')} rows
            {usableFilters.length ? `: ${usableFilters.map(describeFilter).join(' and ')}` : ' (no filters, the whole file)'}.
          </p>
          <div className="mt-3">
            <button type="button" className={primaryButton} onClick={exportFiltered}>
              <Download className="h-3.5 w-3.5" /> Download rows
            </button>
          </div>
        </Card>

        <Card title="Analysis report (JSON)" subtitle="The question, the data, the filters, the calculation and the result.">
          <p className="text-xs text-slate-600 dark:text-slate-300">
            {result ? 'Includes the data quality summary and any warnings.' : 'Run an analysis to enable this export.'}
          </p>
          <div className="mt-3">
            <button type="button" className={primaryButton} disabled={!result} onClick={exportReport}>
              <Download className="h-3.5 w-3.5" /> Download report
            </button>
          </div>
        </Card>
      </div>

      <p className="min-h-5 text-xs font-semibold text-emerald-700 dark:text-emerald-300" role="status" aria-live="polite">
        {done}
      </p>
    </div>
  );
};
