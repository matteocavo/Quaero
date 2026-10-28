import React from 'react';
import { Upload } from 'lucide-react';
import { useApp, type WorkspaceTab } from '../../context/AppContext';
import { BuildTab } from './BuildTab';
import { ExploreTab } from './ExploreTab';
import { ExportTab } from './ExportTab';
import { PreviewTab } from './PreviewTab';
import { QualityTab } from './QualityTab';
import { ResultsTab } from './ResultsTab';
import { Card, Notice, primaryButton, secondaryButton } from './ui';

const TABS: { id: WorkspaceTab; label: string }[] = [
  { id: 'preview', label: 'Data Preview' },
  { id: 'quality', label: 'Data Quality' },
  { id: 'explore', label: 'Explore & Filter' },
  { id: 'build', label: 'Build Analysis' },
  { id: 'results', label: 'Results & Charts' },
  { id: 'export', label: 'Export' },
];

const PANELS: Record<WorkspaceTab, React.FC> = {
  preview: PreviewTab,
  quality: QualityTab,
  explore: ExploreTab,
  build: BuildTab,
  results: ResultsTab,
  export: ExportTab,
};

export const AnalysisWorkspace: React.FC = () => {
  const { activeDataset: dataset, workspaceTab, setWorkspaceTab, setActiveTab, loadSampleDataset } = useApp();

  if (!dataset) {
    return (
      <div className="mx-auto max-w-2xl space-y-4">
        <Card title="Analyze a dataset">
          <p className="text-sm text-slate-700 dark:text-slate-200">
            Load a CSV to filter it, group it, calculate totals and averages, chart the result and download it, all in your browser.
          </p>
          <div className="mt-4 flex flex-wrap gap-2">
            <button type="button" className={primaryButton} onClick={() => setActiveTab('import')}>
              <Upload className="h-3.5 w-3.5" /> Upload a CSV
            </button>
            <button type="button" className={secondaryButton} onClick={() => loadSampleDataset('release_impact')}>
              Try the release_impact sample
            </button>
            <button type="button" className={secondaryButton} onClick={() => loadSampleDataset('global_indicators')}>
              Try the macro indicators sample
            </button>
          </div>
          <p className="mt-3 text-[11px] text-slate-500 dark:text-slate-400">
            Your file stays in this browser tab. It is not uploaded anywhere.
          </p>
        </Card>
      </div>
    );
  }

  const Panel = PANELS[workspaceTab];
  const move = (event: React.KeyboardEvent, index: number) => {
    const step = event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0;
    if (!step) return;
    event.preventDefault();
    const next = TABS[(index + step + TABS.length) % TABS.length];
    setWorkspaceTab(next.id);
    document.getElementById(`workspace-tab-${next.id}`)?.focus();
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900">
        <div className="min-w-0">
          <h2 className="truncate text-base font-extrabold text-slate-900 dark:text-white" title={dataset.fileName}>
            {dataset.fileName}
          </h2>
          <p className="text-xs text-slate-600 dark:text-slate-300">
            {dataset.rows.length.toLocaleString('en-US')} rows · {dataset.columns.length} columns ·{' '}
            {dataset.origin === 'sample' ? 'bundled sample' : 'your file'}
          </p>
        </div>
        <button type="button" className={secondaryButton} onClick={() => setActiveTab('import')}>
          Load a different dataset
        </button>
      </div>

      <Notice>
        Browser analysis: calculated here, on the rows you loaded. Nothing is uploaded, and it is not the Quaero Python
        pipeline (no cleaning stages, marts, lineage or anomaly framing).
      </Notice>

      <div role="tablist" aria-label="Analysis workspace" className="flex gap-1 overflow-x-auto border-b border-slate-200 dark:border-slate-800">
        {TABS.map((tab, index) => {
          const selected = tab.id === workspaceTab;
          return (
            <button
              key={tab.id}
              id={`workspace-tab-${tab.id}`}
              role="tab"
              type="button"
              aria-selected={selected}
              aria-controls="workspace-panel"
              tabIndex={selected ? 0 : -1}
              onKeyDown={(e) => move(e, index)}
              onClick={() => setWorkspaceTab(tab.id)}
              className={`shrink-0 whitespace-nowrap border-b-2 px-3 py-2 text-xs font-bold transition ${
                selected
                  ? 'border-sky-600 text-sky-700 dark:border-sky-400 dark:text-sky-300'
                  : 'border-transparent text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-slate-100'
              }`}
            >
              {tab.label}
            </button>
          );
        })}
      </div>

      <div id="workspace-panel" role="tabpanel" aria-labelledby={`workspace-tab-${workspaceTab}`}>
        <Panel />
      </div>
    </div>
  );
};
