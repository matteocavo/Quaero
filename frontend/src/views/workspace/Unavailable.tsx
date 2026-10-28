import React from 'react';
import { useApp } from '../../context/AppContext';
import { Card, Notice, primaryButton, secondaryButton } from '../analysis/ui';

/** What a Browser Workspace deliberately does not have. Shown wherever the demo would otherwise suggest it. */
export const UNAVAILABLE_ITEMS: { name: string; reason: string }[] = [
  {
    name: 'Parquet marts',
    reason: 'They are written by the Python pipeline. A browser tab cannot create them, so none is listed or invented.',
  },
  {
    name: 'Python lineage',
    reason: 'The lineage graph describes pipeline stages that never ran for this file.',
  },
  {
    name: 'Pipeline execution',
    reason: 'The static demo has no backend. Runs, run history and anomaly framing need the Python pipeline and the API.',
  },
];

export const UnavailableCard: React.FC = () => (
  <Card
    title="Not available for a Browser Workspace"
    subtitle="This dataset lives only in this tab and was never processed by the Quaero Python pipeline."
  >
    <ul className="space-y-2 text-xs text-slate-700 dark:text-slate-200">
      {UNAVAILABLE_ITEMS.map((item) => (
        <li key={item.name} className="rounded-lg border border-slate-200 bg-slate-50 p-2.5 dark:border-slate-800 dark:bg-slate-800/40">
          <span className="font-bold text-slate-900 dark:text-white">{item.name}</span>
          <span className="ml-1 text-slate-600 dark:text-slate-300">not available in the static demo. {item.reason}</span>
        </li>
      ))}
    </ul>
    <p className="mt-3 text-[11px] text-slate-500 dark:text-slate-400">
      To get these for your own data, run Quaero locally with a project configuration. Uploading a CSV to the pipeline is
      not supported yet.
    </p>
  </Card>
);

/** Replaces a project-only screen while the Browser Workspace is the active context. */
export const BrowserWorkspaceUnavailable: React.FC<{ screen: string; alternative?: { label: string; open: () => void } }> = ({
  screen,
  alternative,
}) => {
  const { activeDataset, selectedProject, setSelectedProject, setActiveTab, setWorkspaceTab } = useApp();
  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-xl font-bold tracking-tight text-slate-900 dark:text-white">{screen}</h2>
        <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
          Active context: Browser workspace
          {activeDataset ? <span className="font-mono"> {activeDataset.fileName}</span> : null}
        </p>
      </div>
      <Notice tone="warning">
        {screen} is not available for a Browser Workspace. No demo data is shown here, so nothing from a demo project can be
        mistaken for your dataset.
      </Notice>
      <UnavailableCard />
      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          className={primaryButton}
          onClick={() => {
            setWorkspaceTab('preview');
            setActiveTab('analyze');
          }}
        >
          Open Analyze Dataset
        </button>
        {alternative && (
          <button type="button" className={secondaryButton} onClick={alternative.open}>
            {alternative.label}
          </button>
        )}
        <button type="button" className={secondaryButton} onClick={() => setSelectedProject(selectedProject)}>
          Switch to demo project {selectedProject}
        </button>
      </div>
    </div>
  );
};
