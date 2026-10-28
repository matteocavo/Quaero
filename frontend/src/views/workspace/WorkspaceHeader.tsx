import React, { useState } from 'react';
import { FileSpreadsheet } from 'lucide-react';
import { useApp } from '../../context/AppContext';
import { secondaryButton } from '../analysis/ui';

/** Identity of the active Browser Workspace, with the actions that apply to the whole dataset. */
export const WorkspaceHeader: React.FC<{ title: string }> = ({ title }) => {
  const { activeDataset, selectedProject, setSelectedProject, setActiveTab, setWorkspaceTab, clearDataset, showFlash } = useApp();
  const [confirming, setConfirming] = useState(false);
  if (!activeDataset) return null;

  const origin = activeDataset.origin === 'sample' ? 'bundled sample loaded in this tab' : 'your file';

  return (
    <div className="rounded-2xl border border-sky-300 bg-sky-50 p-4 dark:border-sky-800 dark:bg-sky-950/40">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <FileSpreadsheet className="h-5 w-5 shrink-0 text-sky-700 dark:text-sky-300" />
            <h2 className="min-w-0 break-words text-xl font-bold tracking-tight text-sky-950 dark:text-sky-100">{title}</h2>
            <span className="rounded-full bg-sky-600 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-white dark:bg-sky-500">
              Browser workspace
            </span>
          </div>
          <p className="mt-1 break-words font-mono text-xs font-semibold text-sky-900 dark:text-sky-200">{activeDataset.fileName}</p>
          <p className="mt-1 text-xs text-sky-900/90 dark:text-sky-200/90">
            Source: {origin}. It exists only in the memory of this tab: nothing is uploaded and nothing is saved, so reloading
            the page removes it.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            className={secondaryButton}
            onClick={() => {
              setWorkspaceTab('preview');
              setActiveTab('analyze');
            }}
          >
            Analyze Dataset
          </button>
          <button type="button" className={secondaryButton} onClick={() => setActiveTab('import')}>
            Replace dataset
          </button>
          <button type="button" className={secondaryButton} onClick={() => setSelectedProject(selectedProject)}>
            Use demo project
          </button>
          {confirming ? (
            <span className="inline-flex items-center gap-2 rounded-xl border border-rose-300 bg-rose-50 px-2 py-1 text-xs dark:border-rose-800 dark:bg-rose-950/40">
              <span className="font-semibold text-rose-900 dark:text-rose-200">Remove it and its analyses?</span>
              <button
                type="button"
                className="rounded-lg bg-rose-600 px-2 py-1 font-bold text-white hover:bg-rose-500"
                onClick={() => {
                  clearDataset();
                  showFlash('Dataset removed from this tab. Nothing was stored anywhere.', 'info');
                }}
              >
                Remove
              </button>
              <button type="button" className="font-semibold text-slate-700 underline dark:text-slate-200" onClick={() => setConfirming(false)}>
                Cancel
              </button>
            </span>
          ) : (
            <button type="button" className={secondaryButton} onClick={() => setConfirming(true)}>
              Remove dataset
            </button>
          )}
        </div>
      </div>
    </div>
  );
};
