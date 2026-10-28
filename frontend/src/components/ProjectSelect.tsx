import React from 'react';
import { BROWSER_WORKSPACE_ID, useApp } from '../context/AppContext';

const BASE = 'appearance-none rounded-xl border text-xs font-bold transition focus:outline-none';
const PROJECT_STYLE =
  'border-slate-300 bg-slate-100 text-slate-900 hover:bg-slate-200 focus:border-sky-500 focus:bg-white ' +
  'dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100 dark:hover:bg-slate-700 dark:focus:border-sky-400';
// A different colour family, so a personal dataset never looks like a demo project.
const WORKSPACE_STYLE =
  'border-sky-500 bg-sky-50 text-sky-950 hover:bg-sky-100 focus:border-sky-600 ' +
  'dark:border-sky-400 dark:bg-sky-950/60 dark:text-sky-100 dark:hover:bg-sky-950';

/**
 * The global context selector: demo / Local projects, plus the Browser Workspace once a dataset has been
 * loaded in this tab. The two are separate groups on purpose.
 */
export const ProjectSelect: React.FC<{ id: string; className?: string }> = ({ id, className = '' }) => {
  const { projects, selectorValue, selectWorkspace, browserWorkspaceActive, activeDataset } = useApp();
  return (
    <select
      id={id}
      value={selectorValue}
      onChange={(e) => selectWorkspace(e.target.value)}
      className={`${BASE} ${browserWorkspaceActive ? WORKSPACE_STYLE : PROJECT_STYLE} ${className}`}
    >
      <optgroup label="Demo projects">
        {projects.map((p) => (
          <option key={p.project} value={p.project}>
            {p.project} ({p.marts_count} marts)
          </option>
        ))}
      </optgroup>
      {activeDataset && (
        <optgroup label="Browser workspace (this tab only)">
          <option value={BROWSER_WORKSPACE_ID}>Browser workspace: {activeDataset.fileName}</option>
        </optgroup>
      )}
    </select>
  );
};
