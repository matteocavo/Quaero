import React from 'react';
import {
  Home,
  Upload,
  LayoutDashboard,
  FolderSearch,
  PlayCircle,
  Table,
  ShieldAlert,
  GitFork,
  Activity,
  Database,
  ExternalLink,
  BarChart3,
  HardDrive,
} from 'lucide-react';
import { ProjectSelect } from './ProjectSelect';
import { useApp } from '../context/AppContext';
import { NavigationTab } from '../types/api';

interface NavItem {
  id: NavigationTab;
  label: string;
  icon: React.ElementType;
  /** Only shown in Local Operations mode: it manages files on the machine running the API. */
  liveOnly?: boolean;
}

const NAV_ITEMS: NavItem[] = [
  { id: 'home', label: 'Get Started & Workflow', icon: Home },
  { id: 'import', label: 'Import & Profile Data', icon: Upload },
  { id: 'analyze', label: 'Analyze Dataset', icon: BarChart3 },
  { id: 'overview', label: 'Overview Dashboard', icon: LayoutDashboard },
  { id: 'explorer', label: 'Project Explorer', icon: FolderSearch },
  { id: 'operations', label: 'Pipeline Operations', icon: PlayCircle },
  { id: 'marts', label: 'Mart Explorer', icon: Table },
  { id: 'quality', label: 'Quality & Anomalies', icon: ShieldAlert },
  { id: 'lineage', label: 'Data Lineage DAG', icon: GitFork },
  { id: 'storage', label: 'Local Storage', icon: HardDrive, liveOnly: true },
];

export const Sidebar: React.FC = () => {
  const {
    activeTab,
    setActiveTab,
    currentOverview,
    mode,
    activeDatasetProfile,
    isMobileNavOpen,
    setMobileNavOpen,
    browserWorkspaceActive,
    activeDataset,
  } = useApp();

  return (
    <aside
      aria-label="Navigation"
      className={`flex flex-col justify-between border-r border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900 fixed bottom-0 left-0 top-16 z-40 w-72 max-w-[85vw] overflow-y-auto transition-transform duration-200 md:static md:z-auto md:w-64 md:max-w-none md:shrink-0 md:translate-x-0 md:overflow-visible md:bg-white/80 md:dark:bg-slate-900/80 ${isMobileNavOpen ? 'translate-x-0 shadow-xl' : '-translate-x-full'}`}
    >
      {/* Navigation Links */}
      <div className="space-y-4">
        <div className="space-y-1 md:hidden">
          <label
            htmlFor="project-select-menu"
            className="px-3 text-[11px] font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400"
          >
            Active project
          </label>
          <ProjectSelect id="project-select-menu" className="w-full truncate px-3 py-2" />
        </div>
        <div className="space-y-1">
          <div className="px-3 py-1.5 text-[11px] font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400">
            Analytics Workflow
          </div>
          <nav className="space-y-0.5">
            {NAV_ITEMS.filter((item) => !item.liveOnly || mode === 'live').map((item) => {
              const Icon = item.icon;
              const isActive = activeTab === item.id;
              return (
                <button
                  key={item.id}
                  onClick={() => {
                    setActiveTab(item.id);
                    setMobileNavOpen(false);
                  }}
                  className={`group flex w-full items-center justify-between rounded-xl px-3 py-2 text-xs font-semibold transition ${
                    isActive
                      ? 'bg-sky-50 text-sky-800 shadow-2xs dark:bg-sky-950/70 dark:text-sky-200'
                      : 'text-slate-700 hover:bg-slate-100/80 hover:text-slate-900 dark:text-slate-300 dark:hover:bg-slate-800/60 dark:hover:text-white'
                  }`}
                >
                  <div className="flex items-center space-x-2.5">
                    <Icon
                      className={`h-4 w-4 transition ${
                        isActive
                          ? 'text-sky-600 dark:text-sky-400'
                          : 'text-slate-400 group-hover:text-slate-600 dark:text-slate-400 dark:group-hover:text-slate-200'
                      }`}
                    />
                    <span>{item.label}</span>
                  </div>
                  {item.id === 'analyze' && activeDatasetProfile && (
                    <span className="rounded-full bg-sky-100 px-1.5 py-0.2 text-[9px] font-bold text-sky-800 dark:bg-sky-950 dark:text-sky-300">
                      Ready
                    </span>
                  )}
                  {item.id === 'import' && activeDatasetProfile && (
                    <span className="rounded-full bg-emerald-100 px-1.5 py-0.2 text-[9px] font-bold text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300">
                      Loaded
                    </span>
                  )}
                  {item.id === 'marts' && currentOverview && (
                    <span className="rounded-full bg-slate-100 px-1.5 py-0.2 text-[10px] font-semibold text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                      {currentOverview.marts_count}
                    </span>
                  )}
                </button>
              );
            })}
          </nav>
        </div>
      </div>

      {/* Selected Project Overview Card */}
      <div className="mt-auto space-y-3 pt-4">
        {browserWorkspaceActive && activeDataset && (
          <div className="rounded-2xl border border-sky-300 bg-sky-50 p-3.5 shadow-2xs dark:border-sky-800 dark:bg-sky-950/40">
            <div className="flex items-center justify-between gap-2 text-xs">
              <span className="min-w-0 truncate font-bold text-sky-950 dark:text-sky-100" title={activeDataset.fileName}>
                {activeDataset.fileName}
              </span>
              <span className="shrink-0 rounded bg-sky-200 px-1.5 py-0.2 text-[9px] font-bold text-sky-900 dark:bg-sky-900 dark:text-sky-100">
                BROWSER
              </span>
            </div>
            <div className="mt-2 space-y-1 text-[11px] text-sky-900 dark:text-sky-200">
              <div className="flex items-center justify-between">
                <span>Rows x columns:</span>
                <span className="font-semibold">
                  {activeDataset.rows.length.toLocaleString('en-US')} x {activeDataset.columns.length}
                </span>
              </div>
              <div>Kept in this tab only. Not saved.</div>
            </div>
          </div>
        )}
        {currentOverview && (
          <div className="rounded-2xl border border-slate-200/90 bg-slate-50/80 p-3.5 shadow-2xs dark:border-slate-800 dark:bg-slate-800/50">
            <div className="flex items-center justify-between text-xs">
              <span className="font-bold text-slate-800 dark:text-slate-200 truncate max-w-[120px]" title={currentOverview.project}>
                {currentOverview.project}
              </span>
              <span className="rounded bg-sky-100 px-1.5 py-0.2 text-[9px] font-mono font-semibold text-sky-800 dark:bg-sky-950 dark:text-sky-300">
                {currentOverview.project_mode || 'single'}
              </span>
            </div>
            <div className="mt-2 space-y-1 text-[11px] text-slate-600 dark:text-slate-300 font-sans">
              <div className="flex items-center justify-between">
                <span>Analytical Marts:</span>
                <span className="font-semibold text-slate-900 dark:text-white">
                  {currentOverview.marts_count}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span>Primary KPI:</span>
                <span className="font-mono text-sky-700 dark:text-sky-300 font-semibold truncate max-w-[100px]">
                  {currentOverview.primary_kpi || 'avg_streams'}
                </span>
              </div>
            </div>
          </div>
        )}

        {/* System & Mode Summary */}
        <div className="rounded-xl border border-slate-200/80 bg-white p-2.5 text-[11px] dark:border-slate-800 dark:bg-slate-800/80">
          <div className="flex items-center justify-between text-slate-600 dark:text-slate-300">
            <span className="flex items-center">
              <Database className="mr-1.5 h-3 w-3 text-slate-500 dark:text-slate-400" />
              Runtime
            </span>
            <span className="font-bold text-slate-800 dark:text-slate-200">
              {mode === 'demo' ? 'Static Demo' : 'FastAPI Local'}
            </span>
          </div>
          <div className="mt-1 flex items-center justify-between text-slate-500 dark:text-slate-400">
            <span className="flex items-center">
              <Activity className="mr-1.5 h-3 w-3 text-emerald-500" />
              Web App
            </span>
            <span className="font-mono text-[10px] text-slate-600 dark:text-slate-300">
              Quaero 0.7.0
            </span>
          </div>
        </div>

        {/* External Documentation Link */}
        <div className="flex items-center justify-between px-1 text-[11px] text-slate-500 dark:text-slate-400">
          <span>Quaero Repository</span>
          <a
            href="https://github.com/matteocavo/Quaero"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center hover:text-sky-600 dark:hover:text-sky-400"
          >
            <span>MIT License</span>
            <ExternalLink className="ml-1 h-3 w-3" />
          </a>
        </div>
        <p className="px-1 text-[10px] leading-snug text-slate-400 dark:text-slate-500">
          Demo data is bundled with the app. The macro sample is illustrative (rounded values),
          inspired by the World Bank World Development Indicators (CC BY 4.0).
        </p>
      </div>
    </aside>
  );
};
