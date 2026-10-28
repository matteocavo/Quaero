import React from 'react';
import { AppProvider, useApp } from './context/AppContext';
import { Header } from './components/Header';
import { Sidebar } from './components/Sidebar';
import { ConnectionModal } from './components/ConnectionModal';
import { HomeView } from './views/HomeView';
import { DataImportView } from './views/DataImportView';
import { AnalysisWorkspace } from './views/analysis/AnalysisWorkspace';
import { OverviewView } from './views/OverviewView';
import { ProjectExplorerView } from './views/ProjectExplorerView';
import { OperationsView } from './views/OperationsView';
import { MartExplorerView } from './views/MartExplorerView';
import { QualityView } from './views/QualityView';
import { LineageView } from './views/LineageView';
import { WorkspaceOverview } from './views/workspace/WorkspaceOverview';
import { WorkspaceExplorer } from './views/workspace/WorkspaceExplorer';
import { BrowserWorkspaceUnavailable } from './views/workspace/Unavailable';
import { X, CheckCircle2, AlertCircle, Info, AlertTriangle } from 'lucide-react';

const AppContent: React.FC = () => {
  const {
    activeTab,
    setActiveTab,
    setWorkspaceTab,
    browserWorkspaceActive,
    flashMessage,
    clearFlash,
    mode,
    setMode,
    connectionStatus,
    setIsConnectionModalOpen,
    isMobileNavOpen,
    setMobileNavOpen,
  } = useApp();

  return (
    <div className="flex min-h-screen flex-col bg-slate-50 text-slate-900 antialiased dark:bg-slate-950 dark:text-slate-100">
      {/* Top Header */}
      <Header />

      {/* Main Body */}
      <div className="flex flex-1">
        {/* Persistent Sidebar (a slide-over menu on small screens) */}
        {isMobileNavOpen && (
          <div
            aria-hidden="true"
            onClick={() => setMobileNavOpen(false)}
            className="fixed inset-0 top-16 z-30 bg-slate-900/40 md:hidden"
          />
        )}
        <Sidebar />

        {/* Dynamic Main View Area */}
        <main className="min-w-0 flex-1 overflow-x-hidden overflow-y-auto p-4 sm:p-6 lg:p-8">
          {/* Offline Recovery Banner in Live Mode */}
          {mode === 'live' && connectionStatus === 'disconnected' && (
            <div className="mb-6 flex flex-col justify-between space-y-2 rounded-2xl border border-amber-200 bg-amber-50 p-4 text-xs text-amber-900 dark:border-amber-900/60 dark:bg-amber-950/40 dark:text-amber-200 sm:flex-row sm:items-center sm:space-y-0">
              <div className="flex items-center space-x-2.5">
                <AlertTriangle className="h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
                <div>
                  <span className="font-bold">FastAPI Backend Offline:</span>
                  <span className="ml-1">
                    Local Operations mode could not connect to localhost:8000 via proxy.
                  </span>
                </div>
              </div>
              <div className="flex items-center space-x-2">
                <button
                  onClick={() => setMode('demo')}
                  className="rounded-xl bg-amber-200/80 px-3 py-1 font-bold text-amber-900 hover:bg-amber-200 dark:bg-amber-900 dark:text-amber-200"
                >
                  Return to Public Demo
                </button>
                <button
                  onClick={() => setIsConnectionModalOpen(true)}
                  className="rounded-xl border border-amber-300 bg-white px-3 py-1 font-semibold text-amber-900 hover:bg-amber-50 dark:border-amber-800 dark:bg-slate-900 dark:text-amber-200"
                >
                  Setup Guide
                </button>
              </div>
            </div>
          )}

          {/* Global Flash Banner with High Contrast */}
          {flashMessage && (
            <div
              className={`mb-6 flex items-center justify-between rounded-2xl border p-4 text-xs font-semibold shadow-2xs transition-all ${
                flashMessage.type === 'success'
                  ? 'border-emerald-200 bg-emerald-50 text-emerald-900 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-100'
                  : flashMessage.type === 'error'
                  ? 'border-rose-200 bg-rose-50 text-rose-900 dark:border-rose-800 dark:bg-rose-950 dark:text-rose-100'
                  : 'border-sky-200 bg-sky-50 text-sky-900 dark:border-sky-800 dark:bg-sky-950 dark:text-sky-100'
              }`}
            >
              <div className="flex items-center space-x-2.5">
                {flashMessage.type === 'success' && <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400" />}
                {flashMessage.type === 'error' && <AlertCircle className="h-4 w-4 shrink-0 text-rose-600 dark:text-rose-400" />}
                {flashMessage.type === 'info' && <Info className="h-4 w-4 shrink-0 text-sky-600 dark:text-sky-400" />}
                <span className="font-medium">{flashMessage.text}</span>
              </div>
              <button
                onClick={clearFlash}
                className="rounded p-1 text-slate-400 hover:bg-black/5 dark:hover:bg-white/10 dark:text-slate-300"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          )}

          {/* Active Screen Tab View */}
          {activeTab === 'home' && <HomeView />}
          {activeTab === 'import' && <DataImportView />}
          {activeTab === 'analyze' && <AnalysisWorkspace />}
          {/* While the Browser Workspace is active, project-only screens are replaced, never fed demo data. */}
          {activeTab === 'overview' && (browserWorkspaceActive ? <WorkspaceOverview /> : <OverviewView />)}
          {activeTab === 'explorer' && (browserWorkspaceActive ? <WorkspaceExplorer /> : <ProjectExplorerView />)}
          {activeTab === 'operations' &&
            (browserWorkspaceActive ? <BrowserWorkspaceUnavailable screen="Pipeline Operations" /> : <OperationsView />)}
          {activeTab === 'marts' &&
            (browserWorkspaceActive ? <BrowserWorkspaceUnavailable screen="Mart Explorer" /> : <MartExplorerView />)}
          {activeTab === 'quality' &&
            (browserWorkspaceActive ? (
              <BrowserWorkspaceUnavailable
                screen="Quality & Anomalies"
                alternative={{
                  label: 'Open Data Quality for this dataset',
                  open: () => {
                    setWorkspaceTab('quality');
                    setActiveTab('analyze');
                  },
                }}
              />
            ) : (
              <QualityView />
            ))}
          {activeTab === 'lineage' &&
            (browserWorkspaceActive ? <BrowserWorkspaceUnavailable screen="Data Lineage DAG" /> : <LineageView />)}
        </main>
      </div>

      {/* Global Connection / Environment Settings Modal */}
      <ConnectionModal />
    </div>
  );
};

export const App: React.FC = () => {
  return (
    <AppProvider>
      <AppContent />
    </AppProvider>
  );
};

export default App;
