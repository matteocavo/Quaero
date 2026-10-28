import React from 'react';
import {
  Menu,
  Moon,
  Sun,
  Server,
  Layers,
  FileSpreadsheet,
  ChevronDown,
  Github,
  CheckCircle2,
  AlertCircle,
  Sparkles,
} from 'lucide-react';
import { useApp } from '../context/AppContext';
import { ProjectSelect } from './ProjectSelect';

export const Header: React.FC = () => {
  const {
    mode,
    browserWorkspaceActive,
    theme,
    toggleTheme,
    connectionStatus,
    setIsConnectionModalOpen,
    isMobileNavOpen,
    setMobileNavOpen,
  } = useApp();

  return (
    <header className="sticky top-0 z-30 flex h-16 w-full items-center justify-between border-b border-slate-200 bg-white/95 px-4 backdrop-blur-md dark:border-slate-800 dark:bg-slate-900/95 sm:px-6">
      {/* Brand & Project Selector */}
      <div className="flex min-w-0 items-center space-x-2 sm:space-x-6">
        <button
          type="button"
          aria-label="Open navigation menu"
          aria-expanded={isMobileNavOpen}
          onClick={() => setMobileNavOpen(!isMobileNavOpen)}
          className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-slate-300 bg-slate-100 text-slate-800 md:hidden dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200"
        >
          <Menu className="h-4 w-4" />
        </button>
        <div className="flex items-center space-x-2.5">
          {/* Modern vector logo mark replacing the 90s emoji */}
          <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-linear-to-br from-sky-500 via-sky-600 to-indigo-600 text-white shadow-xs ring-1 ring-sky-400/30">
            <svg
              className="h-5 w-5 text-white"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2.2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <circle cx="11" cy="11" r="7" />
              <path d="m21 21-4.35-4.35" />
              <circle cx="11" cy="11" r="2.5" fill="currentColor" fillOpacity="0.4" />
            </svg>
          </div>
          <div className="hidden sm:block">
            <div className="flex items-center space-x-2">
              <span className="text-base font-extrabold tracking-tight text-slate-900 dark:text-white">
                Quaero
              </span>
              <span className="rounded-md border border-sky-300 bg-sky-100 px-1.5 py-0.2 text-[10px] font-bold text-sky-900 dark:border-sky-800 dark:bg-sky-950 dark:text-sky-300">
                v0.7.0
              </span>
            </div>
            <p className="hidden text-[11px] font-medium text-slate-500 dark:text-slate-400 sm:block">
              Question-Driven Analytics
            </p>
          </div>
        </div>

        {/* Project Selector */}
        <div className="relative hidden md:block">
          <label htmlFor="project-select" className="sr-only">
            Select Active Project
          </label>
          <div className="relative inline-flex max-w-[20rem] items-center">
            {browserWorkspaceActive ? (
              <FileSpreadsheet className="pointer-events-none absolute left-3 h-3.5 w-3.5 text-sky-600 dark:text-sky-300" />
            ) : (
              <Layers className="pointer-events-none absolute left-3 h-3.5 w-3.5 text-slate-400 dark:text-slate-500" />
            )}
            <ProjectSelect id="project-select" className="h-9 w-full truncate py-1 pl-8.5 pr-8" />
            <ChevronDown className="pointer-events-none absolute right-2.5 h-3.5 w-3.5 text-slate-500 dark:text-slate-400" />
          </div>
        </div>
      </div>

      {/* Actions & Status */}
      <div className="flex shrink-0 items-center space-x-2 sm:space-x-3">
        {/* Operating Mode Pill */}
        <button
          onClick={() => setIsConnectionModalOpen(true)}
          type="button"
          className={`flex items-center space-x-2 rounded-full px-3 py-1 text-xs font-bold transition ${
            mode === 'demo'
              ? 'border border-amber-300 bg-amber-100 text-amber-900 hover:bg-amber-200 dark:border-amber-800 dark:bg-amber-950/80 dark:text-amber-200'
              : connectionStatus === 'connected'
              ? 'border border-emerald-300 bg-emerald-100 text-emerald-900 hover:bg-emerald-200 dark:border-emerald-800 dark:bg-emerald-950/80 dark:text-emerald-200'
              : 'border border-rose-300 bg-rose-100 text-rose-900 hover:bg-rose-200 dark:border-rose-800 dark:bg-rose-950/80 dark:text-rose-200'
          }`}
          title="Click to view environment settings"
        >
          {mode === 'demo' ? (
            <>
              <Sparkles className="h-3 w-3 text-amber-700 dark:text-amber-400" />
              <span className="hidden sm:inline">Public Demo Sandbox</span>
              <span className="sm:hidden">Demo</span>
            </>
          ) : connectionStatus === 'connected' ? (
            <>
              <CheckCircle2 className="h-3.5 w-3.5 text-emerald-700 dark:text-emerald-400" />
              <span>FastAPI Connected</span>
            </>
          ) : (
            <>
              <AlertCircle className="h-3.5 w-3.5 text-rose-700 dark:text-rose-400" />
              <span>FastAPI Offline</span>
            </>
          )}
        </button>

        {/* Environment Modal Button */}
        <button
          onClick={() => setIsConnectionModalOpen(true)}
          className="flex h-9 items-center space-x-1.5 rounded-xl border border-slate-300 bg-slate-100 px-2.5 text-xs font-bold text-slate-900 shadow-2xs hover:bg-slate-200 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100 dark:hover:bg-slate-700"
          title="Configure environment connection"
        >
          <Server className="h-3.5 w-3.5 text-slate-600 dark:text-slate-400" />
          <span className="hidden sm:inline">Settings</span>
        </button>

        {/* GitHub link */}
        <a
          href="https://github.com/matteocavo/Quaero"
          target="_blank"
          rel="noopener noreferrer"
          className="hidden h-9 w-9 items-center justify-center sm:flex rounded-xl border border-slate-300 bg-slate-100 text-slate-800 shadow-2xs hover:bg-slate-200 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700"
          title="View source on GitHub (MIT License)"
        >
          <Github className="h-4 w-4" />
        </a>

        {/* Theme Toggle Button */}
        <button
          onClick={toggleTheme}
          type="button"
          className="flex h-9 w-9 items-center justify-center rounded-xl border border-slate-300 bg-slate-100 text-slate-800 shadow-2xs hover:bg-slate-200 dark:border-slate-700 dark:bg-slate-800 dark:text-amber-300 dark:hover:bg-slate-700"
          title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
        >
          {theme === 'dark' ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </button>
      </div>
    </header>
  );
};
