import React from 'react';
import {
  Sparkles,
  Upload,
  FolderOpen,
  ArrowRight,
  Database,
  Table,
  ShieldCheck,
  GitFork,
  FileSpreadsheet,
  ExternalLink,
  Layers,
  ChevronRight,
} from 'lucide-react';
import { useApp } from '../context/AppContext';

export const HomeView: React.FC = () => {
  const {
    setActiveTab,
    loadSampleDataset,
    projects,
    mode,
    setIsConnectionModalOpen,
  } = useApp();

  return (
    <div className="mx-auto max-w-6xl space-y-8 pb-12">
      {/* ------------------------------------------------------------------ */}
      {/* Hero Banner: Clean, high-contrast, authentic Quaero identity       */}
      {/* ------------------------------------------------------------------ */}
      <div className="relative overflow-hidden rounded-3xl border border-slate-200 bg-white p-6 shadow-sm transition dark:border-slate-800 dark:bg-slate-900 sm:p-8 lg:p-10">
        <div className="relative z-10 flex flex-col justify-between space-y-6 lg:flex-row lg:items-center lg:space-y-0">
          <div className="max-w-2xl space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              <span className="inline-flex items-center space-x-1.5 rounded-full border border-sky-300 bg-sky-50 px-3 py-1 text-xs font-bold text-sky-900 dark:border-sky-800 dark:bg-sky-950/60 dark:text-sky-300">
                <Sparkles className="h-3.5 w-3.5 text-sky-700 dark:text-sky-400" />
                <span>Quaero Web Console v0.7.0</span>
              </span>
              <span className="rounded-full border border-slate-300 bg-slate-100 px-3 py-1 font-mono text-xs font-semibold text-slate-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200">
                MIT Open Source
              </span>
              <span className="rounded-full border border-slate-300 bg-slate-100 px-3 py-1 font-mono text-xs font-semibold text-slate-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200">
                Python 3.11+ Core
              </span>
            </div>

            <h1 className="text-3xl font-extrabold tracking-tight text-slate-900 dark:text-white sm:text-4xl lg:text-5xl">
              Question-Driven Analytics Pipeline
            </h1>

            <p className="text-sm leading-relaxed text-slate-600 dark:text-slate-300 sm:text-base">
              From raw telemetry and CSV datasets to semantic KPI contracts, automated data quality scoring, dimensional data marts, and interactive lineage.
            </p>

            <p className="text-xs italic text-slate-500 font-mono dark:text-slate-400">
              quaerō · Latin · "to seek, to ask, to inquire"
            </p>
          </div>

          {/* Quick CTA buttons with strict WCAG contrast */}
          <div className="flex flex-col space-y-3 sm:flex-row sm:space-x-3 sm:space-y-0 lg:flex-col lg:space-x-0 lg:space-y-3">
            <button
              onClick={() => {
                loadSampleDataset('release_impact');
                setActiveTab('import');
              }}
              className="flex items-center justify-center space-x-2 rounded-2xl bg-sky-600 px-5 py-3 text-xs font-bold text-white shadow-sm transition hover:bg-sky-500 active:scale-[0.99] dark:bg-sky-500 dark:hover:bg-sky-400"
            >
              <Sparkles className="h-4 w-4" />
              <span>Launch Interactive Demo</span>
            </button>

            <button
              onClick={() => setActiveTab('import')}
              className="flex items-center justify-center space-x-2 rounded-2xl border border-slate-300 bg-slate-100 px-5 py-3 text-xs font-bold text-slate-900 shadow-2xs transition hover:bg-slate-200 active:scale-[0.99] dark:border-slate-700 dark:bg-slate-800 dark:text-white dark:hover:bg-slate-700"
            >
              <Upload className="h-4 w-4 text-sky-600 dark:text-sky-400" />
              <span>Import Custom CSV</span>
            </button>
          </div>
        </div>

        {/* Bottom Technology Ribbon */}
        <div className="mt-8 flex flex-wrap items-center justify-between gap-4 border-t border-slate-100 pt-4 text-xs text-slate-500 dark:border-slate-800/80 dark:text-slate-400">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-semibold text-slate-700 dark:text-slate-300">Stack:</span>
            {['FastAPI', 'Pandas', 'Parquet', 'SQLite', 'Power BI Export', 'React + Vite'].map((tech) => (
              <span
                key={tech}
                className="rounded-lg border border-slate-200 bg-slate-50 px-2 py-0.5 font-mono text-[11px] font-semibold text-slate-750 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200"
              >
                {tech}
              </span>
            ))}
          </div>

          <a
            href="https://github.com/matteocavo/Quaero"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center space-x-1 font-semibold text-sky-700 hover:text-sky-800 dark:text-sky-400 dark:hover:text-sky-300"
          >
            <span>github.com/matteocavo/Quaero</span>
            <ExternalLink className="h-3 w-3" />
          </a>
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* 3 Core Interactive Entry Points                                    */}
      {/* ------------------------------------------------------------------ */}
      <div>
        <div className="mb-4">
          <h2 className="text-xs font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400">
            Primary Workflows
          </h2>
        </div>

        <div className="grid grid-cols-1 gap-5 md:grid-cols-3">
          {/* Card 1: Interactive Browser Demo */}
          <div className="flex flex-col justify-between rounded-3xl border border-slate-200 bg-white p-6 shadow-2xs transition hover:border-sky-300 hover:shadow-md dark:border-slate-800 dark:bg-slate-900 dark:hover:border-sky-600">
            <div className="space-y-3">
              <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-sky-50 text-sky-600 dark:bg-sky-950/60 dark:text-sky-400">
                <Sparkles className="h-6 w-6" />
              </div>
              <div className="flex items-center justify-between">
                <span className="rounded-full bg-sky-100 px-2.5 py-0.5 text-[10px] font-bold text-sky-900 dark:bg-sky-950 dark:text-sky-300">
                  Quick Start
                </span>
                <span className="text-xs font-mono text-slate-500 dark:text-slate-400">Sandbox</span>
              </div>
              <h3 className="text-base font-bold text-slate-900 dark:text-white">
                1. Try Interactive Demo
              </h3>
              <p className="text-xs leading-relaxed text-slate-600 dark:text-slate-300">
                Instant browser profiling of CSV files: test music release streams (<span className="font-mono font-semibold text-slate-800 dark:text-slate-200">release_impact</span>) or macro indicators (<span className="font-mono font-semibold text-slate-800 dark:text-slate-200">global_indicators</span>).
              </p>
            </div>
            <div className="mt-6 border-t border-slate-100 pt-4 dark:border-slate-800">
              <button
                onClick={() => {
                  loadSampleDataset('release_impact');
                  setActiveTab('import');
                }}
                className="flex w-full items-center justify-center space-x-2 rounded-2xl bg-sky-600 py-2.5 text-xs font-bold text-white shadow-2xs transition hover:bg-sky-500 dark:bg-sky-500 dark:hover:bg-sky-400"
              >
                <span>Launch Interactive Demo</span>
                <ArrowRight className="h-3.5 w-3.5" />
              </button>
            </div>
          </div>

          {/* Card 2: Import Custom CSV */}
          <div className="flex flex-col justify-between rounded-3xl border border-slate-200 bg-white p-6 shadow-2xs transition hover:border-emerald-300 hover:shadow-md dark:border-slate-800 dark:bg-slate-900 dark:hover:border-emerald-600">
            <div className="space-y-3">
              <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-emerald-50 text-emerald-600 dark:bg-emerald-950/60 dark:text-emerald-400">
                <Upload className="h-6 w-6" />
              </div>
              <div className="flex items-center justify-between">
                <span className="rounded-full bg-emerald-100 px-2.5 py-0.5 text-[10px] font-bold text-emerald-900 dark:bg-emerald-950 dark:text-emerald-300">
                  New Analysis
                </span>
                <span className="text-xs font-mono text-slate-500 dark:text-slate-400">Stage 01</span>
              </div>
              <h3 className="text-base font-bold text-slate-900 dark:text-white">
                2. Import Custom CSV
              </h3>
              <p className="text-xs leading-relaxed text-slate-600 dark:text-slate-300">
                Upload your CSV to auto-detect delimiters, infer column datatypes, audit missingness & uniqueness, detect IQR distribution outliers, and download an audit JSON.
              </p>
            </div>
            <div className="mt-6 border-t border-slate-100 pt-4 dark:border-slate-800">
              <button
                onClick={() => setActiveTab('import')}
                className="flex w-full items-center justify-center space-x-2 rounded-2xl border border-slate-300 bg-slate-100 py-2.5 text-xs font-bold text-slate-900 shadow-2xs transition hover:bg-slate-200 dark:border-slate-700 dark:bg-slate-800 dark:text-white dark:hover:bg-slate-700"
              >
                <span>Upload & Profile CSV</span>
                <ArrowRight className="h-3.5 w-3.5" />
              </button>
            </div>
          </div>

          {/* Card 3: Open Existing Project */}
          <div className="flex flex-col justify-between rounded-3xl border border-slate-200 bg-white p-6 shadow-2xs transition hover:border-purple-300 hover:shadow-md dark:border-slate-800 dark:bg-slate-900 dark:hover:border-purple-600">
            <div className="space-y-3">
              <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-purple-50 text-purple-600 dark:bg-purple-950/60 dark:text-purple-400">
                <FolderOpen className="h-6 w-6" />
              </div>
              <div className="flex items-center justify-between">
                <span className="rounded-full bg-purple-100 px-2.5 py-0.5 text-[10px] font-bold text-purple-900 dark:bg-purple-950 dark:text-purple-300">
                  Marts & KPIs
                </span>
                <span className="text-xs font-mono text-slate-500 dark:text-slate-400">{projects.length} Projects</span>
              </div>
              <h3 className="text-base font-bold text-slate-900 dark:text-white">
                3. Open Existing Project
              </h3>
              <p className="text-xs leading-relaxed text-slate-600 dark:text-slate-300">
                Explore pre-computed analytical data marts, inspect semantic contracts, review automated quality scores, and export Power BI & SQL definitions.
              </p>
            </div>
            <div className="mt-6 border-t border-slate-100 pt-4 dark:border-slate-800">
              <button
                onClick={() => setActiveTab('explorer')}
                className="flex w-full items-center justify-center space-x-2 rounded-2xl border border-slate-300 bg-slate-100 py-2.5 text-xs font-bold text-slate-900 shadow-2xs transition hover:bg-slate-200 dark:border-slate-700 dark:bg-slate-800 dark:text-white dark:hover:bg-slate-700"
              >
                <span>Browse Projects ({projects.length})</span>
                <ArrowRight className="h-3.5 w-3.5" />
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Quaero 5-Stage Pipeline Workflow Architecture                       */}
      {/* ------------------------------------------------------------------ */}
      <div>
        <div className="mb-4 flex flex-col justify-between sm:flex-row sm:items-center">
          <div>
            <h2 className="text-xs font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400">
              End-to-End Pipeline Architecture
            </h2>
            <p className="text-xs text-slate-600 dark:text-slate-400">
              Quaero structures data processing into five distinct, auditable pipeline stages.
            </p>
          </div>
          <span className="mt-2 text-xs font-mono text-slate-500 sm:mt-0 dark:text-slate-400">
            raw → staging → semantic → marts → consumption
          </span>
        </div>

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-5">
          {/* Stage 01 */}
          <div
            onClick={() => setActiveTab('import')}
            className="group cursor-pointer rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs transition hover:border-sky-300 hover:shadow-sm dark:border-slate-800 dark:bg-slate-900 dark:hover:border-sky-600"
          >
            <div className="flex items-center justify-between text-xs font-bold">
              <span className="rounded bg-sky-100 px-2 py-0.5 text-[10px] text-sky-900 dark:bg-sky-950 dark:text-sky-300">
                STAGE 01
              </span>
              <FileSpreadsheet className="h-4 w-4 text-sky-600 dark:text-sky-400" />
            </div>
            <h4 className="mt-3 text-sm font-bold text-slate-900 group-hover:text-sky-600 dark:text-white dark:group-hover:text-sky-400">
              Ingestion & Profiling
            </h4>
            <p className="mt-1 text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">
              Browser-side delimiter detection, schema inference, null counts, and distribution auditing.
            </p>
            <div className="mt-3 flex items-center text-[11px] font-semibold text-sky-700 dark:text-sky-400">
              <span>Open Profiler</span>
              <ChevronRight className="h-3 w-3 transition group-hover:translate-x-1" />
            </div>
          </div>

          {/* Stage 02 */}
          <div
            onClick={() => setActiveTab('explorer')}
            className="group cursor-pointer rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs transition hover:border-purple-300 hover:shadow-sm dark:border-slate-800 dark:bg-slate-900 dark:hover:border-purple-600"
          >
            <div className="flex items-center justify-between text-xs font-bold">
              <span className="rounded bg-purple-100 px-2 py-0.5 text-[10px] text-purple-900 dark:bg-purple-950 dark:text-purple-300">
                STAGE 02
              </span>
              <Layers className="h-4 w-4 text-purple-600 dark:text-purple-400" />
            </div>
            <h4 className="mt-3 text-sm font-bold text-slate-900 group-hover:text-purple-600 dark:text-white dark:group-hover:text-purple-400">
              Semantic KPI Engine
            </h4>
            <p className="mt-1 text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">
              Formal KPI definitions, business questions, grain mapping, and dimensional metadata contracts.
            </p>
            <div className="mt-3 flex items-center text-[11px] font-semibold text-purple-700 dark:text-purple-400">
              <span>View Contracts</span>
              <ChevronRight className="h-3 w-3 transition group-hover:translate-x-1" />
            </div>
          </div>

          {/* Stage 03 */}
          <div
            onClick={() => setActiveTab('quality')}
            className="group cursor-pointer rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs transition hover:border-amber-300 hover:shadow-sm dark:border-slate-800 dark:bg-slate-900 dark:hover:border-amber-600"
          >
            <div className="flex items-center justify-between text-xs font-bold">
              <span className="rounded bg-amber-100 px-2 py-0.5 text-[10px] text-amber-900 dark:bg-amber-950 dark:text-amber-300">
                STAGE 03
              </span>
              <ShieldCheck className="h-4 w-4 text-amber-600 dark:text-amber-400" />
            </div>
            <h4 className="mt-3 text-sm font-bold text-slate-900 group-hover:text-amber-600 dark:text-white dark:group-hover:text-amber-400">
              Quality & DQ Scorer
            </h4>
            <p className="mt-1 text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">
              Rules validation, IQR outlier framing, completeness scoring, and persistent audit trail.
            </p>
            <div className="mt-3 flex items-center text-[11px] font-semibold text-amber-800 dark:text-amber-400">
              <span>Inspect Rules</span>
              <ChevronRight className="h-3 w-3 transition group-hover:translate-x-1" />
            </div>
          </div>

          {/* Stage 04 */}
          <div
            onClick={() => setActiveTab('marts')}
            className="group cursor-pointer rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs transition hover:border-emerald-300 hover:shadow-sm dark:border-slate-800 dark:bg-slate-900 dark:hover:border-emerald-600"
          >
            <div className="flex items-center justify-between text-xs font-bold">
              <span className="rounded bg-emerald-100 px-2 py-0.5 text-[10px] text-emerald-900 dark:bg-emerald-950 dark:text-emerald-300">
                STAGE 04
              </span>
              <Table className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
            </div>
            <h4 className="mt-3 text-sm font-bold text-slate-900 group-hover:text-emerald-600 dark:text-white dark:group-hover:text-emerald-400">
              Analytical Marts
            </h4>
            <p className="mt-1 text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">
              Columnar Parquet marts, star schema aggregation, facts, and dimension tables.
            </p>
            <div className="mt-3 flex items-center text-[11px] font-semibold text-emerald-700 dark:text-emerald-400">
              <span>Query Marts</span>
              <ChevronRight className="h-3 w-3 transition group-hover:translate-x-1" />
            </div>
          </div>

          {/* Stage 05 */}
          <div
            onClick={() => setActiveTab('lineage')}
            className="group cursor-pointer rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs transition hover:border-sky-300 hover:shadow-sm dark:border-slate-800 dark:bg-slate-900 dark:hover:border-sky-600"
          >
            <div className="flex items-center justify-between text-xs font-bold">
              <span className="rounded bg-sky-100 px-2 py-0.5 text-[10px] text-sky-900 dark:bg-sky-950 dark:text-sky-300">
                STAGE 05
              </span>
              <GitFork className="h-4 w-4 text-sky-600 dark:text-sky-400" />
            </div>
            <h4 className="mt-3 text-sm font-bold text-slate-900 group-hover:text-sky-600 dark:text-white dark:group-hover:text-sky-400">
              Lineage & BI Exports
            </h4>
            <p className="mt-1 text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">
              Interactive end-to-end DAG lineage visualization, Power BI DAX expressions, and SQL DDL.
            </p>
            <div className="mt-3 flex items-center text-[11px] font-semibold text-sky-700 dark:text-sky-400">
              <span>Explore DAG</span>
              <ChevronRight className="h-3 w-3 transition group-hover:translate-x-1" />
            </div>
          </div>
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Architecture & Execution Model Transparency Card                   */}
      {/* ------------------------------------------------------------------ */}
      <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
        <div className="flex flex-col justify-between space-y-4 lg:flex-row lg:items-center lg:space-y-0">
          <div className="flex items-start space-x-3">
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-2.5 text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300">
              <Database className="h-5 w-5" />
            </div>
            <div className="space-y-1">
              <div className="flex items-center space-x-2">
                <h3 className="text-sm font-bold text-slate-900 dark:text-white">
                  Execution Model & Operating Modes
                </h3>
                <span
                  className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${
                    mode === 'demo'
                      ? 'border border-amber-300 bg-amber-100 text-amber-900 dark:border-amber-800 dark:bg-amber-950/80 dark:text-amber-300'
                      : 'border border-emerald-300 bg-emerald-100 text-emerald-900 dark:border-emerald-800 dark:bg-emerald-950/80 dark:text-emerald-300'
                  }`}
                >
                  Current: {mode === 'demo' ? 'Public Demo Mode' : 'Local Operations Mode'}
                </span>
              </div>
              <p className="max-w-3xl text-xs text-slate-600 dark:text-slate-400">
                <strong>Public Demo Mode</strong> operates autonomously with zero cloud dependencies using bundled demo fixtures and in-browser CSV parsing. <strong>Local Operations Mode</strong> connects via same-origin proxy to your local Quaero FastAPI backend (<code className="rounded border border-slate-200 bg-slate-100 px-1 py-0.5 text-[11px] text-slate-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200">uvicorn api.main:app</code>) and SQLite database.
              </p>
            </div>
          </div>

          <div className="flex shrink-0 space-x-2">
            <button
              onClick={() => setIsConnectionModalOpen(true)}
              className="rounded-xl border border-slate-300 bg-slate-100 px-3.5 py-2 text-xs font-bold text-slate-900 shadow-2xs transition hover:bg-slate-200 active:scale-[0.99] dark:border-slate-700 dark:bg-slate-800 dark:text-white dark:hover:bg-slate-700"
            >
              Configure Environment
            </button>
            <button
              onClick={() => setActiveTab('overview')}
              className="rounded-xl bg-slate-900 px-3.5 py-2 text-xs font-bold text-white shadow-2xs transition hover:bg-slate-800 active:scale-[0.99] dark:bg-sky-600 dark:text-white dark:hover:bg-sky-500"
            >
              Open Dashboard
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};
