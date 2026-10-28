import React, { useState, useEffect, useMemo } from 'react';
import {
  Table as TableIcon,
  Download,
  Search,
  ChevronLeft,
  ChevronRight,
  Database,
  FileSpreadsheet,
  Filter,
  Columns,
} from 'lucide-react';
import { useApp } from '../context/AppContext';
import { toCsv } from '../utils/csv';
import { downloadTextFile } from '../utils/download';
import { MartInfo, MartPreviewResponse } from '../types/api';

export const MartExplorerView: React.FC = () => {
  const { selectedProject, apiClient, showFlash } = useApp();

  const [marts, setMarts] = useState<MartInfo[]>([]);
  const [selectedMart, setSelectedMart] = useState<string>('');
  const [pageSize, setPageSize] = useState<number>(50);
  const [page, setPage] = useState<number>(1);
  const [preview, setPreview] = useState<MartPreviewResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [previewNote, setPreviewNote] = useState<string | null>(null);
  const [filterText, setFilterText] = useState<string>('');

  // 1. Fetch marts list
  useEffect(() => {
    let isMounted = true;
    const fetchMarts = async () => {
      try {
        const res = await apiClient.listMarts(selectedProject);
        if (isMounted) {
          setMarts(res.marts);
          if (res.marts.length > 0) {
            setSelectedMart(res.marts[0].name);
            setPage(1);
          } else {
            setSelectedMart('');
            setPreview(null);
          }
        }
      } catch (err: any) {
        showFlash(`Could not load marts: ${err.message}`, 'error');
      }
    };
    fetchMarts();
    return () => {
      isMounted = false;
    };
  }, [selectedProject, apiClient]);

  // 2. Fetch mart preview
  useEffect(() => {
    if (!selectedMart) return;
    let isMounted = true;
    const fetchPreview = async () => {
      setLoading(true);
      try {
        const data = await apiClient.getMartPreview(
          selectedProject,
          selectedMart,
          page,
          pageSize,
        );
        if (isMounted) {
          setPreview(data);
          setPreviewNote(null);
        }
      } catch (err: any) {
        if (isMounted) {
          // Never keep showing the previous mart's rows under a different mart's name.
          setPreview(null);
          setPreviewNote(err.message);
        }
        if (err?.statusCode !== 404) {
          showFlash(`Could not load mart preview: ${err.message}`, 'error');
        }
      } finally {
        if (isMounted) setLoading(false);
      }
    };
    fetchPreview();
    return () => {
      isMounted = false;
    };
  }, [selectedProject, selectedMart, page, pageSize, apiClient]);

  // 3. Client-side row filtering across all values
  const filteredRows = useMemo(() => {
    if (!preview || !preview.rows) return [];
    const query = filterText.trim().toLowerCase();
    if (!query) return preview.rows;

    return preview.rows.filter((row) =>
      Object.values(row).some((val) =>
        String(val ?? '').toLowerCase().includes(query),
      ),
    );
  }, [preview, filterText]);

  // 4. Download as CSV
  const handleDownloadCsv = () => {
    if (!preview || filteredRows.length === 0) return;

    downloadTextFile(
      `${selectedProject}_${selectedMart}_page${page}.csv`,
      toCsv(preview.columns, filteredRows),
      'text/csv;charset=utf-8;',
    );
  };

  const totalPages = preview ? Math.max(1, Math.ceil(preview.total_rows / pageSize)) : 1;

  return (
    <div className="space-y-6">
      {/* Control Bar */}
      <div className="flex flex-col justify-between space-y-4 rounded-xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900 md:flex-row md:items-center md:space-y-0">
        <div className="flex flex-wrap items-center gap-3">
          {/* Mart Selector */}
          <div>
            <label className="text-[11px] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">
              Analytical Mart Table
            </label>
            <div className="mt-1">
              <select
                value={selectedMart}
                onChange={(e) => {
                  setSelectedMart(e.target.value);
                  setPage(1);
                }}
                className="h-9 rounded-lg border border-slate-200 bg-white px-3 text-xs font-semibold text-slate-800 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
              >
                {marts.map((m) => (
                  <option key={m.name} value={m.name}>
                    {m.name}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {/* Page Size Selector */}
          <div>
            <label className="text-[11px] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">
              Rows Per Page
            </label>
            <div className="mt-1">
              <select
                value={pageSize}
                onChange={(e) => {
                  setPageSize(Number(e.target.value));
                  setPage(1);
                }}
                className="h-9 rounded-lg border border-slate-200 bg-white px-3 text-xs text-slate-800 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
              >
                <option value={25}>25 rows</option>
                <option value={50}>50 rows</option>
                <option value={100}>100 rows</option>
                <option value={250}>250 rows</option>
              </select>
            </div>
          </div>
        </div>

        {/* Action Controls */}
        <div className="flex items-center space-x-2">
          <button
            onClick={handleDownloadCsv}
            disabled={!preview || filteredRows.length === 0}
            className="flex items-center space-x-1.5 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 shadow-2xs hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200"
          >
            <Download className="h-3.5 w-3.5 text-slate-500" />
            <span>Export Page (CSV)</span>
          </button>
        </div>
      </div>

      {/* Metrics Row */}
      {preview && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <div className="rounded-lg border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900">
            <span className="text-[11px] text-slate-500 dark:text-slate-400">Total Rows</span>
            <div className="text-lg font-bold text-slate-900 dark:text-white">
              {preview.total_rows.toLocaleString()}
            </div>
          </div>
          <div className="rounded-lg border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900">
            <span className="text-[11px] text-slate-500 dark:text-slate-400">Columns</span>
            <div className="text-lg font-bold text-slate-900 dark:text-white">
              {preview.columns.length}
            </div>
          </div>
          <div className="rounded-lg border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900">
            <span className="text-[11px] text-slate-500 dark:text-slate-400">Current Page</span>
            <div className="text-lg font-bold text-slate-900 dark:text-white">
              {page} / {totalPages}
            </div>
          </div>
          <div className="rounded-lg border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900">
            <span className="text-[11px] text-slate-500 dark:text-slate-400">Filtered View</span>
            <div className="text-lg font-bold text-slate-900 dark:text-white">
              {filteredRows.length} rows
            </div>
          </div>
        </div>
      )}

      {/* Search Filter Bar */}
      <div className="flex items-center space-x-2">
        <div className="relative flex-1">
          <Search className="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
          <input
            type="text"
            value={filterText}
            onChange={(e) => setFilterText(e.target.value)}
            placeholder="Filter current page rows (client-side substring match)..."
            className="w-full rounded-lg border border-slate-200 bg-white py-2 pl-9 pr-4 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-900 dark:text-white"
          />
        </div>
        {filterText && (
          <button
            onClick={() => setFilterText('')}
            className="rounded-lg border border-slate-200 px-3 py-2 text-xs text-slate-600 hover:bg-slate-100 dark:border-slate-700 dark:text-slate-300"
          >
            Clear
          </button>
        )}
      </div>

      {/* Data Table */}
      <div className="rounded-xl border border-slate-200 bg-white shadow-2xs dark:border-slate-800 dark:bg-slate-900">
        <div className="overflow-x-auto">
          {loading ? (
            <div className="flex h-64 items-center justify-center text-xs text-slate-500">
              Loading parquet records...
            </div>
          ) : !preview || filteredRows.length === 0 ? (
            <div className="flex h-64 items-center justify-center text-xs text-slate-500">
              {!preview && previewNote ? previewNote : 'No matching records found on this page.'}
            </div>
          ) : (
            <table className="w-full text-left text-xs">
              <thead className="border-b border-slate-100 bg-slate-50/80 font-medium text-slate-600 dark:border-slate-800 dark:bg-slate-800/50 dark:text-slate-300">
                <tr>
                  <th className="w-12 px-4 py-3 text-slate-400">#</th>
                  {preview.columns.map((col) => (
                    <th key={col} className="px-4 py-3 font-mono font-semibold">
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60 font-mono">
                {filteredRows.map((row, idx) => (
                  <tr key={idx} className="hover:bg-slate-50/60 dark:hover:bg-slate-800/40">
                    <td className="px-4 py-2.5 text-slate-400 text-[11px]">
                      {(page - 1) * pageSize + idx + 1}
                    </td>
                    {preview.columns.map((col) => (
                      <td key={col} className="px-4 py-2.5 text-slate-800 dark:text-slate-200">
                        {String(row[col] ?? '—')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Pagination Bar */}
        <div className="flex items-center justify-between border-t border-slate-100 p-4 dark:border-slate-800">
          <span className="text-xs text-slate-500 dark:text-slate-400">
            Page {page} of {totalPages}
          </span>
          <div className="flex items-center space-x-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page <= 1}
              className="flex items-center space-x-1 rounded-md border border-slate-200 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
            >
              <ChevronLeft className="h-3.5 w-3.5" />
              <span>Previous</span>
            </button>
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page >= totalPages}
              className="flex items-center space-x-1 rounded-md border border-slate-200 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
            >
              <span>Next</span>
              <ChevronRight className="h-3.5 w-3.5" />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};
