import React, { useEffect, useState } from 'react';
import { formatCell } from '../../analysis/format';
import { useApp } from '../../context/AppContext';
import { Card, KindBadge, inputClass, secondaryButton } from './ui';

export const PreviewTab: React.FC = () => {
  const { activeDataset: dataset } = useApp();
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  useEffect(() => setPage(1), [dataset?.id]);
  if (!dataset) return null;

  const pages = Math.max(1, Math.ceil(dataset.rows.length / pageSize));
  const current = Math.min(page, pages);
  const rows = dataset.rows.slice((current - 1) * pageSize, current * pageSize);

  return (
    <div className="space-y-4">
      <Card
        title="Columns"
        subtitle="The type of each column is read from your file; the next steps use these types."
      >
        <ul className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {dataset.columns.map((column) => (
            <li
              key={column.name}
              className="flex items-center justify-between gap-2 rounded-lg border border-slate-200 px-3 py-2 text-xs dark:border-slate-800"
            >
              <span className="min-w-0 truncate font-mono font-semibold text-slate-900 dark:text-slate-100" title={column.name}>
                {column.name}
              </span>
              <span className="flex shrink-0 items-center gap-2">
                {column.nullCount > 0 && (
                  <span className="text-[11px] text-amber-700 dark:text-amber-300">{column.nullCount} missing</span>
                )}
                <KindBadge kind={column.kind} />
              </span>
            </li>
          ))}
        </ul>
      </Card>

      <Card title="Rows" subtitle="Values as the analysis reads them. A dash means the value is missing.">
        <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-50 font-semibold text-slate-600 dark:bg-slate-800/60 dark:text-slate-300">
              <tr>
                <th className="px-3 py-2 text-slate-400">#</th>
                {dataset.columns.map((c) => (
                  <th key={c.name} className="whitespace-nowrap px-3 py-2">
                    {c.name}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={index} className="border-t border-slate-100 dark:border-slate-800">
                  <td className="px-3 py-1.5 text-slate-400">{(current - 1) * pageSize + index + 1}</td>
                  {dataset.columns.map((c) => (
                    <td
                      key={c.name}
                      className={`whitespace-nowrap px-3 py-1.5 ${
                        row[c.name] === null ? 'text-slate-400' : 'text-slate-800 dark:text-slate-100'
                      }`}
                    >
                      {formatCell(row[c.name])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-600 dark:text-slate-300">
          <span>
            Rows {(current - 1) * pageSize + 1}-{Math.min(current * pageSize, dataset.rows.length)} of{' '}
            {dataset.rows.length.toLocaleString('en-US')}
          </span>
          <div className="flex items-center gap-2">
            <label htmlFor="preview-page-size" className="sr-only">
              Rows per page
            </label>
            <select
              id="preview-page-size"
              value={pageSize}
              onChange={(e) => {
                setPageSize(Number(e.target.value));
                setPage(1);
              }}
              className={`${inputClass} w-auto`}
            >
              {[10, 25, 50].map((n) => (
                <option key={n} value={n}>
                  {n} per page
                </option>
              ))}
            </select>
            <button type="button" className={secondaryButton} disabled={current <= 1} onClick={() => setPage(current - 1)}>
              Previous
            </button>
            <span>
              {current} / {pages}
            </span>
            <button type="button" className={secondaryButton} disabled={current >= pages} onClick={() => setPage(current + 1)}>
              Next
            </button>
          </div>
        </div>
      </Card>
    </div>
  );
};
