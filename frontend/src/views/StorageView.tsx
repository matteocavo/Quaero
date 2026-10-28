import React, { useCallback, useEffect, useRef, useState } from 'react';
import { AlertTriangle, HardDrive, RefreshCw, ShieldCheck, Trash2 } from 'lucide-react';
import { formatBytes } from '../analysis/format';
import { useApp } from '../context/AppContext';
import type {
  CleanupRequest,
  CleanupResponse,
  LifecyclePolicy,
  StorageStatusResponse,
  StorageUploadItem,
} from '../types/api';
import { Card, Notice, Stat, primaryButton, secondaryButton } from './analysis/ui';

const POLICIES: { value: LifecyclePolicy; label: string; text: string }[] = [
  {
    value: 'ask_on_exit',
    label: 'Ask on exit (default)',
    text: 'When the launcher closes it lists this session’s temporary uploads and asks before deleting anything. Without a terminal to ask, it keeps them.',
  },
  {
    value: 'auto_cleanup',
    label: 'Auto cleanup',
    text: 'When the launcher closes it deletes this session’s temporary uploads without asking.',
  },
  {
    value: 'keep_files',
    label: 'Keep files',
    text: 'Nothing is deleted automatically. You remove files here.',
  },
];

const dangerButton =
  'inline-flex items-center justify-center gap-1.5 rounded-xl bg-rose-600 px-4 py-2 text-xs font-bold text-white ' +
  'shadow-xs transition hover:bg-rose-500 disabled:cursor-not-allowed disabled:opacity-50';

/** What the user is about to delete, described for the confirmation. The server decides what really goes. */
interface Pending {
  request: CleanupRequest;
  title: string;
  lines: string[];
  bytes: number;
}

const uploadLine = (u: StorageUploadItem) => `${u.original_filename ?? 'unknown name'} (${u.upload_id}, ${formatBytes(u.size_bytes)})`;

const stateLabel = (u: StorageUploadItem) =>
  u.incomplete ? 'incomplete' : u.state === 'current_session' ? 'this session' : 'earlier session';

export const StorageView: React.FC = () => {
  const { apiClient, mode, showFlash } = useApp();
  const [status, setStatus] = useState<StorageStatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [pending, setPending] = useState<Pending | null>(null);
  const [result, setResult] = useState<CleanupResponse | null>(null);
  const cancelRef = useRef<HTMLButtonElement>(null);

  const load = useCallback(async () => {
    try {
      setStatus(await apiClient.getStorage());
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not read the local storage status.');
    }
  }, [apiClient]);

  useEffect(() => {
    if (mode === 'live') void load();
  }, [mode, load]);

  useEffect(() => {
    if (!pending) return;
    cancelRef.current?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !busy) setPending(null);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [pending, busy]);

  if (mode !== 'live') {
    return (
      <Notice>
        Local Storage manages files on the machine where Quaero runs, so it is only available in Local Operations mode. The
        Public Demo and the Browser Workspace keep nothing on disk.
      </Notice>
    );
  }

  const changePolicy = async (policy: LifecyclePolicy) => {
    const previous = status?.lifecycle_policy;
    // Show the choice at once (the radio is controlled by this state) and put the old one back if saving fails.
    setStatus((s) => (s ? { ...s, lifecycle_policy: policy } : s));
    try {
      const saved = await apiClient.setLifecyclePolicy(policy);
      setStatus((s) => (s ? { ...s, lifecycle_policy: saved.lifecycle_policy } : s));
      showFlash(`Cleanup on exit set to “${POLICIES.find((p) => p.value === policy)?.label}”.`, 'success');
    } catch (err) {
      if (previous) setStatus((s) => (s ? { ...s, lifecycle_policy: previous } : s));
      showFlash(err instanceof Error ? err.message : 'Could not save the setting.', 'error');
    }
  };

  const confirmDeletion = async () => {
    if (!pending) return;
    setBusy(true);
    try {
      // The only place a deletion is sent, and only with the explicit confirmation of the dialog.
      setResult(await apiClient.cleanupStorage({ ...pending.request, confirm: true }));
    } catch (err) {
      setResult(null);
      showFlash(err instanceof Error ? err.message : 'The deletion failed.', 'error');
    } finally {
      setBusy(false);
      setPending(null);
      await load();
    }
  };

  const uploads = status?.uploads ?? [];
  const mine = uploads.filter((u) => u.state === 'current_session');
  const leftovers = uploads.filter((u) => u.state === 'orphaned');
  const temporary = [...mine, ...leftovers];
  const saved = uploads.filter((u) => u.state === 'saved');
  const sum = (items: StorageUploadItem[]) => items.reduce((total, u) => total + u.size_bytes, 0);

  const askBulk = (scope: 'current_session' | 'orphaned', items: StorageUploadItem[], title: string) =>
    setPending({ request: { scope, confirm: false }, title, lines: items.map(uploadLine), bytes: sum(items) });

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="flex items-center gap-2 text-xl font-bold tracking-tight text-slate-900 dark:text-white">
            <HardDrive className="h-5 w-5 text-sky-600 dark:text-sky-400" /> Local Storage
          </h2>
          <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
            What Quaero keeps on this machine: temporary uploads, projects you saved, and the files the pipeline generated.
          </p>
        </div>
        <button type="button" className={secondaryButton} onClick={() => void load()}>
          <RefreshCw className="h-3.5 w-3.5" /> Refresh
        </button>
      </div>

      {error && <Notice tone="error">{error}</Notice>}

      {status && (
        <>
          {status.orphaned_uploads > 0 && (
            <div className="flex flex-col gap-2 rounded-2xl border border-amber-300 bg-amber-50 p-4 text-xs text-amber-950 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-100 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex items-start gap-2">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                <span>
                  <strong>
                    {status.orphaned_uploads} temporary upload{status.orphaned_uploads === 1 ? '' : 's'} ({formatBytes(sum(leftovers))})
                  </strong>{' '}
                  from an earlier session {status.orphaned_uploads === 1 ? 'is' : 'are'} still on disk, for example after Quaero closed
                  unexpectedly. Nothing was deleted automatically.
                </span>
              </div>
              <button
                type="button"
                className={dangerButton}
                onClick={() => askBulk('orphaned', leftovers, 'Delete leftovers from earlier sessions')}
              >
                <Trash2 className="h-3.5 w-3.5" /> Review and delete…
              </button>
            </div>
          )}

          {result && (
            <div role="status" className="rounded-2xl border border-emerald-300 bg-emerald-50 p-4 text-xs text-emerald-950 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100">
              <div className="font-bold">{result.message}</div>
              {result.deleted.length > 0 && (
                <ul className="mt-1 list-disc pl-5">
                  {result.deleted.map((id) => (
                    <li key={id}>Deleted {id}</li>
                  ))}
                </ul>
              )}
              {result.skipped.length > 0 && (
                <ul className="mt-1 list-disc pl-5 text-amber-900 dark:text-amber-200">
                  {result.skipped.map((s) => (
                    <li key={`${s.id}-${s.reason}`}>
                      Left in place: {s.id} ({s.reason})
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}

          <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
            <Stat label="Temporary uploads" value={formatBytes(status.totals.temporary_uploads_bytes)} hint={`${temporary.length} file set${temporary.length === 1 ? '' : 's'}`} />
            <Stat label="Saved uploads" value={formatBytes(status.totals.saved_uploads_bytes)} hint={`${saved.length}, used by projects`} />
            <Stat label="Project settings" value={formatBytes(status.totals.project_configs_bytes)} />
            <Stat label="Generated files" value={formatBytes(status.totals.artifacts_bytes)} hint="marts, metadata, reports" />
            <Stat label="Total" value={formatBytes(status.totals.total_bytes)} />
          </div>

          <Card title="When Quaero closes" subtitle="Applies to temporary uploads made in the current session. Saved projects are never deleted.">
            <fieldset className="space-y-2">
              <legend className="sr-only">Cleanup policy</legend>
              {POLICIES.map((p) => (
                <label key={p.value} className="flex cursor-pointer items-start gap-2 rounded-lg border border-slate-200 p-2.5 text-xs dark:border-slate-800">
                  <input
                    type="radio"
                    name="lifecycle-policy"
                    value={p.value}
                    checked={status.lifecycle_policy === p.value}
                    onChange={() => void changePolicy(p.value)}
                    className="mt-0.5"
                  />
                  <span>
                    <span className="font-bold text-slate-900 dark:text-white">{p.label}</span>
                    <span className="block text-slate-600 dark:text-slate-300">{p.text}</span>
                  </span>
                </label>
              ))}
            </fieldset>
            <p className="mt-2 text-[11px] text-slate-500 dark:text-slate-400">
              This runs when Quaero is started with <span className="font-mono">scripts/start-local.ps1</span>. If you start the
              servers yourself, nothing happens at exit; use the buttons below.
            </p>
          </Card>

          <Card title="Temporary uploads" subtitle="CSV files you uploaded that no project uses.">
            {temporary.length === 0 ? (
              <p className="text-xs text-slate-600 dark:text-slate-300">None. There is nothing temporary on disk.</p>
            ) : (
              <>
                <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
                  <table className="w-full text-left text-xs">
                    <thead className="bg-slate-50 font-semibold text-slate-600 dark:bg-slate-800/60 dark:text-slate-300">
                      <tr>
                        <th className="px-3 py-2">File</th>
                        <th className="px-3 py-2">Origin</th>
                        <th className="px-3 py-2">Size</th>
                        <th className="px-3 py-2" />
                      </tr>
                    </thead>
                    <tbody>
                      {temporary.map((u) => (
                        <tr key={u.upload_id} className="border-t border-slate-100 dark:border-slate-800">
                          <td className="px-3 py-1.5">
                            <div className="font-semibold text-slate-900 dark:text-slate-100">{u.original_filename ?? 'unknown name'}</div>
                            <div className="font-mono text-[10px] text-slate-500">{u.upload_id}</div>
                          </td>
                          <td className="px-3 py-1.5">{stateLabel(u)}</td>
                          <td className="whitespace-nowrap px-3 py-1.5">{formatBytes(u.size_bytes)}</td>
                          <td className="px-3 py-1.5 text-right">
                            <button
                              type="button"
                              className={secondaryButton}
                              aria-label={`Delete ${u.original_filename ?? u.upload_id}`}
                              onClick={() =>
                                setPending({
                                  request: { scope: 'single_upload', target_id: u.upload_id, confirm: false },
                                  title: 'Delete this upload',
                                  lines: [uploadLine(u)],
                                  bytes: u.size_bytes,
                                })
                              }
                            >
                              <Trash2 className="h-3.5 w-3.5" /> Delete…
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  <button
                    type="button"
                    className={secondaryButton}
                    disabled={mine.length === 0}
                    onClick={() => askBulk('current_session', mine, 'Delete this session’s temporary uploads')}
                  >
                    Delete this session’s uploads ({mine.length})…
                  </button>
                  <button
                    type="button"
                    className={secondaryButton}
                    disabled={leftovers.length === 0}
                    onClick={() => askBulk('orphaned', leftovers, 'Delete leftovers from earlier sessions')}
                  >
                    Delete leftovers from earlier sessions ({leftovers.length})…
                  </button>
                </div>
              </>
            )}
          </Card>

          <Card title="Saved projects" subtitle="Projects keep their settings and the upload they use. These are never deleted automatically.">
            {status.projects.length === 0 ? (
              <p className="text-xs text-slate-600 dark:text-slate-300">No projects found.</p>
            ) : (
              <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
                <table className="w-full text-left text-xs">
                  <thead className="bg-slate-50 font-semibold text-slate-600 dark:bg-slate-800/60 dark:text-slate-300">
                    <tr>
                      <th className="px-3 py-2">Project</th>
                      <th className="px-3 py-2">Kind</th>
                      <th className="px-3 py-2">Settings</th>
                      <th className="px-3 py-2">Generated files</th>
                      <th className="px-3 py-2" />
                    </tr>
                  </thead>
                  <tbody>
                    {status.projects.map((p) => {
                      const upload = saved.find((u) => u.upload_id === p.upload_id);
                      return (
                        <tr key={p.project} className="border-t border-slate-100 align-top dark:border-slate-800">
                          <td className="px-3 py-1.5 font-mono font-semibold text-slate-900 dark:text-slate-100">{p.project}</td>
                          <td className="px-3 py-1.5">
                            {p.kind === 'upload_project' ? (
                              <>
                                created from an upload
                                {upload && <div className="text-[10px] text-slate-500">uses {upload.original_filename ?? upload.upload_id} ({formatBytes(upload.size_bytes)}), kept</div>}
                              </>
                            ) : (
                              <span className="inline-flex items-center gap-1">
                                <ShieldCheck className="h-3 w-3 text-emerald-600" /> existing project, protected
                              </span>
                            )}
                          </td>
                          <td className="whitespace-nowrap px-3 py-1.5">{formatBytes(p.config_bytes)}</td>
                          <td className="whitespace-nowrap px-3 py-1.5">
                            {formatBytes(p.artifacts_bytes)} in {p.artifact_files} file{p.artifact_files === 1 ? '' : 's'}
                            {p.marts_count > 0 ? `, ${p.marts_count} mart${p.marts_count === 1 ? '' : 's'}` : ''}
                          </td>
                          <td className="px-3 py-1.5 text-right">
                            {p.can_clear_artifacts ? (
                              <button
                                type="button"
                                className={secondaryButton}
                                aria-label={`Clear generated files of ${p.project}`}
                                onClick={() =>
                                  setPending({
                                    request: { scope: 'project_artifacts', target_id: p.project, confirm: false },
                                    title: `Delete the generated files of ${p.project}`,
                                    lines: [
                                      `${p.artifact_files} generated file${p.artifact_files === 1 ? '' : 's'} (${formatBytes(p.artifacts_bytes)}) of ${p.project}`,
                                      'The project and its settings stay. You can run the pipeline again to recreate the files.',
                                    ],
                                    bytes: p.artifacts_bytes,
                                  })
                                }
                              >
                                <Trash2 className="h-3.5 w-3.5" /> Clear generated files…
                              </button>
                            ) : null}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        </>
      )}

      {pending && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 p-4">
          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="delete-title"
            aria-describedby="delete-body"
            className="w-full max-w-md rounded-2xl border border-slate-200 bg-white p-5 shadow-xl dark:border-slate-700 dark:bg-slate-900"
          >
            <h3 id="delete-title" className="text-base font-bold text-slate-900 dark:text-white">
              {pending.title}?
            </h3>
            <div id="delete-body" className="mt-2 space-y-2 text-xs text-slate-700 dark:text-slate-200">
              <p className="font-semibold text-rose-700 dark:text-rose-300">This permanently deletes the following. It cannot be undone.</p>
              <ul className="max-h-48 list-disc space-y-0.5 overflow-y-auto pl-5">
                {pending.lines.map((line) => (
                  <li key={line} className="break-words">
                    {line}
                  </li>
                ))}
              </ul>
              {pending.request.scope !== 'project_artifacts' && (
                <p>Total {formatBytes(pending.bytes)}. Projects and the files they use are not affected.</p>
              )}
            </div>
            <div className="mt-4 flex justify-end gap-2">
              <button ref={cancelRef} type="button" className={primaryButton} disabled={busy} onClick={() => setPending(null)}>
                Cancel
              </button>
              <button type="button" className={dangerButton} disabled={busy} onClick={() => void confirmDeletion()}>
                {busy ? 'Deleting…' : 'Delete permanently'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
