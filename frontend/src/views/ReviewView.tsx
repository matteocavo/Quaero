import { useCallback, useEffect, useRef, useState } from 'react';
import { RefreshCw } from 'lucide-react';
import { useApp } from '../context/AppContext';
import type { ReviewItem, ReviewKind, ReviewStatus, ReviewSyncResponse } from '../types/api';
import { Card, Notice, inputClass, primaryButton, secondaryButton } from './analysis/ui';
import { ItemDetail } from './review/ItemDetail';
import { StatusBadge } from './review/StatusBadge';
import { KIND_LABEL, RULE_LABEL, describeError, isValidRunId, type DescribedError } from './review/logic';

const PAGE_SIZE = 20;
const STATUSES: ReviewStatus[] = ['REVIEW_REQUIRED', 'CORRECTED', 'ACCEPTED', 'REJECTED'];
const STATUS_OPTION: Record<ReviewStatus, string> = {
  REVIEW_REQUIRED: 'Review required',
  CORRECTED: 'Corrected',
  ACCEPTED: 'Accepted',
  REJECTED: 'Rejected',
};

function ErrorBox({ err }: { err: DescribedError }) {
  return (
    <Notice tone="error">
      <div data-testid="review-error">
        {err.code && (
          <>
            <code className="font-mono text-xs">{err.code}</code>
            {' — '}
          </>
        )}
        {err.explanation ?? err.message}
        {err.explanation && err.message && err.explanation !== err.message ? (
          <div className="mt-1 text-xs opacity-80">{err.message}</div>
        ) : null}
      </div>
    </Notice>
  );
}

export function ReviewView() {
  const { apiClient, selectedProject, projects } = useApp();
  const [project, setProject] = useState(selectedProject);
  const [status, setStatus] = useState<ReviewStatus | ''>('');
  const [kind, setKind] = useState<ReviewKind | ''>('');
  const [runId, setRunId] = useState('');
  const [superseded, setSuperseded] = useState<'' | 'true' | 'false'>('');
  const [offset, setOffset] = useState(0);

  const [items, setItems] = useState<ReviewItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState<DescribedError | null>(null);
  const [selectedId, setSelectedId] = useState<number | null>(null);

  const [syncing, setSyncing] = useState(false);
  const [syncResult, setSyncResult] = useState<ReviewSyncResponse | null>(null);
  const [syncError, setSyncError] = useState<DescribedError | null>(null);
  const syncInFlight = useRef(false);
  const loadSeq = useRef(0);

  const projectNames = Array.from(new Set([project, ...projects.map((p) => p.project)].filter(Boolean)));
  const runFilter = runId.trim() === '' ? undefined : runId.trim();
  const runInvalid = runFilter !== undefined && !isValidRunId(runFilter);

  const load = useCallback(async () => {
    if (!project || runInvalid) return;
    const seq = ++loadSeq.current;
    setLoading(true);
    setListError(null);
    try {
      const res = await apiClient.listReviewItems({
        project,
        status: status || undefined,
        kind: kind || undefined,
        run_id: runFilter,
        superseded: superseded === '' ? undefined : superseded === 'true',
        limit: PAGE_SIZE,
        offset,
      });
      if (seq !== loadSeq.current) return;
      setItems(res.items);
      setTotal(res.total);
    } catch (err) {
      if (seq !== loadSeq.current) return;
      setItems([]);
      setTotal(0);
      setListError(describeError(err));
    } finally {
      if (seq === loadSeq.current) setLoading(false);
    }
  }, [apiClient, project, status, kind, runFilter, runInvalid, superseded, offset]);

  useEffect(() => {
    void load();
  }, [load]);

  const resetPage = () => setOffset(0);

  async function runSync() {
    if (syncInFlight.current) return;
    syncInFlight.current = true;
    setSyncing(true);
    setSyncError(null);
    setSyncResult(null);
    try {
      const res = await apiClient.syncReview(project);
      setSyncResult(res);
      await load();
    } catch (err) {
      setSyncError(describeError(err));
    } finally {
      syncInFlight.current = false;
      setSyncing(false);
    }
  }

  const filtered = status !== '' || kind !== '' || runId !== '' || superseded !== '';
  const page = Math.floor(offset / PAGE_SIZE) + 1;
  const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  return (
    <div className="space-y-4" data-testid="review-view">
      <div>
        <h1 className="text-2xl font-bold">Human Review</h1>
        <p className="text-sm text-slate-600 dark:text-slate-300">
          Items that need a person to look at them. “Review required” is a normal waiting state, not a pipeline error.
        </p>
      </div>

      <Card title="Project and sync" subtitle="Sync reads the latest completed run and creates review items. It never runs automatically.">
        <div className="flex flex-wrap items-end gap-3">
          <label className="text-sm">
            <span className="mb-1 block font-medium">Project</span>
            <select
              className={inputClass}
              value={project}
              onChange={(e) => {
                setProject(e.target.value);
                setSelectedId(null);
                setSyncResult(null);
                setSyncError(null);
                resetPage();
              }}
            >
              {projectNames.map((p) => (
                <option key={p} value={p}>
                  {p}
                </option>
              ))}
            </select>
          </label>
          <button type="button" className={primaryButton} onClick={() => void runSync()} disabled={syncing || !project}>
            <RefreshCw className={`mr-1 inline h-4 w-4 ${syncing ? 'animate-spin' : ''}`} aria-hidden="true" />
            {syncing ? 'Syncing…' : 'Sync Review'}
          </button>
        </div>
        {syncError && (
          <div className="mt-3">
            <ErrorBox err={syncError} />
          </div>
        )}
        {syncResult && (
          <div className="mt-3" data-testid="sync-result">
            <Notice tone="info">
              <div>
                Synced <strong>{syncResult.project}</strong>, run <strong>{syncResult.run_id}</strong>: {syncResult.created}{' '}
                created, {syncResult.existing} already existing.
              </div>
              <div className="mt-1 text-xs">
                Active rules:{' '}
                {syncResult.active_rules.length ? syncResult.active_rules.map((r) => RULE_LABEL[r] ?? r).join('; ') : 'none'}
              </div>
            </Notice>
          </div>
        )}
      </Card>

      <Card title="Queue">
        <div className="mb-3 flex flex-wrap items-end gap-3">
          <label className="text-sm">
            <span className="mb-1 block font-medium">Status</span>
            <select
              className={inputClass}
              value={status}
              onChange={(e) => {
                setStatus(e.target.value as ReviewStatus | '');
                resetPage();
              }}
            >
              <option value="">All</option>
              {STATUSES.map((s) => (
                <option key={s} value={s}>
                  {STATUS_OPTION[s]}
                </option>
              ))}
            </select>
          </label>
          <label className="text-sm">
            <span className="mb-1 block font-medium">Kind</span>
            <select
              className={inputClass}
              value={kind}
              onChange={(e) => {
                setKind(e.target.value as ReviewKind | '');
                resetPage();
              }}
            >
              <option value="">All</option>
              <option value="anomaly">{KIND_LABEL.anomaly}</option>
              <option value="dq">{KIND_LABEL.dq}</option>
            </select>
          </label>
          <label className="text-sm">
            <span className="mb-1 block font-medium">Run</span>
            <input
              className={`${inputClass} w-72 max-w-full`}
              spellCheck={false}
              value={runId}
              aria-invalid={runInvalid}
              onChange={(e) => {
                setRunId(e.target.value);
                resetPage();
              }}
              placeholder="any"
            />
          </label>
          <label className="text-sm">
            <span className="mb-1 block font-medium">Superseded</span>
            <select
              className={inputClass}
              value={superseded}
              onChange={(e) => {
                setSuperseded(e.target.value as '' | 'true' | 'false');
                resetPage();
              }}
            >
              <option value="">All</option>
              <option value="false">Current only</option>
              <option value="true">Superseded only</option>
            </select>
          </label>
          <button type="button" className={secondaryButton} onClick={() => void load()} disabled={loading}>
            Refresh
          </button>
        </div>

        {runInvalid && <Notice tone="warning">Run id may contain only letters, digits, "-" and "_" (up to 64 characters).</Notice>}
        {listError && (
          <div className="space-y-2">
            <ErrorBox err={listError} />
            <button type="button" className={secondaryButton} onClick={() => void load()}>
              Retry
            </button>
          </div>
        )}
        {loading && !listError && <p role="status">Loading review items…</p>}

        {!loading && !listError && !runInvalid && items.length === 0 && (
          <div data-testid="review-empty" className="text-sm text-slate-600 dark:text-slate-300">
            {filtered
              ? 'No review items match these filters.'
              : 'No review items yet. Use “Sync Review” to create them from the latest completed run. Reports written by older versions have no impact level and produce no items.'}
          </div>
        )}

        {items.length > 0 && (
          <ul className="divide-y divide-slate-200 dark:divide-slate-700" aria-label="Review items">
            {items.map((it) => (
              <li key={it.id} data-testid="review-row">
                <button
                  type="button"
                  className={`flex w-full flex-wrap items-center gap-2 px-2 py-2 text-left text-sm hover:bg-slate-50 dark:hover:bg-slate-800 ${
                    selectedId === it.id ? 'bg-slate-100 dark:bg-slate-800' : ''
                  }`}
                  aria-current={selectedId === it.id ? 'true' : undefined}
                  onClick={() => setSelectedId(it.id)}
                >
                  <StatusBadge status={it.status} />
                  <span className="font-medium">{it.ref}</span>
                  <span className="text-xs text-slate-500 dark:text-slate-400">
                    {KIND_LABEL[it.kind]} · {RULE_LABEL[it.rule_id] ?? it.rule_id} · run {it.run_id}
                  </span>
                  {it.superseded && (
                    <span className="rounded-full border border-orange-400 px-2 py-0.5 text-xs font-semibold text-orange-800 dark:text-orange-300">
                      Superseded
                    </span>
                  )}
                </button>
              </li>
            ))}
          </ul>
        )}

        {total > 0 && (
          <div className="mt-3 flex items-center gap-3 text-sm" data-testid="review-pagination">
            <button
              type="button"
              className={secondaryButton}
              disabled={offset === 0 || loading}
              onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
            >
              Previous
            </button>
            <span>
              Page {page} of {pages} · {total} item{total === 1 ? '' : 's'}
            </span>
            <button
              type="button"
              className={secondaryButton}
              disabled={offset + PAGE_SIZE >= total || loading}
              onClick={() => setOffset(offset + PAGE_SIZE)}
            >
              Next
            </button>
          </div>
        )}
      </Card>

      {selectedId !== null && <ItemDetail key={selectedId} itemId={selectedId} onChanged={() => void load()} />}
    </div>
  );
}
