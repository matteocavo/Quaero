import { useCallback, useEffect, useRef, useState } from 'react';
import { CheckCircle2, ShieldAlert, ShieldCheck } from 'lucide-react';
import { useApp } from '../../context/AppContext';
import type { ReviewDecision, ReviewEvent, ReviewItemDetail } from '../../types/api';
import { Card, Notice, inputClass, primaryButton, secondaryButton } from '../analysis/ui';
import {
  ALLOWED_DECISIONS,
  CORRECTION_FIELDS,
  KIND_LABEL,
  RULE_LABEL,
  RequestIdKeeper,
  STATUS_LABEL,
  buildRequest,
  describeError,
  formatWhen,
  isTerminal,
  reasonRequired,
  show,
  validateDraft,
  type DescribedError,
} from './logic';
import { StatusBadge } from './StatusBadge';

const REVIEWER_KEY = 'quaero.review.reviewer';

function readReviewer(): string {
  try {
    return localStorage.getItem(REVIEWER_KEY) ?? '';
  } catch {
    return '';
  }
}

function Integrity({ label, ok, okText, badText }: { label: string; ok: boolean; okText: string; badText: string }) {
  const Icon = ok ? ShieldCheck : ShieldAlert;
  return (
    <div
      data-testid={`integrity-${label}`}
      role={ok ? 'status' : 'alert'}
      className={
        ok
          ? 'flex items-start gap-2 rounded-xl border border-emerald-300 bg-emerald-50 p-3 text-xs text-emerald-900 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-200'
          : 'flex items-start gap-2 rounded-xl border-2 border-red-500 bg-red-50 p-3 text-xs font-medium text-red-900 dark:border-red-500 dark:bg-red-950/40 dark:text-red-200'
      }
    >
      <Icon className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
      <div>
        <div className="font-semibold">
          {label}: {ok ? 'verified' : 'NOT verified'}
        </div>
        <div className="font-normal">{ok ? okText : badText}</div>
      </div>
    </div>
  );
}

function Json({ value, label }: { value: unknown; label: string }) {
  // Rendered strictly as text: artifact content is data, never markup.
  return (
    <pre
      aria-label={label}
      className="max-h-64 overflow-auto rounded-xl bg-slate-100 p-3 text-xs text-slate-800 dark:bg-slate-900 dark:text-slate-200"
    >
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

interface Props {
  itemId: number;
  onChanged: () => void;
}

export function ItemDetail({ itemId, onChanged }: Props) {
  const { apiClient } = useApp();
  const [item, setItem] = useState<ReviewItemDetail | null>(null);
  const [events, setEvents] = useState<ReviewEvent[]>([]);
  const [loadError, setLoadError] = useState<DescribedError | null>(null);
  const [loading, setLoading] = useState(false);
  const [decision, setDecision] = useState<ReviewDecision | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const [detail, evs] = await Promise.all([apiClient.getReviewItem(itemId), apiClient.getReviewEvents(itemId)]);
      setItem(detail);
      setEvents(evs);
    } catch (e) {
      setItem(null);
      setLoadError(describeError(e));
    } finally {
      setLoading(false);
    }
  }, [apiClient, itemId]);

  useEffect(() => {
    setSuccess(null);
    setDecision(null);
    void load();
  }, [load]);

  if (loading && !item) return <p className="text-xs text-slate-500">Loading item…</p>;
  if (loadError)
    return (
      <Notice tone="error">
        <div>
          Could not load the item{loadError.code ? ` (${loadError.code})` : ''}: {loadError.message}
        </div>
        <button type="button" className={`${secondaryButton} mt-2`} onClick={() => void load()}>
          Retry
        </button>
      </Notice>
    );
  if (!item) return null;

  const allowed = ALLOWED_DECISIONS[item.status];

  return (
    <div className="space-y-4" data-testid="review-detail">
      <Card title={`${KIND_LABEL[item.kind]} · ${item.ref}`} subtitle={`${item.project} · run ${item.run_id}`}>
        <div className="flex flex-wrap items-center gap-2">
          <StatusBadge status={item.status} />
          {item.superseded && (
            <span
              data-testid="superseded-badge"
              className="rounded-full border border-amber-400 bg-amber-100 px-2 py-0.5 text-xs font-semibold text-amber-900 dark:border-amber-600 dark:bg-amber-950/50 dark:text-amber-200"
            >
              Superseded
            </span>
          )}
          <span className="text-xs text-slate-600 dark:text-slate-300">
            Rule {RULE_LABEL[item.rule_id] ?? item.rule_id}
          </span>
        </div>
        {item.superseded && (
          <div className="mt-3">
            <Notice tone="warning">
              A newer run replaced this item. You can still read it, but it describes an older run
              {isTerminal(item.status) ? '.' : ' and decisions on it are likely out of date.'}
            </Notice>
          </div>
        )}
        <dl className="mt-3 grid grid-cols-2 gap-2 text-xs text-slate-600 dark:text-slate-300">
          <div>
            <dt className="font-semibold">Created</dt>
            <dd>{formatWhen(item.created_at)}</dd>
          </div>
          <div>
            <dt className="font-semibold">Updated</dt>
            <dd>{formatWhen(item.updated_at)}</dd>
          </div>
          <div className="col-span-2">
            <dt className="font-semibold">Artifact SHA-256</dt>
            <dd className="break-all font-mono">{item.artifact_sha256}</dd>
          </div>
        </dl>
      </Card>

      <Card title="Integrity" subtitle="Two independent checks. Both must pass for the item to be trusted.">
        <div className="grid gap-2 sm:grid-cols-2">
          <Integrity
            label="Snapshot"
            ok={item.snapshot_verified}
            okText="The stored copy of this item matches its recorded hash."
            badText="The stored copy of this item does not match its recorded hash. Do not rely on it."
          />
          <Integrity
            label="Artifact provenance"
            ok={item.artifact_provenance_verified}
            okText="The source artifact matched the fingerprint recorded for its run."
            badText="The source artifact could not be tied to its run. Do not rely on it."
          />
        </div>
      </Card>

      <Card title="Content" subtitle="Original comes from the artifact. Effective applies an active correction.">
        <div className="grid gap-3 md:grid-cols-2">
          <div>
            <h4 className="mb-1 text-xs font-semibold">Original</h4>
            <Json value={item.original} label="Original" />
          </div>
          <div>
            <h4 className="mb-1 text-xs font-semibold">Effective</h4>
            <Json value={item.effective} label="Effective" />
          </div>
        </div>
        {item.correction ? (
          <p className="mt-2 text-xs" data-testid="correction-summary">
            Correction applied: <strong>{item.correction.field}</strong> {show(item.correction.from_value)} →{' '}
            {show(item.correction.to_value)}
          </p>
        ) : (
          <p className="mt-2 text-xs text-slate-500">No correction applies to the effective view.</p>
        )}
      </Card>

      <Card title="Decision">
        {success && (
          <div className="mb-3" role="status">
            <Notice tone="info">
              <span className="inline-flex items-center gap-1">
                <CheckCircle2 className="h-4 w-4" aria-hidden /> {success}
              </span>
            </Notice>
          </div>
        )}
        {isTerminal(item.status) ? (
          <p className="text-xs text-slate-600 dark:text-slate-300" data-testid="terminal-note">
            This item is {STATUS_LABEL[item.status].toLowerCase()} and final. No further decisions are possible.
          </p>
        ) : (
          <div className="flex flex-wrap gap-2">
            {(['ACCEPTED', 'CORRECTED', 'REJECTED'] as ReviewDecision[]).map((d) => (
              <button
                key={d}
                type="button"
                disabled={!allowed.includes(d)}
                title={allowed.includes(d) ? undefined : 'Not allowed from the current status'}
                className={d === 'ACCEPTED' ? primaryButton : secondaryButton}
                onClick={() => setDecision(d)}
              >
                {d === 'ACCEPTED' ? 'Accept' : d === 'CORRECTED' ? 'Correct' : 'Reject'}
              </button>
            ))}
          </div>
        )}
      </Card>

      {decision && (
        <DecisionDialog
          item={item}
          decision={decision}
          onClose={() => setDecision(null)}
          onReload={load}
          onDone={async (msg) => {
            setDecision(null);
            setSuccess(msg);
            await load();
            onChanged();
          }}
        />
      )}

      <Card title="Audit trail" subtitle="Read-only history of every decision on this item.">
        <p className="mb-2 text-xs text-amber-800 dark:text-amber-300" data-testid="identity-note">
          Reviewer names are declared by whoever typed them. They are not authenticated.
        </p>
        {events.length === 0 ? (
          <p className="text-xs text-slate-500">No decisions yet.</p>
        ) : (
          <ol className="space-y-2" aria-label="Decision history">
            {[...events]
              .sort((a, b) => a.at.localeCompare(b.at) || a.id - b.id)
              .map((e) => (
                <li
                  key={e.id}
                  data-testid="review-event"
                  className="rounded-xl border border-slate-200 p-3 text-xs dark:border-slate-700"
                >
                  <div className="font-semibold">
                    {STATUS_LABEL[e.from_status]} → {STATUS_LABEL[e.to_status]}
                  </div>
                  <div className="text-slate-600 dark:text-slate-300">
                    {formatWhen(e.at)} · {e.reviewer} ({e.reviewer_source}, not verified)
                  </div>
                  {e.reason && <div className="mt-1">Reason: {e.reason}</div>}
                </li>
              ))}
          </ol>
        )}
      </Card>
    </div>
  );
}

interface DialogProps {
  item: ReviewItemDetail;
  decision: ReviewDecision;
  onClose: () => void;
  onReload: () => Promise<void>;
  onDone: (message: string) => Promise<void>;
}

function DecisionDialog({ item, decision, onClose, onReload, onDone }: DialogProps) {
  const { apiClient } = useApp();
  const [reviewer, setReviewer] = useState(readReviewer);
  const [reason, setReason] = useState('');
  const [toValue, setToValue] = useState<string | boolean | null>(null);
  const [error, setError] = useState<DescribedError | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const inFlight = useRef(false);
  const keeper = useRef(new RequestIdKeeper());
  const firstField = useRef<HTMLInputElement>(null);
  const spec = CORRECTION_FIELDS[item.kind];

  useEffect(() => {
    firstField.current?.focus();
  }, []);

  const submit = async () => {
    if (inFlight.current) return;
    const draft = { decision, reviewer, reason, toValue };
    const invalid = validateDraft(item, draft);
    setLocalError(invalid);
    if (invalid) return;
    const { client_request_id: _omit, ...shape } = buildRequest(item, draft, '');
    void _omit;
    const body = { ...shape, client_request_id: keeper.current.idFor(item.id, shape) };
    inFlight.current = true;
    setPending(true);
    setError(null);
    try {
      const res = await apiClient.decideReviewItem(item.id, body);
      keeper.current.release();
      try {
        localStorage.setItem(REVIEWER_KEY, body.reviewer);
      } catch {
        /* storage is optional */
      }
      await onDone(
        res.replayed
          ? 'This decision had already been recorded; nothing new was written.'
          : `Decision recorded: ${STATUS_LABEL[res.item.status]}.`,
      );
    } catch (e) {
      const d = describeError(e);
      setError(d);
      if (d.code === 'stale_status' || d.code === 'invalid_transition' || d.code === 'not_found') {
        keeper.current.release();
        await onReload();
      } else if (d.code === 'request_id_conflict' || (d.status !== null && d.status < 500 && !d.uncertain)) {
        keeper.current.release();
      }
    } finally {
      inFlight.current = false;
      setPending(false);
    }
  };

  const stale = error?.code === 'stale_status' || error?.code === 'invalid_transition' || error?.code === 'not_found';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="decision-title"
        onKeyDown={(e) => {
          if (e.key === 'Escape' && !pending) onClose();
        }}
        className="max-h-full w-full max-w-lg overflow-auto rounded-2xl bg-white p-5 shadow-xl dark:bg-slate-900"
      >
        <h3 id="decision-title" className="text-sm font-semibold">
          Confirm: {decision === 'ACCEPTED' ? 'accept' : decision === 'CORRECTED' ? 'correct' : 'reject'} this item
        </h3>
        <p className="mt-1 text-xs text-slate-600 dark:text-slate-300">
          {item.ref} · currently {STATUS_LABEL[item.status]}.{' '}
          {decision !== 'CORRECTED' ? 'This cannot be undone.' : 'You can still accept or reject it afterwards.'}
        </p>
        <div className="mt-3 space-y-3">
          <div>
            <label htmlFor="rv-reviewer" className="text-xs font-semibold">
              Reviewer (declared name, not verified)
            </label>
            <input
              id="rv-reviewer"
              ref={firstField}
              className={inputClass}
              value={reviewer}
              maxLength={80}
              disabled={pending}
              onChange={(e) => setReviewer(e.target.value)}
            />
          </div>
          {decision === 'CORRECTED' && (
            <div>
              <label htmlFor="rv-value" className="text-xs font-semibold">
                Corrected {spec.field} (original: {show(item.original[spec.field])})
              </label>
              <select
                id="rv-value"
                className={inputClass}
                disabled={pending}
                value={toValue === null ? '' : String(toValue)}
                onChange={(e) => {
                  const v = e.target.value;
                  setToValue(v === '' ? null : item.kind === 'dq' ? v === 'true' : v);
                }}
              >
                <option value="">Choose…</option>
                {spec.values.map((v) => (
                  <option key={String(v)} value={String(v)}>
                    {String(v)}
                  </option>
                ))}
              </select>
            </div>
          )}
          <div>
            <label htmlFor="rv-reason" className="text-xs font-semibold">
              Reason {reasonRequired(decision) ? '(required)' : '(optional)'}
            </label>
            <textarea
              id="rv-reason"
              className={inputClass}
              rows={3}
              maxLength={1000}
              disabled={pending}
              value={reason}
              onChange={(e) => setReason(e.target.value)}
            />
          </div>
        </div>

        {localError && (
          <div className="mt-3">
            <Notice tone="error">{localError}</Notice>
          </div>
        )}
        {error && (
          <div className="mt-3" data-testid="decision-error">
            <Notice tone="error">
              <div>
                {error.code ? <code className="font-mono">{error.code}</code> : `HTTP ${error.status ?? '?'}`}:{' '}
                {error.explanation ?? error.message}
              </div>
              {error.uncertain && (
                <div className="mt-1">
                  The server may or may not have recorded this decision. Retrying sends the same request identifier,
                  so it cannot be applied twice. You can also reload the item to check.
                </div>
              )}
              {stale && <div className="mt-1">The item was reloaded. Close this dialog and decide again if needed.</div>}
            </Notice>
          </div>
        )}

        <div className="mt-4 flex flex-wrap justify-end gap-2">
          {error?.uncertain && (
            <button type="button" className={secondaryButton} disabled={pending} onClick={() => void onReload()}>
              Reload item
            </button>
          )}
          <button type="button" className={secondaryButton} disabled={pending} onClick={onClose}>
            Cancel
          </button>
          {!stale && (
            <button type="button" className={primaryButton} disabled={pending} onClick={() => void submit()}>
              {pending ? 'Submitting…' : error?.uncertain ? 'Retry same request' : 'Confirm decision'}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
