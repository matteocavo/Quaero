import { ApiError } from '../../services/apiClient';
import type { ReviewDecision, ReviewDecisionRequest, ReviewItemDetail, ReviewKind, ReviewStatus } from '../../types/api';

/** Decisions the server accepts from each status. ACCEPTED and REJECTED are terminal. */
export const ALLOWED_DECISIONS: Record<ReviewStatus, ReviewDecision[]> = {
  REVIEW_REQUIRED: ['ACCEPTED', 'CORRECTED', 'REJECTED'],
  CORRECTED: ['ACCEPTED', 'REJECTED'],
  ACCEPTED: [],
  REJECTED: [],
};

export const STATUS_LABEL: Record<ReviewStatus, string> = {
  REVIEW_REQUIRED: 'Review required',
  ACCEPTED: 'Accepted',
  CORRECTED: 'Corrected',
  REJECTED: 'Rejected',
};

export const KIND_LABEL: Record<ReviewKind, string> = {
  anomaly: 'Anomaly',
  dq: 'Data quality',
};

export const RULE_LABEL: Record<string, string> = {
  R1: 'R1 · high-impact anomaly',
  R2: 'R2 · medium-impact anomaly',
  R3: 'R3 · outlier flag on a column',
  R4: 'R4 · quality below threshold',
};

/** The only field a reviewer may correct, per item kind, with its allowed values. */
export const CORRECTION_FIELDS: Record<ReviewKind, { field: string; values: (string | boolean)[] }> = {
  anomaly: { field: 'impact', values: ['low', 'medium', 'high'] },
  dq: { field: 'outlier_flag', values: [true, false] },
};

export function isTerminal(status: ReviewStatus): boolean {
  return ALLOWED_DECISIONS[status].length === 0;
}

export function reasonRequired(decision: ReviewDecision): boolean {
  return decision === 'CORRECTED' || decision === 'REJECTED';
}

/** Plain-language explanation of a stable server error code. The code itself is always shown too. */
const ERROR_EXPLANATIONS: Record<string, string> = {
  binding_unverifiable: 'The run has no provenance record linking its artifacts to it, so they cannot be trusted yet.',
  unmanaged_write: 'The artifact was last written outside the API (for example from the command line), so it is not trusted.',
  artifact_changed: 'The artifact on disk no longer matches the fingerprint recorded when the run finished.',
  artifact_unavailable: 'The artifact file could not be read or is not valid JSON.',
  run_active: 'A run or write is in progress for this project. Wait for it to finish, then try again.',
  run_mismatch: 'The artifact on disk belongs to a different run than the one being synchronised.',
  no_completed_run: 'This project has no completed run yet. Run the pipeline first.',
  rules_config_invalid: 'The review rules file (review_rules.json) is missing or malformed. Fix it and try again.',
  invalid_project: 'The project name is not valid.',
  stale_status: 'Someone else already changed this item. Its current state has been reloaded; review it and decide again.',
  invalid_transition: 'That decision is not allowed from the item’s current status.',
  request_id_conflict: 'This request identifier was already used for a different decision. Nothing was changed.',
  invalid_correction: 'The correction was rejected: only the permitted field can change, to a different permitted value.',
  not_found: 'The review item no longer exists.',
};

export interface DescribedError {
  code: string | null;
  status: number | null;
  message: string;
  explanation: string | null;
  /** True when the server may or may not have processed the request (network/timeout/5xx). */
  uncertain: boolean;
}

export function describeError(err: unknown): DescribedError {
  if (err instanceof ApiError) {
    const code = err.code ?? null;
    const status = err.statusCode ?? 0;
    const uncertain = status === 503 || status === 504 || status === 0 || status >= 500;
    const explanation =
      code && ERROR_EXPLANATIONS[code]
        ? ERROR_EXPLANATIONS[code]
        : status === 422
          ? 'The server rejected the request as invalid.'
          : uncertain
            ? 'The server did not answer reliably. Check that Local Operations is running.'
            : null;
    return { code, status, message: err.message, explanation, uncertain };
  }
  return {
    code: null,
    status: null,
    message: err instanceof Error ? err.message : 'Unexpected error.',
    explanation: null,
    uncertain: true,
  };
}

export function newRequestId(): string {
  const rnd =
    typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
      ? crypto.randomUUID()
      : `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 12)}`;
  return `ui-${rnd}`.slice(0, 64);
}

export interface DecisionDraft {
  decision: ReviewDecision;
  reviewer: string;
  reason: string;
  /** Selected corrected value (strings for impact, booleans for outlier_flag); null when not correcting. */
  toValue: string | boolean | null;
}

export function validateDraft(item: ReviewItemDetail, d: DecisionDraft): string | null {
  if (!ALLOWED_DECISIONS[item.status].includes(d.decision)) return 'This decision is not allowed from the current status.';
  const reviewer = d.reviewer.trim();
  if (!reviewer) return 'Enter the reviewer name.';
  if (reviewer.length > 80) return 'The reviewer name must be at most 80 characters.';
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(reviewer)) return 'The reviewer name must not contain control characters.';
  if (reasonRequired(d.decision) && !d.reason.trim()) return 'A reason is required for this decision.';
  if (d.decision === 'CORRECTED') {
    const spec = CORRECTION_FIELDS[item.kind];
    if (d.toValue === null) return 'Choose the corrected value.';
    if (d.toValue === item.original[spec.field]) return 'The corrected value equals the original value.';
  }
  return null;
}

export function buildRequest(item: ReviewItemDetail, d: DecisionDraft, clientRequestId: string): ReviewDecisionRequest {
  const body: ReviewDecisionRequest = {
    decision: d.decision,
    reviewer: d.reviewer.trim(),
    expected_status: item.status as 'REVIEW_REQUIRED' | 'CORRECTED',
    client_request_id: clientRequestId,
  };
  const reason = d.reason.trim();
  if (reason) body.reason = reason;
  if (d.decision === 'CORRECTED' && d.toValue !== null) {
    body.correction = { field: CORRECTION_FIELDS[item.kind].field, to_value: d.toValue };
  }
  return body;
}

/**
 * Keeps one client_request_id per decision intent. The same intent (item, status shown, payload)
 * reuses its id so a retry is idempotent; any change of payload gets a fresh id, so an id is never
 * reused for a different request. `release` is called once the server has answered definitively.
 */
export class RequestIdKeeper {
  private key: string | null = null;
  private id: string | null = null;

  constructor(private readonly make: () => string = newRequestId) {}

  idFor(itemId: number, body: Omit<ReviewDecisionRequest, 'client_request_id'>): string {
    const key = JSON.stringify([itemId, body]);
    if (this.key !== key || !this.id) {
      this.key = key;
      this.id = this.make();
    }
    return this.id;
  }

  release(): void {
    this.key = null;
    this.id = null;
  }
}

export function formatWhen(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return `${d.toISOString().replace('T', ' ').slice(0, 19)} UTC`;
}

export function show(value: unknown): string {
  return typeof value === 'string' ? value : JSON.stringify(value);
}
