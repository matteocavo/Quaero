import { describe, expect, it } from 'vitest';
import { ApiError } from '../../services/apiClient';
import type { ReviewItemDetail } from '../../types/api';
import {
  ALLOWED_DECISIONS,
  RequestIdKeeper,
  buildRequest,
  describeError,
  formatWhen,
  isTerminal,
  newRequestId,
  reasonRequired,
  show,
  validateDraft,
  type DecisionDraft,
} from './logic';

function item(over: Partial<ReviewItemDetail> = {}): ReviewItemDetail {
  return {
    id: 1,
    project: 'p',
    run_id: 3,
    kind: 'anomaly',
    ref: 'amount|iqr',
    rule_id: 'R1',
    status: 'REVIEW_REQUIRED',
    superseded: false,
    snapshot_verified: true,
    artifact_provenance_verified: true,
    created_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
    original: { impact: 'high' },
    effective: { impact: 'high' },
    correction: null,
    artifact_sha256: 'x',
    ...over,
  } as ReviewItemDetail;
}

const draft = (over: Partial<DecisionDraft> = {}): DecisionDraft => ({
  decision: 'ACCEPTED',
  reviewer: 'Ada',
  reason: '',
  toValue: null,
  ...over,
});

describe('transitions', () => {
  it('allows the documented transitions only', () => {
    expect(ALLOWED_DECISIONS.REVIEW_REQUIRED).toEqual(['ACCEPTED', 'CORRECTED', 'REJECTED']);
    expect(ALLOWED_DECISIONS.CORRECTED).toEqual(['ACCEPTED', 'REJECTED']);
    expect(isTerminal('ACCEPTED')).toBe(true);
    expect(isTerminal('REJECTED')).toBe(true);
    expect(isTerminal('REVIEW_REQUIRED')).toBe(false);
    expect(isTerminal('CORRECTED')).toBe(false);
  });
  it('requires a reason for CORRECTED and REJECTED only', () => {
    expect(reasonRequired('CORRECTED')).toBe(true);
    expect(reasonRequired('REJECTED')).toBe(true);
    expect(reasonRequired('ACCEPTED')).toBe(false);
  });
});

describe('describeError', () => {
  it('keeps the code and explains known codes', () => {
    const d = describeError(new ApiError('boom', 409, false, true, false, 'unmanaged_write'));
    expect(d.code).toBe('unmanaged_write');
    expect(d.explanation).toMatch(/outside the API/);
    expect(d.uncertain).toBe(false);
  });
  it('treats 5xx, 503 and network failures as uncertain', () => {
    expect(describeError(new ApiError('x', 503)).uncertain).toBe(true);
    expect(describeError(new ApiError('x', 500, false, false, false, 'rules_config_invalid')).uncertain).toBe(true);
    expect(describeError(new TypeError('failed')).uncertain).toBe(true);
  });
  it('describes 422 generically', () => {
    expect(describeError(new ApiError('x', 422)).explanation).toMatch(/invalid/);
  });
});

describe('validateDraft', () => {
  it('blocks disallowed decisions from terminal states', () => {
    expect(validateDraft(item({ status: 'ACCEPTED' }), draft())).toMatch(/not allowed/);
    expect(validateDraft(item({ status: 'CORRECTED' }), draft({ decision: 'CORRECTED', toValue: 'low', reason: 'r' }))).toMatch(
      /not allowed/,
    );
  });
  it('validates reviewer', () => {
    expect(validateDraft(item(), draft({ reviewer: '  ' }))).toMatch(/reviewer/i);
    expect(validateDraft(item(), draft({ reviewer: 'a'.repeat(81) }))).toMatch(/80/);
    expect(validateDraft(item(), draft({ reviewer: 'a\u0007b' }))).toMatch(/control/);
    expect(validateDraft(item(), draft())).toBeNull();
  });
  it('requires a reason for reject and correct', () => {
    expect(validateDraft(item(), draft({ decision: 'REJECTED' }))).toMatch(/reason/);
    expect(validateDraft(item(), draft({ decision: 'REJECTED', reason: '  ' }))).toMatch(/reason/);
    expect(validateDraft(item(), draft({ decision: 'REJECTED', reason: 'no' }))).toBeNull();
  });
  it('validates corrections for anomaly impact and dq outlier_flag', () => {
    const a = item();
    expect(validateDraft(a, draft({ decision: 'CORRECTED', reason: 'r' }))).toMatch(/Choose/);
    expect(validateDraft(a, draft({ decision: 'CORRECTED', reason: 'r', toValue: 'high' }))).toMatch(/equals/);
    expect(validateDraft(a, draft({ decision: 'CORRECTED', reason: 'r', toValue: 'low' }))).toBeNull();
    const dq = item({ kind: 'dq', original: { outlier_flag: true }, effective: { outlier_flag: true } });
    expect(validateDraft(dq, draft({ decision: 'CORRECTED', reason: 'r', toValue: true }))).toMatch(/equals/);
    expect(validateDraft(dq, draft({ decision: 'CORRECTED', reason: 'r', toValue: false }))).toBeNull();
  });
});

describe('buildRequest', () => {
  it('sends expected_status equal to the displayed status', () => {
    const body = buildRequest(item({ status: 'CORRECTED' }), draft({ reviewer: ' Ada ' }), 'ui-abcdefgh');
    expect(body).toEqual({
      decision: 'ACCEPTED',
      reviewer: 'Ada',
      expected_status: 'CORRECTED',
      client_request_id: 'ui-abcdefgh',
    });
  });
  it('adds the correction with the right field per kind', () => {
    const a = buildRequest(item(), draft({ decision: 'CORRECTED', reason: ' why ', toValue: 'low' }), 'ui-abcdefgh');
    expect(a.correction).toEqual({ field: 'impact', to_value: 'low' });
    expect(a.reason).toBe('why');
    const dq = buildRequest(item({ kind: 'dq' }), draft({ decision: 'CORRECTED', reason: 'r', toValue: false }), 'ui-abcdefgh');
    expect(dq.correction).toEqual({ field: 'outlier_flag', to_value: false });
  });
});

describe('RequestIdKeeper', () => {
  const body = (reviewer: string) => ({
    decision: 'ACCEPTED' as const,
    reviewer,
    expected_status: 'REVIEW_REQUIRED' as const,
  });
  it('reuses the id for the same intent and changes it for another payload', () => {
    let n = 0;
    const k = new RequestIdKeeper(() => `id-${++n}-xxxxxxx`);
    const a = k.idFor(1, body('Ada'));
    expect(k.idFor(1, body('Ada'))).toBe(a);
    const b = k.idFor(1, body('Bob'));
    expect(b).not.toBe(a);
    expect(k.idFor(2, body('Bob'))).not.toBe(b);
  });
  it('issues a fresh id after release', () => {
    const k = new RequestIdKeeper();
    const a = k.idFor(1, body('Ada'));
    k.release();
    expect(k.idFor(1, body('Ada'))).not.toBe(a);
  });
  it('generates ids matching the server pattern', () => {
    expect(newRequestId()).toMatch(/^[A-Za-z0-9_.:-]{8,64}$/);
  });
});

describe('formatting', () => {
  it('formats dates in UTC and tolerates garbage', () => {
    expect(formatWhen('2026-03-04T05:06:07Z')).toBe('2026-03-04 05:06:07 UTC');
    expect(formatWhen('nope')).toBe('nope');
  });
  it('shows strings as is and others as JSON', () => {
    expect(show('<b>x</b>')).toBe('<b>x</b>');
    expect(show(true)).toBe('true');
  });
});
