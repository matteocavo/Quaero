import type { ReviewStatus } from '../../types/api';
import { STATUS_LABEL } from './logic';

const TONE: Record<ReviewStatus, string> = {
  REVIEW_REQUIRED:
    'border-amber-300 bg-amber-50 text-amber-900 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-200',
  CORRECTED: 'border-sky-300 bg-sky-50 text-sky-900 dark:border-sky-700 dark:bg-sky-950/40 dark:text-sky-200',
  ACCEPTED:
    'border-emerald-300 bg-emerald-50 text-emerald-900 dark:border-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-200',
  REJECTED: 'border-slate-400 bg-slate-100 text-slate-800 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200',
};

/** REVIEW_REQUIRED is a normal state awaiting a human, not a pipeline error. */
export function StatusBadge({ status }: { status: ReviewStatus }) {
  return (
    <span className={`rounded-full border px-2 py-0.5 text-xs font-semibold ${TONE[status]}`} data-status={status}>
      {STATUS_LABEL[status]}
    </span>
  );
}
