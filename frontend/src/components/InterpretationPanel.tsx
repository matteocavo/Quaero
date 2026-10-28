import React from 'react';
import { AlertTriangle, CheckCircle2, Info, XCircle } from 'lucide-react';
import type { InterpretationIssue, QuestionInterpretation, SelectionOrigin } from '../types/api';

const ORIGIN: Record<SelectionOrigin, { label: string; hint: string; className: string }> = {
  user_provided: {
    label: 'you chose',
    hint: 'Selected explicitly. It wins over anything read from the question.',
    className: 'bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200',
  },
  auto_inferred: {
    label: 'read from the question',
    hint: 'Worked out from the wording of the question and the column names.',
    className: 'bg-sky-100 text-sky-900 dark:bg-sky-950 dark:text-sky-200',
  },
  assumed_default: {
    label: 'assumed',
    hint: 'The question does not say. This is a default, not something Quaero read.',
    className: 'bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200',
  },
};

const AGGREGATION_LABEL: Record<string, string> = {
  average: 'Average',
  sum: 'Total (sum)',
  count: 'Count of records',
};

const STATUS: Record<QuestionInterpretation['status'], { title: string; text: string; className: string }> = {
  ready: {
    title: 'Ready',
    text: 'Quaero can answer this as described below.',
    className: 'border-emerald-200 bg-emerald-50 text-emerald-950 dark:border-emerald-900/60 dark:bg-emerald-950/30 dark:text-emerald-100',
  },
  needs_input: {
    title: 'Needs more information',
    text: 'Quaero will not guess. Name or choose what is missing, then check again.',
    className: 'border-amber-200 bg-amber-50 text-amber-950 dark:border-amber-900/60 dark:bg-amber-950/30 dark:text-amber-100',
  },
  unsupported: {
    title: 'Cannot be answered as asked',
    text: 'Nothing will be run. See what is not supported and what to ask instead.',
    className: 'border-rose-200 bg-rose-50 text-rose-950 dark:border-rose-900/60 dark:bg-rose-950/30 dark:text-rose-100',
  },
};

const SEVERITY: Record<InterpretationIssue['severity'], { label: string; className: string }> = {
  unsupported: { label: 'Not supported', className: 'text-rose-800 dark:text-rose-300' },
  needs_input: { label: 'Missing', className: 'text-amber-800 dark:text-amber-300' },
  warning: { label: 'Check', className: 'text-amber-800 dark:text-amber-300' },
  note: { label: 'Note', className: 'text-slate-600 dark:text-slate-300' },
};

const Origin: React.FC<{ origin: SelectionOrigin | null }> = ({ origin }) =>
  origin ? (
    <span
      title={ORIGIN[origin].hint}
      className={`rounded px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wide ${ORIGIN[origin].className}`}
    >
      {ORIGIN[origin].label}
    </span>
  ) : null;

const Row: React.FC<{ label: string; children: React.ReactNode }> = ({ label, children }) => (
  <div className="flex flex-col gap-0.5 border-b border-slate-100 py-1.5 last:border-b-0 dark:border-slate-800 sm:flex-row sm:items-baseline sm:gap-3">
    <dt className="w-36 shrink-0 text-[11px] font-semibold text-slate-500 dark:text-slate-400">{label}</dt>
    <dd className="flex min-w-0 flex-wrap items-center gap-2 text-xs text-slate-900 dark:text-slate-100">{children}</dd>
  </div>
);

/**
 * What Quaero will do with a question, or why it will not. Shows the question as written, each choice with where
 * it came from, and every issue with what to do about it. There are no scores: a choice is made by the user, read
 * from the question, or assumed because nothing was said.
 */
export const InterpretationPanel: React.FC<{
  interpretation: QuestionInterpretation;
  /** When given, the suggested questions become buttons that put the question back in the form. */
  onUseQuestion?: (question: string) => void;
}> = ({ interpretation: i, onUseQuestion }) => {
  const status = STATUS[i.status];
  const blocking = i.issues.filter((x) => x.severity === 'unsupported' || x.severity === 'needs_input');
  const informing = i.issues.filter((x) => x.severity === 'warning' || x.severity === 'note');
  const measure = i.metric.column === 'record_count' ? 'Number of records' : i.metric.column;

  return (
    <section data-testid="interpretation-panel" aria-label="How Quaero reads your question" className={`rounded-2xl border p-4 ${status.className}`}>
      <div className="flex items-start gap-2">
        {i.status === 'ready' ? (
          <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400" />
        ) : i.status === 'needs_input' ? (
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
        ) : (
          <XCircle className="mt-0.5 h-4 w-4 shrink-0 text-rose-600 dark:text-rose-400" />
        )}
        <div>
          <h4 data-testid="interpretation-status" className="text-sm font-extrabold">
            {status.title}
          </h4>
          <p className="text-xs">{status.text}</p>
        </div>
      </div>

      <dl className="mt-3 rounded-xl border border-slate-200/70 bg-white/70 px-3 py-1 dark:border-slate-700 dark:bg-slate-900/60">
        <Row label="Your question">
          <span className="break-words italic">“{i.question}”</span>
        </Row>
        <Row label="Measure">
          {measure ? (
            <>
              <span className="font-mono font-semibold">{measure}</span>
              <Origin origin={i.metric.origin} />
            </>
          ) : (
            <span className="text-slate-500">not determined</span>
          )}
        </Row>
        <Row label="Grouped by">
          {i.dimension.column ? (
            <>
              <span className="font-mono font-semibold">{i.dimension.column}</span>
              <Origin origin={i.dimension.origin} />
            </>
          ) : (
            <span className="text-slate-500">not determined</span>
          )}
        </Row>
        <Row label="Aggregation">
          {i.aggregation.value ? (
            <>
              <span className="font-semibold">{AGGREGATION_LABEL[i.aggregation.value] ?? i.aggregation.value}</span>
              <Origin origin={i.aggregation.origin} />
            </>
          ) : (
            <span className="text-slate-500">not determined</span>
          )}
        </Row>
        <Row label="Order">
          <span className="font-semibold">{i.ordering.value === 'lowest' ? 'Lowest first' : 'Highest first'}</span>
          <Origin origin={i.ordering.origin} />
        </Row>
        <Row label="Time grouping">
          {i.time.grouping === 'exact_values' ? (
            <span>Exact values of the date column (not months or years)</span>
          ) : (
            <span>None</span>
          )}
        </Row>
        <Row label="Trend time column">
          {i.time.column ? (
            <>
              <span className="font-mono">{i.time.column}</span>
              <Origin origin={i.time.origin} />
              <span className="text-[11px] text-slate-500 dark:text-slate-400">only for the time-trend mart</span>
            </>
          ) : (
            <span className="text-slate-500">none found</span>
          )}
        </Row>
        <Row label="Filters">
          {i.filters.requested ? <span className="font-semibold text-rose-700 dark:text-rose-300">requested, not supported</span> : <span>none requested</span>}
        </Row>
      </dl>

      {i.summary && (
        <p data-testid="interpretation-summary" className="mt-3 text-xs font-semibold">
          {i.summary}
        </p>
      )}

      {[...blocking, ...informing].length > 0 && (
        <ul data-testid="interpretation-issues" className="mt-3 space-y-1.5 text-xs">
          {[...blocking, ...informing].map((issue, index) => (
            <li key={`${issue.code}-${index}`} className="flex items-start gap-1.5">
              <Info className={`mt-0.5 h-3.5 w-3.5 shrink-0 ${SEVERITY[issue.severity].className}`} />
              <span>
                <span className={`font-bold ${SEVERITY[issue.severity].className}`}>{SEVERITY[issue.severity].label}: </span>
                {issue.message}
                {issue.suggestion && <span className="text-slate-700 dark:text-slate-300"> {issue.suggestion}</span>}
              </span>
            </li>
          ))}
        </ul>
      )}

      {i.suggested_questions.length > 0 && (
        <div className="mt-3">
          <div className="text-[11px] font-bold">You could ask instead</div>
          <div className="mt-1 flex flex-wrap gap-1.5">
            {i.suggested_questions.map((q) =>
              onUseQuestion ? (
                <button
                  key={q}
                  type="button"
                  onClick={() => onUseQuestion(q)}
                  className="rounded-lg border border-slate-300 bg-white px-2 py-1 text-[11px] font-semibold text-slate-800 hover:bg-sky-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100"
                >
                  {q}
                </button>
              ) : (
                <span key={q} className="rounded-lg border border-slate-300 bg-white px-2 py-1 text-[11px] dark:border-slate-700 dark:bg-slate-800">
                  {q}
                </span>
              ),
            )}
          </div>
        </div>
      )}
    </section>
  );
};
