import type { DatasetProfile } from '../utils/csvProfiler';
import type { Cell, ColumnKind, Dataset, DatasetColumn, DatasetOrigin, Row } from './types';

/** Same tokens the profiler treats as "no value". */
const NULL_TOKENS = new Set(['', 'null', 'nan', 'none', 'na']);
const NUMBER = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
const TRUE_TOKENS = new Set(['true', '1', 'yes']);
const FALSE_TOKENS = new Set(['false', '0', 'no']);
const ISO_DATE =
  /^(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?/;

export function isNullToken(value: unknown): boolean {
  if (value === null || value === undefined) return true;
  return NULL_TOKENS.has(String(value).trim().toLowerCase());
}

/** Strict decimal parser: no hex, no Infinity, no thousands separators. */
export function parseNumber(text: string): number | null {
  const trimmed = text.trim();
  if (!NUMBER.test(trimmed)) return null;
  const value = Number(trimmed);
  return Number.isFinite(value) ? value : null;
}

export function parseBoolean(text: string): boolean | null {
  const token = text.trim().toLowerCase();
  if (TRUE_TOKENS.has(token)) return true;
  if (FALSE_TOKENS.has(token)) return false;
  return null;
}

const pad = (value: number, width = 2) => String(value).padStart(width, '0');

/**
 * Normalise a date to an ISO string (YYYY-MM-DD, with THH:mm:ss when a time is present) so dates sort
 * and group correctly. Returns null for anything that is not a real calendar date.
 */
export function normalizeDate(text: string): string | null {
  const trimmed = text.trim();
  const match = ISO_DATE.exec(trimmed);
  if (match) {
    const [, y, m, d, hh, mm, ss] = match;
    const year = Number(y);
    const month = Number(m);
    const day = Number(d);
    const check = new Date(Date.UTC(year, month - 1, day));
    if (
      check.getUTCFullYear() !== year ||
      check.getUTCMonth() !== month - 1 ||
      check.getUTCDate() !== day
    ) {
      return null;
    }
    const date = `${pad(year, 4)}-${pad(month)}-${pad(day)}`;
    if (hh === undefined) return date;
    if (Number(hh) > 23 || Number(mm) > 59 || Number(ss ?? 0) > 59) return null;
    return `${date}T${hh}:${mm}:${ss ?? '00'}`;
  }
  if (trimmed.length >= 8) {
    const parsed = new Date(trimmed);
    if (!Number.isNaN(parsed.getTime())) {
      return `${pad(parsed.getFullYear(), 4)}-${pad(parsed.getMonth() + 1)}-${pad(parsed.getDate())}`;
    }
  }
  return null;
}

function kindOf(dtype: string): ColumnKind {
  if (dtype === 'integer' || dtype === 'float') return 'numeric';
  if (dtype === 'date') return 'date';
  if (dtype === 'boolean') return 'boolean';
  return 'categorical';
}

function readCell(text: unknown, kind: ColumnKind): { value: Cell; invalid: boolean } {
  if (isNullToken(text)) return { value: null, invalid: false };
  const raw = String(text);
  if (kind === 'numeric') {
    const value = parseNumber(raw);
    return value === null ? { value: null, invalid: true } : { value, invalid: false };
  }
  if (kind === 'date') {
    const value = normalizeDate(raw);
    return value === null ? { value: null, invalid: true } : { value, invalid: false };
  }
  if (kind === 'boolean') {
    const value = parseBoolean(raw);
    return value === null ? { value: null, invalid: true } : { value, invalid: false };
  }
  return { value: raw.trim(), invalid: false };
}

/**
 * Turn a profiled CSV into typed rows. Column types come from the profiler so the analysis agrees
 * with the Data Quality screen; values that do not fit their column's type become missing and are
 * counted in `invalidCount`.
 */
export function buildDataset(profile: DatasetProfile, origin: DatasetOrigin, id: number): Dataset {
  const kinds = new Map(profile.columns.map((c) => [c.name, kindOf(c.dtype)]));
  const stats = profile.headers.map((name) => ({
    name,
    kind: kinds.get(name) ?? ('categorical' as ColumnKind),
    nullCount: 0,
    invalidCount: 0,
    distinct: new Set<string | number | boolean>(),
    min: undefined as number | string | undefined,
    max: undefined as number | string | undefined,
  }));

  const rows: Row[] = new Array(profile.allRows.length);
  profile.allRows.forEach((source, rowIndex) => {
    const row: Row = {};
    for (const column of stats) {
      const { value, invalid } = readCell(source[column.name], column.kind);
      row[column.name] = value;
      if (value === null) {
        column.nullCount += 1;
        if (invalid) column.invalidCount += 1;
        continue;
      }
      column.distinct.add(value);
      if (column.kind === 'numeric' || column.kind === 'date') {
        if (column.min === undefined || value < column.min) column.min = value as number | string;
        if (column.max === undefined || value > column.max) column.max = value as number | string;
      }
    }
    rows[rowIndex] = row;
  });

  const columns: DatasetColumn[] = stats.map((s) => ({
    name: s.name,
    kind: s.kind,
    nullCount: s.nullCount,
    invalidCount: s.invalidCount,
    distinctCount: s.distinct.size,
    ...(s.min !== undefined ? { min: s.min } : {}),
    ...(s.max !== undefined ? { max: s.max } : {}),
  }));

  return { id, fileName: profile.fileName, origin, columns, rows };
}

export function findColumn(dataset: Dataset, name: string): DatasetColumn | undefined {
  return dataset.columns.find((c) => c.name === name);
}
