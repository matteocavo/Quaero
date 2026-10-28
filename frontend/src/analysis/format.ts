import type { Cell } from './types';

/** Human-friendly number: thousands separators, at most 4 decimals, no scientific notation noise. */
export function formatNumber(value: number): string {
  if (!Number.isFinite(value)) return '—';
  if (Number.isInteger(value)) return value.toLocaleString('en-US');
  return value.toLocaleString('en-US', { maximumFractionDigits: 4 });
}

/** What a cell looks like in a table. Missing values are shown as a dash, never as 0 or "null". */
export function formatCell(cell: Cell): string {
  if (cell === null) return '—';
  if (typeof cell === 'number') return formatNumber(cell);
  if (typeof cell === 'boolean') return cell ? 'true' : 'false';
  return cell;
}

export function truncate(text: string, max: number): string {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}
