/**
 * RFC 4180 CSV reading and writing used by the browser-side import and every export.
 *
 * Reading handles quoted fields, escaped quotes (""), delimiters and line breaks inside quotes,
 * CRLF / LF / CR line endings and a UTF-8 byte order mark. Writing escapes the same way and
 * neutralises spreadsheet formula injection. Everything runs in the browser; nothing is sent anywhere.
 */

export class CsvFormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CsvFormatError';
  }
}

const DELIMITERS = [',', ';', '\t'] as const;

/**
 * Pick the delimiter that appears most often in the first record, ignoring delimiters inside quotes.
 * Ties and files without a delimiter fall back to a comma.
 */
export function detectDelimiter(text: string): string {
  const src = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const counts: Record<string, number> = { ',': 0, ';': 0, '\t': 0 };
  let inQuotes = false;
  for (const ch of src) {
    if (ch === '"') inQuotes = !inQuotes;
    else if (!inQuotes && (ch === '\n' || ch === '\r')) break;
    else if (!inQuotes && ch in counts) counts[ch] += 1;
  }
  let best: string = ',';
  for (const candidate of DELIMITERS) {
    if (counts[candidate] > counts[best]) best = candidate;
  }
  return best;
}

/**
 * Split CSV text into records of cells. Unquoted cells are trimmed; quoted cells keep their exact
 * content. Records that contain a single empty unquoted cell (blank lines) are dropped.
 */
export function parseCsvText(text: string, delimiter: string): string[][] {
  const src = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = '';
  let quoted = false;
  let inQuotes = false;

  const endCell = () => {
    row.push(quoted ? cell : cell.trim());
    cell = '';
    quoted = false;
  };
  const endRow = () => {
    endCell();
    if (!(row.length === 1 && row[0] === '')) rows.push(row);
    row = [];
  };

  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (inQuotes) {
      if (ch === '"') {
        if (src[i + 1] === '"') {
          cell += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cell += ch;
      }
    } else if (ch === '"' && !quoted && cell.trim() === '') {
      inQuotes = true;
      quoted = true;
      cell = '';
    } else if (ch === delimiter) {
      endCell();
    } else if (ch === '\r') {
      if (src[i + 1] === '\n') i += 1;
      endRow();
    } else if (ch === '\n') {
      endRow();
    } else {
      cell += ch;
    }
  }

  if (inQuotes) {
    throw new CsvFormatError('The CSV has a quoted field that is never closed.');
  }
  if (cell !== '' || quoted || row.length > 0) endRow();
  return rows;
}

const FORMULA_START = /^[=+@\t\r]/;
const NUMERIC = /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/;

/** Format one value as a CSV cell (RFC 4180 quoting, formula-injection guard for text). */
export function csvCell(value: unknown, delimiter = ','): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'number') return Number.isFinite(value) ? String(value) : '';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  let text = value instanceof Date ? value.toISOString() : String(value);

  // Spreadsheets run cells that start with = + @ (or a minus that is not a number) as formulas.
  if (FORMULA_START.test(text) || (text.startsWith('-') && !NUMERIC.test(text))) {
    text = `'${text}`;
  }
  const needsQuotes =
    text.includes(delimiter) ||
    text.includes('"') ||
    text.includes('\n') ||
    text.includes('\r') ||
    text !== text.trim();
  return needsQuotes ? `"${text.replace(/"/g, '""')}"` : text;
}

/** Serialize rows to CSV text (CRLF line endings, header first). */
export function toCsv(columns: string[], rows: Record<string, unknown>[], delimiter = ','): string {
  const lines = [columns.map((c) => csvCell(c, delimiter)).join(delimiter)];
  for (const row of rows) {
    lines.push(columns.map((c) => csvCell(row[c], delimiter)).join(delimiter));
  }
  return lines.join('\r\n');
}
