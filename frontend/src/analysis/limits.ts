/**
 * Limits that keep the browser tab responsive. They apply to the in-browser analysis only; the
 * Quaero Python pipeline has no such limits.
 */
export const MAX_FILE_BYTES = 10 * 1024 * 1024;
export const MAX_ROWS = 200_000;
export const MAX_COLUMNS = 200;
export const MAX_GROUPS = 50_000;
export const MAX_RESULT_ROWS = 1_000;
export const MAX_DIMENSIONS = 3;
export const MAX_MEASURES = 6;
export const MAX_FILTERS = 12;

function formatMegabytes(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

/** Checked before the file is read. Returns a user-facing message, or null when the file is fine. */
export function checkFileSize(sizeBytes: number): string | null {
  if (sizeBytes === 0) {
    return 'This file is empty (0 bytes). Choose a CSV that contains a header row and data.';
  }
  if (sizeBytes > MAX_FILE_BYTES) {
    return (
      `This file is ${formatMegabytes(sizeBytes)}. The in-browser analysis is limited to ` +
      `${formatMegabytes(MAX_FILE_BYTES)} so your tab stays responsive. Use a smaller extract, or run ` +
      'Quaero locally to process large files with the Python pipeline.'
    );
  }
  return null;
}

/** Checked after parsing. Returns a user-facing message, or null when the dataset fits the limits. */
export function checkParsedSize(rowCount: number, columnCount: number): string | null {
  if (columnCount === 0) return 'No columns were found. The first row must contain the column names.';
  if (rowCount === 0) {
    return 'The file has a header row but no data rows, so there is nothing to analyze.';
  }
  if (columnCount > MAX_COLUMNS) {
    return `This file has ${columnCount} columns; the in-browser analysis supports up to ${MAX_COLUMNS}.`;
  }
  if (rowCount > MAX_ROWS) {
    return (
      `This file has ${rowCount.toLocaleString('en-US')} rows; the in-browser analysis supports up to ` +
      `${MAX_ROWS.toLocaleString('en-US')}. Use a smaller extract, or run Quaero locally.`
    );
  }
  return null;
}
