/** Save text content as a file in the browser, without any network request. */
export function downloadTextFile(filename: string, content: string, mimeType: string): void {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  anchor.style.display = 'none';
  // Attached to the document and revoked later: some browsers cancel the download otherwise.
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** A safe file name stem derived from a user file name. */
export function fileStem(fileName: string): string {
  const stem = fileName.replace(/\.[^.]+$/, '').replace(/[^A-Za-z0-9._-]+/g, '_');
  return stem || 'dataset';
}
