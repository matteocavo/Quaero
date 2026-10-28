/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** "true" for the static public build (GitHub Pages): Local Operations is disabled. */
  readonly VITE_DEMO_ONLY?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
