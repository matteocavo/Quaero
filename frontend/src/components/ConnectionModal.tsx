import React, { useState } from 'react';
import {
  X,
  Server,
  CheckCircle2,
  AlertTriangle,
  RefreshCw,
  Shield,
  ArrowLeft,
  Sparkles,
} from 'lucide-react';
import { useApp } from '../context/AppContext';

export const ConnectionModal: React.FC = () => {
  const {
    isConnectionModalOpen,
    setIsConnectionModalOpen,
    mode,
    setMode,
    testLiveConnection,
    connectionStatus,
    showFlash,
  } = useApp();

  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ ok: boolean; message: string } | null>(null);

  if (!isConnectionModalOpen) return null;

  const handleTestProxy = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const ok = await testLiveConnection();
      if (ok) {
        setTestResult({
          ok: true,
          message: 'Connected to local FastAPI backend via same-origin proxy (/api/health responded ok).',
        });
        showFlash('FastAPI backend connection verified.', 'success');
      } else {
        setTestResult({
          ok: false,
          message:
            'FastAPI is unreachable at http://127.0.0.1:8000 via the same-origin proxy. The Python backend is not running.',
        });
      }
    } catch (err: any) {
      setTestResult({
        ok: false,
        message: err?.message || 'Connection test failed.',
      });
    } finally {
      setTesting(false);
    }
  };

  const handleSelectLive = () => {
    setMode('live');
    handleTestProxy();
  };

  const handleReturnToDemo = () => {
    setMode('demo');
    setTestResult(null);
    showFlash('Switched back to Public Demo Sandbox.', 'info');
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 p-4 backdrop-blur-xs">
      <div className="relative w-full max-w-lg rounded-3xl border border-slate-200 bg-white p-6 shadow-xl dark:border-slate-800 dark:bg-slate-900">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-100 pb-4 dark:border-slate-800">
          <div className="flex items-center space-x-2.5">
            <div className="rounded-xl bg-sky-100 p-2 text-sky-700 dark:bg-sky-950 dark:text-sky-300">
              <Server className="h-5 w-5" />
            </div>
            <div>
              <h3 className="text-base font-bold text-slate-900 dark:text-white">
                Environment & Operating Mode
              </h3>
              <p className="text-xs text-slate-500 dark:text-slate-400">
                Switch between Public Demo (default) and Local FastAPI operations.
              </p>
            </div>
          </div>
          <button
            onClick={() => setIsConnectionModalOpen(false)}
            className="rounded-lg p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600 dark:hover:bg-slate-800 dark:hover:text-slate-200"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Mode Selector */}
        <div className="mt-5 space-y-4">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <button
              type="button"
              onClick={handleReturnToDemo}
              className={`flex flex-col items-start rounded-2xl border p-4 text-left transition ${
                mode === 'demo'
                  ? 'border-sky-500 bg-sky-50/60 ring-2 ring-sky-500/20 dark:border-sky-400 dark:bg-sky-950/40'
                  : 'border-slate-200 bg-white hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-900 dark:hover:bg-slate-800/60'
              }`}
            >
              <div className="flex w-full items-center justify-between">
                <span className="text-xs font-bold text-slate-900 dark:text-white">
                  Public Demo Mode (Default)
                </span>
                {mode === 'demo' && <CheckCircle2 className="h-4 w-4 text-sky-600 dark:text-sky-400" />}
              </div>
              <p className="mt-1.5 text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">
                100% browser-based. Zero backend requirements. Uses bundled demo fixtures. Safe for GitHub Pages.
              </p>
            </button>

            <button
              type="button"
              onClick={handleSelectLive}
              className={`flex flex-col items-start rounded-2xl border p-4 text-left transition ${
                mode === 'live'
                  ? 'border-sky-500 bg-sky-50/60 ring-2 ring-sky-500/20 dark:border-sky-400 dark:bg-sky-950/40'
                  : 'border-slate-200 bg-white hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-900 dark:hover:bg-slate-800/60'
              }`}
            >
              <div className="flex w-full items-center justify-between">
                <span className="text-xs font-bold text-slate-900 dark:text-white">
                  Local Operations
                </span>
                {mode === 'live' && <CheckCircle2 className="h-4 w-4 text-sky-600 dark:text-sky-400" />}
              </div>
              <p className="mt-1.5 text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">
                Requires local FastAPI process. Connects via same-origin proxy without client-side API keys.
              </p>
            </button>
          </div>

          {/* Security Note on Zero-Key Architecture */}
          <div className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4 text-xs dark:border-slate-800 dark:bg-slate-800/40">
            <div className="flex items-center space-x-2 text-slate-800 dark:text-slate-200">
              <Shield className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
              <span className="font-semibold">Security Model: Zero Client API Keys</span>
            </div>
            <p className="mt-1.5 text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">
              To prevent credential leakage in client-side bundles, API keys are never stored in the browser. In local operations, the server proxy securely attaches <span className="font-mono">QUAERO_API_KEY</span> from server environment variables.
            </p>
          </div>

          {/* Local Backend Status & Testing */}
          {mode === 'live' && (
            <div className="space-y-3 rounded-2xl border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900">
              <div className="flex items-center justify-between text-xs">
                <span className="font-semibold text-slate-700 dark:text-slate-300">
                  FastAPI Proxy Status:
                </span>
                <span
                  className={`inline-flex items-center space-x-1 font-semibold ${
                    connectionStatus === 'connected'
                      ? 'text-emerald-600 dark:text-emerald-400'
                      : connectionStatus === 'checking'
                      ? 'text-sky-600 dark:text-sky-400'
                      : 'text-rose-600 dark:text-rose-400'
                  }`}
                >
                  <span>{connectionStatus}</span>
                </span>
              </div>

              <div className="flex items-center space-x-2">
                <button
                  type="button"
                  onClick={handleTestProxy}
                  disabled={testing}
                  className="flex items-center space-x-1.5 rounded-xl border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-100 disabled:opacity-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200"
                >
                  <RefreshCw className={`h-3.5 w-3.5 ${testing ? 'animate-spin' : ''}`} />
                  <span>{testing ? 'Testing proxy...' : 'Test Connection'}</span>
                </button>
                {connectionStatus === 'disconnected' && (
                  <button
                    type="button"
                    onClick={handleReturnToDemo}
                    className="flex items-center space-x-1.5 rounded-xl bg-amber-50 px-3 py-1.5 text-xs font-semibold text-amber-800 hover:bg-amber-100 dark:bg-amber-950/60 dark:text-amber-300"
                  >
                    <ArrowLeft className="h-3.5 w-3.5" />
                    <span>Return to Demo Mode</span>
                  </button>
                )}
              </div>

              {testResult && (
                <div
                  className={`flex flex-col space-y-2 rounded-xl p-3 text-xs ${
                    testResult.ok
                      ? 'border border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-300'
                      : 'border border-rose-200 bg-rose-50 text-rose-800 dark:border-rose-900 dark:bg-rose-950/40 dark:text-rose-300'
                  }`}
                >
                  <div className="flex items-start space-x-2">
                    {testResult.ok ? (
                      <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400" />
                    ) : (
                      <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-rose-600 dark:text-rose-400" />
                    )}
                    <span>{testResult.message}</span>
                  </div>

                  {!testResult.ok && (
                    <div className="pt-1">
                      <button
                        type="button"
                        onClick={handleReturnToDemo}
                        className="rounded-lg bg-white px-2.5 py-1 text-xs font-bold text-rose-900 shadow-2xs hover:bg-slate-50 dark:bg-slate-800 dark:text-white"
                      >
                        Switch Back to Public Demo Mode Now
                      </button>
                    </div>
                  )}
                </div>
              )}

              {/* Startup terminal snippet */}
              <div className="pt-2">
                <span className="text-[11px] font-semibold text-slate-500">
                  To start your local FastAPI server:
                </span>
                <pre className="mt-1 overflow-x-auto rounded-lg bg-slate-950 p-2.5 font-mono text-[10px] text-sky-300">
                  export QUAERO_API_KEY="your-secret-key"&#10;uvicorn api.main:app --host 127.0.0.1 --port 8000
                </pre>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="mt-6 flex justify-end border-t border-slate-100 pt-4 dark:border-slate-800">
          <button
            type="button"
            onClick={() => setIsConnectionModalOpen(false)}
            className="rounded-xl bg-sky-600 px-4 py-2 text-xs font-semibold text-white shadow-xs hover:bg-sky-500 dark:bg-sky-500 dark:hover:bg-sky-400"
          >
            Done
          </button>
        </div>
      </div>
    </div>
  );
};
