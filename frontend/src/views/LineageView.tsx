import React, { useState, useEffect, useMemo } from 'react';
import {
  GitFork,
  ZoomIn,
  ZoomOut,
  RotateCcw,
  ArrowRight,
  Info,
} from 'lucide-react';
import { useApp } from '../context/AppContext';
import { LineageResponse } from '../types/api';

const EDGE_COLORS: Record<string, string> = {
  passthrough: '#94a3b8',
  normalized: '#0284c7',
  type_cast: '#e11d48',
  aggregated: '#10b981',
};

export const LineageView: React.FC = () => {
  const { selectedProject, setSelectedProject, apiClient } = useApp();

  const [lineage, setLineage] = useState<LineageResponse | null>(null);
  const [selectedEdgeTypes, setSelectedEdgeTypes] = useState<string[]>([
    'passthrough',
    'normalized',
    'type_cast',
    'aggregated',
  ]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<'visual' | 'table'>('visual');
  const [zoomLevel, setZoomLevel] = useState<number>(1);

  useEffect(() => {
    let isMounted = true;
    const fetchLineage = async () => {
      try {
        const data = await apiClient.getLineage(selectedProject);
        if (isMounted) {
          setLineage(data);
        }
      } catch {
        if (isMounted) {
          setLineage(null);
        }
      }
    };
    fetchLineage();
    return () => {
      isMounted = false;
    };
  }, [selectedProject, apiClient]);

  // Group nodes by layer
  const { rawNodes, stagingNodes, martNodes } = useMemo(() => {
    if (!lineage) return { rawNodes: [], stagingNodes: [], martNodes: [] };
    const raw = lineage.nodes.filter((n) => n.layer === 'raw');
    const staging = lineage.nodes.filter((n) => n.layer === 'staging');
    const mart = lineage.nodes.filter((n) => n.layer === 'mart');
    return { rawNodes: raw, stagingNodes: staging, martNodes: mart };
  }, [lineage]);

  // Filter edges based on chosen types
  const visibleEdges = useMemo(() => {
    if (!lineage) return [];
    return lineage.edges.filter((e) => selectedEdgeTypes.includes(e.type));
  }, [lineage, selectedEdgeTypes]);

  const toggleEdgeType = (type: string) => {
    setSelectedEdgeTypes((prev) =>
      prev.includes(type) ? prev.filter((t) => t !== type) : [...prev, type],
    );
  };

  return (
    <div className="space-y-6">
      {/* Control Header */}
      <div className="flex flex-col justify-between space-y-3 rounded-2xl border border-slate-200 bg-white p-5 shadow-2xs dark:border-slate-800 dark:bg-slate-900 sm:flex-row sm:items-center sm:space-y-0">
        <div>
          <div className="flex items-center space-x-2">
            <GitFork className="h-5 w-5 text-sky-600 dark:text-sky-400" />
            <h3 className="text-base font-bold text-slate-900 dark:text-white">
              Column-Level Lineage DAG
            </h3>
            {lineage && (
              <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs font-semibold text-slate-700 dark:bg-slate-800 dark:text-slate-300">
                {lineage.node_count} nodes · {lineage.edge_count} edges
              </span>
            )}
          </div>
          <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
            Traces column origin and transformation contracts from RAW → STAGING → analytical MART tables.
          </p>
        </div>

        {/* View Switcher & Zoom */}
        {lineage && (
          <div className="flex items-center space-x-2">
            <div className="flex rounded-xl border border-slate-200 bg-slate-50 p-0.5 dark:border-slate-700 dark:bg-slate-800">
              <button
                onClick={() => setViewMode('visual')}
                className={`rounded-lg px-3 py-1 text-xs font-semibold transition ${
                  viewMode === 'visual'
                    ? 'bg-white text-slate-900 shadow-2xs dark:bg-slate-700 dark:text-white'
                    : 'text-slate-500 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
                }`}
              >
                Visual DAG
              </button>
              <button
                onClick={() => setViewMode('table')}
                className={`rounded-lg px-3 py-1 text-xs font-semibold transition ${
                  viewMode === 'table'
                    ? 'bg-white text-slate-900 shadow-2xs dark:bg-slate-700 dark:text-white'
                    : 'text-slate-500 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
                }`}
              >
                Tables View
              </button>
            </div>

            {viewMode === 'visual' && (
              <div className="flex items-center space-x-1 rounded-xl border border-slate-200 bg-white px-2 py-1 dark:border-slate-700 dark:bg-slate-800">
                <button
                  onClick={() => setZoomLevel((z) => Math.max(0.7, z - 0.1))}
                  className="p-1 text-slate-500 hover:text-slate-800 dark:hover:text-slate-200"
                  title="Zoom Out"
                >
                  <ZoomOut className="h-3.5 w-3.5" />
                </button>
                <span className="text-[11px] font-mono text-slate-600 dark:text-slate-300">
                  {Math.round(zoomLevel * 100)}%
                </span>
                <button
                  onClick={() => setZoomLevel((z) => Math.min(1.4, z + 0.1))}
                  className="p-1 text-slate-500 hover:text-slate-800 dark:hover:text-slate-200"
                  title="Zoom In"
                >
                  <ZoomIn className="h-3.5 w-3.5" />
                </button>
                <button
                  onClick={() => setZoomLevel(1)}
                  className="p-1 text-slate-500 hover:text-slate-800 dark:hover:text-slate-200"
                  title="Reset Zoom"
                >
                  <RotateCcw className="h-3.5 w-3.5" />
                </button>
              </div>
            )}
          </div>
        )}
      </div>

      {lineage ? (
        <>
          {/* Filter Edge Types Bar */}
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <span className="font-semibold text-slate-500 dark:text-slate-400">
              Edge Transformation Filter:
            </span>
            {['passthrough', 'normalized', 'type_cast', 'aggregated'].map((type) => {
              const isSelected = selectedEdgeTypes.includes(type);
              const color = EDGE_COLORS[type];
              return (
                <button
                  key={type}
                  onClick={() => toggleEdgeType(type)}
                  className={`flex items-center space-x-1.5 rounded-full border px-3 py-1 font-semibold transition ${
                    isSelected
                      ? 'border-slate-300 bg-white text-slate-800 shadow-2xs dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200'
                      : 'border-slate-200 bg-slate-50 text-slate-400 opacity-60 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-600'
                  }`}
                >
                  <span
                    className="h-2 w-2 rounded-full"
                    style={{ backgroundColor: color }}
                  />
                  <span className="capitalize">{type.replace('_', ' ')}</span>
                </button>
              );
            })}
          </div>

          {viewMode === 'visual' ? (
            <div className="relative rounded-3xl border border-slate-200 bg-slate-50/50 p-6 shadow-2xs dark:border-slate-800 dark:bg-slate-900/50">
              <div
                style={{ transform: `scale(${zoomLevel})`, transformOrigin: 'top left' }}
                className="transition-transform duration-150"
              >
                {/* 3 Columns: RAW, STAGING, MART */}
                <div className="grid grid-cols-1 gap-6 md:grid-cols-3">
                  {/* 1. RAW LAYER */}
                  <div className="space-y-3">
                    <div className="flex items-center justify-between rounded-xl bg-rose-100/70 px-3 py-2 text-xs font-bold text-rose-900 dark:bg-rose-950/60 dark:text-rose-200">
                      <span>1. RAW SOURCE</span>
                      <span className="rounded bg-rose-200/60 px-1.5 py-0.5 text-[10px] dark:bg-rose-900">
                        {rawNodes.length} columns
                      </span>
                    </div>
                    <div className="space-y-2">
                      {rawNodes.map((node) => {
                        const isSelected = selectedNodeId === node.id;
                        return (
                          <div
                            key={node.id}
                            onClick={() => setSelectedNodeId(node.id)}
                            className={`cursor-pointer rounded-2xl border p-3 shadow-2xs transition ${
                              isSelected
                                ? 'border-sky-500 bg-white ring-2 ring-sky-500/20 dark:bg-slate-800'
                                : 'border-slate-200 bg-white hover:border-slate-300 dark:border-slate-800 dark:bg-slate-800/80 dark:hover:border-slate-700'
                            }`}
                          >
                            <div className="flex items-center justify-between">
                              <span className="font-mono text-xs font-bold text-slate-900 dark:text-white">
                                {node.name}
                              </span>
                              <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-500 dark:bg-slate-700 dark:text-slate-400">
                                raw
                              </span>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>

                  {/* 2. STAGING LAYER */}
                  <div className="space-y-3">
                    <div className="flex items-center justify-between rounded-xl bg-sky-100/70 px-3 py-2 text-xs font-bold text-sky-900 dark:bg-sky-950/60 dark:text-sky-200">
                      <span>2. STAGING (CLEANED)</span>
                      <span className="rounded bg-sky-200/60 px-1.5 py-0.5 text-[10px] dark:bg-sky-900">
                        {stagingNodes.length} columns
                      </span>
                    </div>
                    <div className="space-y-2">
                      {stagingNodes.map((node) => {
                        const isSelected = selectedNodeId === node.id;
                        return (
                          <div
                            key={node.id}
                            onClick={() => setSelectedNodeId(node.id)}
                            className={`cursor-pointer rounded-2xl border p-3 shadow-2xs transition ${
                              isSelected
                                ? 'border-sky-500 bg-white ring-2 ring-sky-500/20 dark:bg-slate-800'
                                : 'border-slate-200 bg-white hover:border-slate-300 dark:border-slate-800 dark:bg-slate-800/80 dark:hover:border-slate-700'
                            }`}
                          >
                            <div className="flex items-center justify-between">
                              <span className="font-mono text-xs font-bold text-slate-900 dark:text-white">
                                {node.name}
                              </span>
                              <span className="rounded bg-sky-50 px-1.5 py-0.5 font-mono text-[10px] text-sky-700 dark:bg-sky-950 dark:text-sky-300">
                                {node.dtype || 'typed'}
                              </span>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>

                  {/* 3. MART LAYER */}
                  <div className="space-y-3">
                    <div className="flex items-center justify-between rounded-xl bg-emerald-100/70 px-3 py-2 text-xs font-bold text-emerald-900 dark:bg-emerald-950/60 dark:text-emerald-200">
                      <span>3. ANALYTICAL MARTS</span>
                      <span className="rounded bg-emerald-200/60 px-1.5 py-0.5 text-[10px] dark:bg-emerald-900">
                        {martNodes.length} columns
                      </span>
                    </div>
                    <div className="space-y-2">
                      {martNodes.map((node) => {
                        const isSelected = selectedNodeId === node.id;
                        return (
                          <div
                            key={node.id}
                            onClick={() => setSelectedNodeId(node.id)}
                            className={`cursor-pointer rounded-2xl border p-3 shadow-2xs transition ${
                              isSelected
                                ? 'border-sky-500 bg-white ring-2 ring-sky-500/20 dark:bg-slate-800'
                                : 'border-slate-200 bg-white hover:border-slate-300 dark:border-slate-800 dark:bg-slate-800/80 dark:hover:border-slate-700'
                            }`}
                          >
                            <div className="text-[10px] font-mono text-emerald-700 dark:text-emerald-400">
                              {node.mart}
                            </div>
                            <div className="mt-0.5 flex items-center justify-between">
                              <span className="font-mono text-xs font-bold text-slate-900 dark:text-white">
                                {node.name}
                              </span>
                              <span className="rounded bg-emerald-50 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300">
                                mart
                              </span>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>

                {/* Transformation Edges Summary List */}
                <div className="mt-8 rounded-2xl border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
                  <h4 className="text-xs font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400">
                    Active Column Lineage Edges ({visibleEdges.length})
                  </h4>
                  <div className="mt-3 grid grid-cols-1 gap-2.5 sm:grid-cols-2 lg:grid-cols-3">
                    {visibleEdges.map((e, idx) => (
                      <div
                        key={idx}
                        className="flex items-center justify-between rounded-xl border border-slate-100 bg-slate-50/70 p-2.5 text-xs font-mono dark:border-slate-800 dark:bg-slate-800/40"
                      >
                        <span className="truncate max-w-[110px] text-slate-700 dark:text-slate-300" title={e.from}>
                          {e.from.replace(/^(raw|staging|mart)::/, '')}
                        </span>
                        <div className="flex items-center space-x-1 px-1">
                          <span
                            className="rounded px-1.5 py-0.2 text-[9px] font-bold uppercase text-white"
                            style={{ backgroundColor: EDGE_COLORS[e.type] || '#888' }}
                          >
                            {e.type}
                          </span>
                          <ArrowRight className="h-3 w-3 text-slate-400" />
                        </div>
                        <span className="truncate max-w-[110px] text-slate-700 dark:text-slate-300" title={e.to}>
                          {e.to.replace(/^(raw|staging|mart)::/, '')}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          ) : (
            /* Tables View */
            <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
              <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
                <h4 className="text-xs font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400">
                  Lineage Nodes ({lineage.nodes.length})
                </h4>
                <div className="mt-3 max-h-96 overflow-y-auto">
                  <table className="w-full text-left text-xs font-mono">
                    <thead className="border-b border-slate-100 bg-slate-50 text-slate-500 dark:border-slate-800 dark:bg-slate-800">
                      <tr>
                        <th className="p-2">Layer</th>
                        <th className="p-2">Name</th>
                        <th className="p-2">Mart / Type</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                      {lineage.nodes.map((n) => (
                        <tr key={n.id}>
                          <td className="p-2 capitalize">{n.layer}</td>
                          <td className="p-2 font-bold text-slate-900 dark:text-white">
                            {n.name}
                          </td>
                          <td className="p-2 text-slate-500">
                            {n.mart || n.dtype || '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
                <h4 className="text-xs font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400">
                  Lineage Edges ({visibleEdges.length})
                </h4>
                <div className="mt-3 max-h-96 overflow-y-auto">
                  <table className="w-full text-left text-xs font-mono">
                    <thead className="border-b border-slate-100 bg-slate-50 text-slate-500 dark:border-slate-800 dark:bg-slate-800">
                      <tr>
                        <th className="p-2">Source</th>
                        <th className="p-2">Type</th>
                        <th className="p-2">Target</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                      {visibleEdges.map((e, idx) => (
                        <tr key={idx}>
                          <td className="p-2 truncate max-w-[120px]">{e.from}</td>
                          <td className="p-2">
                            <span
                              className="rounded px-1.5 py-0.5 text-[10px] text-white"
                              style={{ backgroundColor: EDGE_COLORS[e.type] }}
                            >
                              {e.type}
                            </span>
                          </td>
                          <td className="p-2 truncate max-w-[120px]">{e.to}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}
        </>
      ) : (
        <div className="rounded-3xl border border-slate-200 bg-white p-10 text-center shadow-2xs dark:border-slate-800 dark:bg-slate-900">
          <GitFork className="mx-auto h-10 w-10 text-slate-300 dark:text-slate-600" />
          <h4 className="mt-3 text-base font-bold text-slate-800 dark:text-slate-200">
            Lineage DAG Not Generated for "{selectedProject}"
          </h4>
          <p className="mt-1.5 text-xs text-slate-500 max-w-md mx-auto leading-relaxed">
            In Quaero v0.6.0, column-level lineage graphs are generated for single-dataset pipeline runs.
            Switch to the <span className="font-semibold text-slate-700 dark:text-slate-300 font-mono">release_impact</span> project to explore the column-level DAG.
          </p>
          <div className="mt-5">
            <button
              onClick={() => setSelectedProject('release_impact')}
              className="rounded-2xl bg-sky-600 px-5 py-2.5 text-xs font-bold text-white shadow-xs hover:bg-sky-500 dark:bg-sky-500 dark:hover:bg-sky-400"
            >
              Inspect release_impact Lineage DAG
            </button>
          </div>
        </div>
      )}
    </div>
  );
};
