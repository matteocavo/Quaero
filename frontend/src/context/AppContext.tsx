import React, { createContext, useContext, useState, useEffect, useCallback, useMemo } from 'react';
import {
  AppMode,
  NavigationTab,
  ProjectSummary,
  ProjectOverviewResponse,
  UploadResponse,
} from '../types/api';
import { QuaeroApiClient, ApiError } from '../services/apiClient';
import { buildDataset } from '../analysis/dataset';
import { AnalysisError, runAnalysis, specToSentence, validateSpec } from '../analysis/query';
import type { AnalysisResult, AnalysisSpec, Dataset, DatasetOrigin } from '../analysis/types';
import { LOCAL_UNAVAILABLE_MESSAGE, localOperationsAvailable } from '../utils/runtime';
import {
  DatasetProfile,
  parseCSV,
  SAMPLE_RELEASE_IMPACT_CSV,
  SAMPLE_GLOBAL_INDICATORS_CSV,
} from '../utils/csvProfiler';

interface AppContextType {
  mode: AppMode;
  setMode: (mode: AppMode) => void;
  selectedProject: string;
  /** Choose a demo/Local project. Leaves the Browser Workspace, which stays loaded in memory. */
  setSelectedProject: (project: string) => void;
  /** True while the dataset loaded in this tab is the active context instead of a project. */
  browserWorkspaceActive: boolean;
  /** Value of the global selector: a project name or BROWSER_WORKSPACE_ID. */
  selectorValue: string;
  selectWorkspace: (value: string) => void;
  /** Forget the dataset, its filters and analyses, and return to the last selected project. */
  clearDataset: () => void;
  analysisHistory: AnalysisRun[];
  activeTab: NavigationTab;
  setActiveTab: (tab: NavigationTab) => void;
  projects: ProjectSummary[];
  currentOverview: ProjectOverviewResponse | null;
  loadingOverview: boolean;
  theme: 'light' | 'dark';
  toggleTheme: () => void;
  apiClient: QuaeroApiClient;
  connectionStatus: 'connected' | 'checking' | 'disconnected' | 'demo';
  isConnectionModalOpen: boolean;
  setIsConnectionModalOpen: (open: boolean) => void;
  flashMessage: { text: string; type: 'success' | 'info' | 'error' } | null;
  showFlash: (text: string, type?: 'success' | 'info' | 'error') => void;
  clearFlash: () => void;
  refreshProjects: (preferredProject?: string) => Promise<void>;
  refreshOverview: () => Promise<void>;
  uploads: UploadResponse[];
  selectedUploadId: string | null;
  setSelectedUploadId: (uploadId: string | null) => void;
  refreshUploads: () => Promise<UploadResponse[]>;
  testLiveConnection: () => Promise<boolean>;
  activeDatasetProfile: DatasetProfile | null;
  loadSampleDataset: (sampleType?: 'release_impact' | 'global_indicators') => void;
  /** Replace the loaded dataset. Resets the analysis and the workspace. */
  loadDatasetProfile: (profile: DatasetProfile, origin: DatasetOrigin) => void;
  activeDataset: Dataset | null;
  workspaceTab: WorkspaceTab;
  setWorkspaceTab: (tab: WorkspaceTab) => void;
  draftSpec: AnalysisSpec;
  setDraftSpec: (spec: AnalysisSpec) => void;
  appliedSpec: AnalysisSpec | null;
  analysisResult: AnalysisResult | null;
  analysisError: string | null;
  runDraftAnalysis: () => boolean;
  localOperationsAvailable: boolean;
  isMobileNavOpen: boolean;
  setMobileNavOpen: (open: boolean) => void;
}

/** One analysis the user ran on the loaded dataset (kept in memory only). */
export interface AnalysisRun {
  id: number;
  spec: AnalysisSpec;
  sentence: string;
  inputRows: number;
  filteredRows: number;
  groups: number;
}

/** Option value of the Browser Workspace in the global selector; never a valid project name. */
export const BROWSER_WORKSPACE_ID = '__browser_workspace__';

export type WorkspaceTab = 'preview' | 'quality' | 'explore' | 'build' | 'results' | 'export';

const EMPTY_SPEC: AnalysisSpec = { filters: [], dimensions: [], measures: [{ fn: 'count' }] };

const AppContext = createContext<AppContextType | null>(null);

export const AppProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  // Theme state - Light by default, with dark mode toggleable
  const [theme, setTheme] = useState<'light' | 'dark'>(() => {
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem('quaero_theme');
      if (saved === 'dark') return 'dark';
    }
    return 'light'; // Default to light mode
  });

  useEffect(() => {
    if (theme === 'dark') {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
    localStorage.setItem('quaero_theme', theme);
  }, [theme]);

  const toggleTheme = useCallback(() => {
    setTheme((prev) => (prev === 'light' ? 'dark' : 'light'));
  }, []);

  const localAvailable = useMemo(() => localOperationsAvailable(), []);
  const [isMobileNavOpen, setMobileNavOpen] = useState<boolean>(false);

  // Mode state - ALWAYS default to 'demo' for safety in preview & GitHub Pages
  const [mode, setModeState] = useState<AppMode>('demo');

  // Default to 'home'
  const [activeTab, setActiveTabRaw] = useState<NavigationTab>('home');
  const [selectedProject, setSelectedProjectRaw] = useState<string>('release_impact');
  const [browserWorkspaceActive, setBrowserWorkspaceActive] = useState<boolean>(false);
  const [projects, setProjects] = useState<ProjectSummary[]>([]);
  const [uploads, setUploads] = useState<UploadResponse[]>([]);
  const [selectedUploadId, setSelectedUploadId] = useState<string | null>(null);
  const [currentOverview, setCurrentOverview] = useState<ProjectOverviewResponse | null>(null);
  const [loadingOverview, setLoadingOverview] = useState<boolean>(false);
  const [connectionStatus, setConnectionStatus] = useState<'connected' | 'checking' | 'disconnected' | 'demo'>('demo');
  const [isConnectionModalOpen, setIsConnectionModalOpen] = useState<boolean>(false);
  const [flashMessage, setFlashMessage] = useState<{ text: string; type: 'success' | 'info' | 'error' } | null>(null);

  // The dataset the user loaded (parsed in-browser). None until they upload a file or pick a sample.
  const [activeDatasetProfile, setActiveDatasetProfile] = useState<DatasetProfile | null>(null);
  const [datasetMeta, setDatasetMeta] = useState<{ id: number; origin: DatasetOrigin }>({
    id: 0,
    origin: 'upload',
  });
  const [workspaceTab, setWorkspaceTab] = useState<WorkspaceTab>('preview');
  const [draftSpec, setDraftSpec] = useState<AnalysisSpec>(EMPTY_SPEC);
  const [appliedSpec, setAppliedSpec] = useState<AnalysisSpec | null>(null);
  const [analysisResult, setAnalysisResult] = useState<AnalysisResult | null>(null);
  const [analysisError, setAnalysisError] = useState<string | null>(null);
  const [analysisHistory, setAnalysisHistory] = useState<AnalysisRun[]>([]);

  // Typed rows for the analysis, rebuilt only when a different dataset is loaded.
  const activeDataset = useMemo<Dataset | null>(
    () => (activeDatasetProfile ? buildDataset(activeDatasetProfile, datasetMeta.origin, datasetMeta.id) : null),
    [activeDatasetProfile, datasetMeta],
  );

  const loadDatasetProfile = useCallback((profile: DatasetProfile, origin: DatasetOrigin) => {
    setActiveDatasetProfile(profile);
    setDatasetMeta((previous) => ({ id: previous.id + 1, origin }));
    // Nothing from the previous dataset may survive: filters, grouping, results and the open tab.
    setDraftSpec(EMPTY_SPEC);
    setAppliedSpec(null);
    setAnalysisResult(null);
    setAnalysisError(null);
    setAnalysisHistory([]);
    setWorkspaceTab('preview');
    // A new dataset becomes the active context straight away.
    setBrowserWorkspaceActive(true);
  }, []);

  const clearDataset = useCallback(() => {
    setActiveDatasetProfile(null);
    setDraftSpec(EMPTY_SPEC);
    setAppliedSpec(null);
    setAnalysisResult(null);
    setAnalysisError(null);
    setAnalysisHistory([]);
    setWorkspaceTab('preview');
    setBrowserWorkspaceActive(false);
  }, []);

  const setSelectedProject = useCallback((project: string) => {
    setSelectedProjectRaw(project);
    setBrowserWorkspaceActive(false);
    // A notice about the previous context must not stay on top of the new one.
    setFlashMessage(null);
    // The analysis screen belongs to the Browser Workspace; do not leave it open on a project.
    setActiveTabRaw((tab) => (tab === 'analyze' ? 'overview' : tab));
  }, []);

  const setActiveTab = useCallback(
    (tab: NavigationTab) => {
      // Opening the analysis of the loaded dataset makes it the active context.
      if (tab === 'analyze' && activeDatasetProfile) setBrowserWorkspaceActive(true);
      setActiveTabRaw(tab);
    },
    [activeDatasetProfile],
  );

  const selectWorkspace = useCallback(
    (value: string) => {
      if (value === BROWSER_WORKSPACE_ID) {
        if (activeDatasetProfile) {
          setBrowserWorkspaceActive(true);
          setFlashMessage(null);
        }
        return;
      }
      setSelectedProject(value);
    },
    [activeDatasetProfile, setSelectedProject],
  );

  const runDraftAnalysis = useCallback((): boolean => {
    if (!activeDataset) return false;
    const problems = validateSpec(activeDataset, draftSpec);
    if (problems.length > 0) {
      setAnalysisError(problems.join(' '));
      return false;
    }
    try {
      const result = runAnalysis(activeDataset, draftSpec);
      setAppliedSpec(draftSpec);
      setAnalysisResult(result);
      setAnalysisHistory((previous) =>
        [
          ...previous,
          {
            id: (previous[previous.length - 1]?.id ?? 0) + 1,
            spec: draftSpec,
            sentence: specToSentence(draftSpec),
            inputRows: result.inputRows,
            filteredRows: result.filteredRows,
            groups: result.totalGroups,
          },
        ].slice(-10),
      );
      setAnalysisError(null);
      return true;
    } catch (err) {
      setAnalysisError(err instanceof AnalysisError ? err.message : 'The analysis could not be calculated.');
      return false;
    }
  }, [activeDataset, draftSpec]);

  const loadSampleDataset = useCallback((sampleType: 'release_impact' | 'global_indicators' = 'release_impact') => {
    try {
      const isRelease = sampleType === 'release_impact';
      const rawText = isRelease ? SAMPLE_RELEASE_IMPACT_CSV : SAMPLE_GLOBAL_INDICATORS_CSV;
      const fileName = isRelease ? 'release_impact_sample.csv' : 'global_economic_indicators_sample.csv';

      const profile = parseCSV(rawText, fileName);
      loadDatasetProfile(profile, 'sample');
      setFlashMessage({
        text: `Loaded sample ${fileName} into browser memory. Nothing is uploaded.`,
        type: 'success',
      });
    } catch (err: any) {
      setFlashMessage({ text: `Failed to load sample: ${err.message}`, type: 'error' });
    }
  }, [loadDatasetProfile]);

  // Single client instance
  const apiClient = useMemo(() => {
    return new QuaeroApiClient(mode);
  }, [mode]);

  const showFlash = useCallback((text: string, type: 'success' | 'info' | 'error' = 'info') => {
    setFlashMessage({ text, type });
  }, []);

  const clearFlash = useCallback(() => {
    setFlashMessage(null);
  }, []);

  const setMode = useCallback((newMode: AppMode) => {
    if (newMode === 'live' && !localAvailable) {
      // Static hosting (GitHub Pages): no API exists, so stay in demo and say why.
      setFlashMessage({ text: LOCAL_UNAVAILABLE_MESSAGE, type: 'info' });
      return;
    }
    setModeState(newMode);
    apiClient.setMode(newMode);
    // Never carry data from the other mode over (demo fixtures must not appear in Live).
    setProjects([]);
    setUploads([]);
    setSelectedUploadId(null);
    setCurrentOverview(null);
    if (newMode === 'demo') {
      setConnectionStatus('demo');
      showFlash('Operating in Public Demo Sandbox. All data is processed locally.', 'info');
    } else {
      setConnectionStatus('checking');
      showFlash('Switched to Local Operations mode. Testing proxy connection to localhost:8000...', 'info');
    }
  }, [apiClient, localAvailable, showFlash]);

  const testLiveConnection = useCallback(async (): Promise<boolean> => {
    if (!localAvailable) {
      setConnectionStatus('demo');
      return false;
    }
    const tempClient = new QuaeroApiClient('live');
    try {
      setConnectionStatus('checking');
      const health = await tempClient.checkHealth();
      if (health.status === 'ok') {
        setConnectionStatus('connected');
        return true;
      }
      setConnectionStatus('disconnected');
      return false;
    } catch {
      setConnectionStatus('disconnected');
      return false;
    }
  }, [localAvailable]);

  // Load projects list
  const refreshProjects = useCallback(async (preferredProject?: string) => {
    try {
      const res = await apiClient.listProjects();
      setProjects(res.projects);
      const target = preferredProject || selectedProject;
      if (preferredProject && res.projects.some((p) => p.project === preferredProject)) {
        setSelectedProjectRaw(preferredProject);
        setBrowserWorkspaceActive(false);
      } else if (res.projects.length > 0 && !res.projects.some((p) => p.project === target)) {
        setSelectedProjectRaw(res.projects[0].project);
      }
    } catch (err: any) {
      setProjects([]);
      if (mode === 'live') {
        setConnectionStatus('disconnected');
      }
      // Never flash error in demo mode
      if (mode === 'live') {
        showFlash(`FastAPI is offline: ${err?.message || err}`, 'error');
      }
    }
  }, [apiClient, mode, selectedProject, showFlash]);

  const refreshUploads = useCallback(async (): Promise<UploadResponse[]> => {
    if (mode !== 'live') {
      setUploads([]);
      return [];
    }
    try {
      const res = await apiClient.listUploads();
      setUploads(res.uploads);
      if (res.uploads.length > 0 && !selectedUploadId) {
        setSelectedUploadId(res.uploads[0].upload_id);
      }
      return res.uploads;
    } catch {
      return [];
    }
  }, [apiClient, mode, selectedUploadId]);

  // Load overview for currently selected project
  const refreshOverview = useCallback(async () => {
    if (!selectedProject) return;
    setLoadingOverview(true);
    try {
      const overview = await apiClient.getProjectOverview(selectedProject);
      setCurrentOverview(overview);
    } catch (err: any) {
      setCurrentOverview(null);
      if (err instanceof ApiError && err.statusCode === 404) {
        // Artifacts not yet generated is normal in Quaero
      } else if (mode === 'live') {
        showFlash(`Could not fetch overview for ${selectedProject}: ${err.message}`, 'error');
      }
    } finally {
      setLoadingOverview(false);
    }
  }, [apiClient, mode, selectedProject, showFlash]);

  // Initialize
  useEffect(() => {
    if (mode === 'live') {
      testLiveConnection().then((connected) => {
        if (connected) {
          refreshProjects();
          refreshUploads();
        }
      });
    } else {
      setConnectionStatus('demo');
      refreshProjects();
    }
  }, [mode, testLiveConnection, refreshProjects, refreshUploads]);

  useEffect(() => {
    refreshOverview();
  }, [selectedProject, refreshOverview]);

  const value = {
    mode,
    setMode,
    selectedProject,
    setSelectedProject,
    browserWorkspaceActive,
    selectorValue: browserWorkspaceActive ? BROWSER_WORKSPACE_ID : selectedProject,
    selectWorkspace,
    clearDataset,
    analysisHistory,
    activeTab,
    setActiveTab,
    projects,
    // Never show a project's overview while the Browser Workspace is the active context.
    currentOverview: browserWorkspaceActive ? null : currentOverview,
    loadingOverview,
    theme,
    toggleTheme,
    apiClient,
    connectionStatus,
    isConnectionModalOpen,
    setIsConnectionModalOpen,
    flashMessage,
    showFlash,
    clearFlash,
    refreshProjects,
    refreshOverview,
    uploads,
    selectedUploadId,
    setSelectedUploadId,
    refreshUploads,
    testLiveConnection,
    activeDatasetProfile,
    loadSampleDataset,
    loadDatasetProfile,
    activeDataset,
    workspaceTab,
    setWorkspaceTab,
    draftSpec,
    setDraftSpec,
    appliedSpec,
    analysisResult,
    analysisError,
    runDraftAnalysis,
    localOperationsAvailable: localAvailable,
    isMobileNavOpen,
    setMobileNavOpen,
  };

  return <AppContext.Provider value={value}>{children}</AppContext.Provider>;
};

export const useApp = () => {
  const context = useContext(AppContext);
  if (!context) {
    throw new Error('useApp must be used within an AppProvider');
  }
  return context;
};
