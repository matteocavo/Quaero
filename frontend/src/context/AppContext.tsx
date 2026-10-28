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
  DARK_QUERY,
  migrateLegacyTheme,
  readThemePreference,
  resolveTheme,
  saveThemePreference,
  systemPrefersDark,
  type ThemePreference,
} from '../utils/theme';
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
  /** The theme in use now ('system' already resolved to the device's choice). */
  theme: 'light' | 'dark';
  /** What the user picked: dark (the default), light or system. */
  themePreference: ThemePreference;
  setThemePreference: (preference: ThemePreference) => void;
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
  /** Put a ready-made analysis into the query builder (keeping the current filters) and run it. */
  applySpec: (spec: AnalysisSpec) => boolean;
  /** Column shown in the distribution of Explore & Filter; set when a suggestion points at one. */
  distributionColumn: string;
  setDistributionColumn: (column: string) => void;
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
  // Theme: Dark by default. Only an explicit choice is stored (see utils/theme.ts).
  const [themePreference, setThemePreferenceState] = useState<ThemePreference>(() => readThemePreference());
  const [systemDark, setSystemDark] = useState<boolean>(() => systemPrefersDark());
  const theme = resolveTheme(themePreference, systemDark);

  useEffect(() => {
    migrateLegacyTheme();
  }, []);

  useEffect(() => {
    if (themePreference !== 'system' || typeof window.matchMedia !== 'function') return;
    const query = window.matchMedia(DARK_QUERY);
    const follow = () => setSystemDark(query.matches);
    follow();
    query.addEventListener('change', follow);
    return () => query.removeEventListener('change', follow);
  }, [themePreference]);

  useEffect(() => {
    document.documentElement.classList.toggle('dark', theme === 'dark');
  }, [theme]);

  const setThemePreference = useCallback((preference: ThemePreference) => {
    setThemePreferenceState(preference);
    saveThemePreference(preference);
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
  const [distributionColumn, setDistributionColumn] = useState<string>('');
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
    setDistributionColumn('');
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
    setDistributionColumn('');
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

  const runSpec = useCallback(
    (spec: AnalysisSpec): boolean => {
      if (!activeDataset) return false;
      const problems = validateSpec(activeDataset, spec);
      if (problems.length > 0) {
        setAnalysisError(problems.join(' '));
        return false;
      }
      try {
        const result = runAnalysis(activeDataset, spec);
        setAppliedSpec(spec);
        setAnalysisResult(result);
        setAnalysisHistory((previous) =>
          [
            ...previous,
            {
              id: (previous[previous.length - 1]?.id ?? 0) + 1,
              spec,
              sentence: specToSentence(spec),
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
    },
    [activeDataset],
  );

  const runDraftAnalysis = useCallback((): boolean => runSpec(draftSpec), [runSpec, draftSpec]);

  const applySpec = useCallback(
    (spec: AnalysisSpec): boolean => {
      // The user's filters stay: a suggestion changes what is calculated, not which rows are used.
      const next: AnalysisSpec = { ...spec, filters: draftSpec.filters, question: draftSpec.question };
      setDraftSpec(next);
      return runSpec(next);
    },
    [runSpec, draftSpec.filters, draftSpec.question],
  );

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

  // After a crash or a closed window, temporary uploads of the earlier session stay on disk. Say so, and
  // never delete them automatically: the Local Storage screen lists them and asks before deleting.
  const reportLeftoverUploads = useCallback(async () => {
    try {
      const storage = await apiClient.getStorage();
      if (storage.orphaned_uploads > 0) {
        const n = storage.orphaned_uploads;
        showFlash(
          `${n} temporary upload${n === 1 ? '' : 's'} from an earlier session ${n === 1 ? 'is' : 'are'} still on disk. Open Local Storage to review or delete ${n === 1 ? 'it' : 'them'}.`,
          'info',
        );
      }
    } catch {
      // An API without the storage endpoints has nothing to report.
    }
  }, [apiClient, showFlash]);

  // Initialize
  useEffect(() => {
    if (mode === 'live') {
      testLiveConnection().then((connected) => {
        if (connected) {
          refreshProjects();
          refreshUploads();
          void reportLeftoverUploads();
        }
      });
    } else {
      setConnectionStatus('demo');
      refreshProjects();
    }
  }, [mode, testLiveConnection, refreshProjects, refreshUploads, reportLeftoverUploads]);

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
    themePreference,
    setThemePreference,
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
    applySpec,
    distributionColumn,
    setDistributionColumn,
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
