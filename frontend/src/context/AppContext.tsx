import React, { createContext, useContext, useState, useEffect, useCallback, useMemo } from 'react';
import {
  AppMode,
  NavigationTab,
  ProjectSummary,
  ProjectOverviewResponse,
} from '../types/api';
import { QuaeroApiClient, ApiError } from '../services/apiClient';
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
  setSelectedProject: (project: string) => void;
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
  refreshProjects: () => Promise<void>;
  refreshOverview: () => Promise<void>;
  testLiveConnection: () => Promise<boolean>;
  activeDatasetProfile: DatasetProfile | null;
  setActiveDatasetProfile: (profile: DatasetProfile | null) => void;
  loadSampleDataset: (sampleType?: 'release_impact' | 'global_indicators') => void;
}

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

  // Mode state - ALWAYS default to 'demo' for safety in preview & GitHub Pages
  const [mode, setModeState] = useState<AppMode>('demo');

  // Default to 'home'
  const [activeTab, setActiveTab] = useState<NavigationTab>('home');
  const [selectedProject, setSelectedProject] = useState<string>('release_impact');
  const [projects, setProjects] = useState<ProjectSummary[]>([]);
  const [currentOverview, setCurrentOverview] = useState<ProjectOverviewResponse | null>(null);
  const [loadingOverview, setLoadingOverview] = useState<boolean>(false);
  const [connectionStatus, setConnectionStatus] = useState<'connected' | 'checking' | 'disconnected' | 'demo'>('demo');
  const [isConnectionModalOpen, setIsConnectionModalOpen] = useState<boolean>(false);
  const [flashMessage, setFlashMessage] = useState<{ text: string; type: 'success' | 'info' | 'error' } | null>(null);

  // Active dataset profile (parsed in-browser from upload or sample)
  const [activeDatasetProfile, setActiveDatasetProfile] = useState<DatasetProfile | null>(() => {
    try {
      return parseCSV(SAMPLE_RELEASE_IMPACT_CSV, 'release_impact_sample.csv');
    } catch {
      return null;
    }
  });

  const loadSampleDataset = useCallback((sampleType: 'release_impact' | 'global_indicators' = 'release_impact') => {
    try {
      const isRelease = sampleType === 'release_impact';
      const rawText = isRelease ? SAMPLE_RELEASE_IMPACT_CSV : SAMPLE_GLOBAL_INDICATORS_CSV;
      const fileName = isRelease ? 'release_impact_sample.csv' : 'global_economic_indicators_sample.csv';

      const profile = parseCSV(rawText, fileName);
      setActiveDatasetProfile(profile);
      setFlashMessage({
        text: `Loaded sample ${fileName} into browser memory. Nothing is uploaded.`,
        type: 'success',
      });
    } catch (err: any) {
      setFlashMessage({ text: `Failed to load sample: ${err.message}`, type: 'error' });
    }
  }, []);

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
    setModeState(newMode);
    apiClient.setMode(newMode);
    // Never carry data from the other mode over (demo fixtures must not appear in Live).
    setProjects([]);
    setCurrentOverview(null);
    if (newMode === 'demo') {
      setConnectionStatus('demo');
      showFlash('Operating in Public Demo Sandbox. All data is processed locally.', 'info');
    } else {
      setConnectionStatus('checking');
      showFlash('Switched to Local Operations mode. Testing proxy connection to localhost:8000...', 'info');
    }
  }, [apiClient, showFlash]);

  const testLiveConnection = useCallback(async (): Promise<boolean> => {
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
  }, []);

  // Load projects list
  const refreshProjects = useCallback(async () => {
    try {
      const res = await apiClient.listProjects();
      setProjects(res.projects);
      if (res.projects.length > 0 && !res.projects.some((p) => p.project === selectedProject)) {
        setSelectedProject(res.projects[0].project);
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
        }
      });
    } else {
      setConnectionStatus('demo');
      refreshProjects();
    }
  }, [mode, testLiveConnection, refreshProjects]);

  useEffect(() => {
    refreshOverview();
  }, [selectedProject, refreshOverview]);

  const value = {
    mode,
    setMode,
    selectedProject,
    setSelectedProject,
    activeTab,
    setActiveTab,
    projects,
    currentOverview,
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
    testLiveConnection,
    activeDatasetProfile,
    setActiveDatasetProfile,
    loadSampleDataset,
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
