import React, { useState, useEffect, useRef, useMemo } from 'react';
import { useBackendStore } from '../store/backendStore';
import { safeApi } from '../api/safeApi';
import { BackendOnlineBanner } from '../components/BackendOnlineBanner';
import { DashboardSkeleton } from '../components/LoadingSkeleton';
import { useT } from '../i18n/I18nProvider';
import { SimpleLiveChart } from '../components/SimpleLiveChart';

const gradientClass = "min-h-screen bg-gradient-to-br from-slate-50 via-purple-50/30 to-slate-50 text-slate-900";
const REFRESH_INTERVAL = 3000; // 3 seconds — all chart data refetched from TimescaleDB so charts reflect continuous updates
const MIN_FETCH_INTERVAL = 2000; // Minimum time between fetches (throttling)

export default function Dashboard() {
  const t = useT();
  const [overview, setOverview] = useState<any>(null);
  const [predictions, setPredictions] = useState<any[]>([]);
  const [aiStatus, setAiStatus] = useState<any>(null);
  const [mssqlStatus, setMssqlStatus] = useState<any>(null);
  const [mssqlRows, setMssqlRows] = useState<any[]>([]);
  const [mssqlDerived, setMssqlDerived] = useState<any>(null);
  const [sensorHistoryRows, setSensorHistoryRows] = useState<any[]>([]);
  const [sensorHistoryRows24h, setSensorHistoryRows24h] = useState<any[]>([]);
  const [sensorHistoryDailyRows, setSensorHistoryDailyRows] = useState<any[]>([]);
  const [pressureConfig, setPressureConfig] = useState<any | null>(null);
  const [showPressureConfigModal, setShowPressureConfigModal] = useState(false);
  const [isSavingPressureConfig, setIsSavingPressureConfig] = useState(false);
  const [pressureConfigError, setPressureConfigError] = useState<string | null>(null);
  const [pressureConfigForm, setPressureConfigForm] = useState<{
    cool_from: number; cool_to: number;
    medium_from: number; medium_to: number;
    hot_from: number; hot_to: number;
    critical_from: number; critical_to: number;
    warning_threshold: number; critical_warning_threshold: number; low_pressure_warning_threshold: number;
    send_email_on_warning: boolean; send_email_on_critical: boolean;
    send_email_on_production_start: boolean; send_email_on_production_stop: boolean;
  }>({
    cool_from: 330, cool_to: 360,
    medium_from: 360, medium_to: 380,
    hot_from: 380, hot_to: 395,
    critical_from: 395, critical_to: 410,
    warning_threshold: 380, critical_warning_threshold: 395, low_pressure_warning_threshold: 340,
    send_email_on_warning: true, send_email_on_critical: true,
    send_email_on_production_start: false, send_email_on_production_stop: false,
  });
  const [machinesStats, setMachinesStats] = useState<any>(null);
  const [isDisconnecting, setIsDisconnecting] = useState(false);
  const [sensorsStats, setSensorsStats] = useState<any>(null);
  const [predictionsStats, setPredictionsStats] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isFallback, setIsFallback] = useState(false);
  const [selectedMachine] = useState<string>('BEX 92-28V');
  const [selectedMaterial, setSelectedMaterial] = useState<string>('Material 1');
  const [previousMaterial, setPreviousMaterial] = useState<string | null>(null);
  const [availableMaterials] = useState<string[]>(['Material 1', 'Material 2', 'Material 3']);
  const [materialChanges, setMaterialChanges] = useState<Array<{ material_id: string; timestamp: string }>>([]);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [isResetting, setIsResetting] = useState(false);
  const [resetMessage, setResetMessage] = useState<string | null>(null);
  const [machineState, setMachineState] = useState<string>('IDLE');
  const [machineStates, setMachineStates] = useState<any>({});
  const [currentDashboardData, setCurrentDashboardData] = useState<any>(null);
  const [showCreateProfileModal, setShowCreateProfileModal] = useState(false);
  const [isCreatingProfile, setIsCreatingProfile] = useState(false);
  const [createProfileError, setCreateProfileError] = useState<string | null>(null);
  const [chartTimeframe, setChartTimeframe] = useState<'1h' | '1d' | '1w' | '1m' | 'all'>('all');
  
  const backendStatus = useBackendStore((state) => state.status);
  const mountedRef = useRef(true);
  const lastFetchRef = useRef<number>(0);
  const intervalRef = useRef<NodeJS.Timeout | null>(null);
  
  useEffect(() => {
    mountedRef.current = true;
    
    const fetchDashboardData = async (isInitial = false) => {
      // Throttle: Don't fetch if last fetch was too recent
      const now = Date.now();
      if (!isInitial && (now - lastFetchRef.current < MIN_FETCH_INTERVAL)) {
        return;
      }
      lastFetchRef.current = now;
      
      if (!isInitial) {
        setIsLoading(false); // Don't show loading on refresh
      }
      
      try {
        // Fetch all data in parallel - matching backend endpoints
        const [overviewResult, predictionsResult, aiResult, machinesStatsResult, sensorsStatsResult, predictionsStatsResult, mssqlStatusResult, mssqlLatestResult, mssqlDerivedResult, sensorHistoryResult, sensorHistory24hResult, sensorHistoryDailyResult, machineStatesResult, machinesResult, currentDashboardResult, materialChangesResult] = await Promise.all([
          safeApi.get('/dashboard/overview'),
          safeApi.get('/predictions?limit=30&sort=desc'),
          safeApi.get('/ai/status'),
          safeApi.get('/dashboard/machines/stats'),
          safeApi.get('/dashboard/sensors/stats'),
          safeApi.get('/dashboard/predictions/stats'),
          safeApi.get('/dashboard/extruder/status'),
          safeApi.get('/dashboard/extruder/latest?limit=50'),
          safeApi.get('/dashboard/extruder/derived?window_minutes=30'),
          safeApi.get('/dashboard/extruder/history?days=365&limit=20000'),
          safeApi.get('/dashboard/extruder/history?hours=24&limit=500000'),
          safeApi.get('/dashboard/extruder/history/daily?days=730'),
          safeApi.get('/machine-state/states/current'),
          safeApi.get('/machines'), // Fetch machines list to match names with IDs
          safeApi.get(`/dashboard/current?material_id=${encodeURIComponent(selectedMaterial)}`), // Single source of truth for dashboard data
          safeApi.get('/dashboard/material/changes?limit=50'), // Fetch material change events for chart markers
        ]);
        
        if (!mountedRef.current) return;
        
        // Debug: Log machine states response
        if (machineStatesResult.data) {
          console.log('Machine States API Response:', machineStatesResult.data);
          Object.entries(machineStatesResult.data).forEach(([machineId, stateInfo]) => {
            console.log(`Machine ${machineId}: state=${stateInfo.state}, confidence=${stateInfo.confidence}`);
          });
        }
        
        const hasFallback = overviewResult.fallback || predictionsResult.fallback || 
                           aiResult.fallback ||
                           machinesStatsResult.fallback || sensorsStatsResult.fallback || predictionsStatsResult.fallback ||
                           mssqlStatusResult.fallback || mssqlLatestResult.fallback || mssqlDerivedResult.fallback || sensorHistoryResult.fallback || sensorHistory24hResult.fallback || sensorHistoryDailyResult.fallback ||
                           machineStatesResult.fallback || currentDashboardResult.fallback;
        setIsFallback(hasFallback);
        
        // If AI is offline or unhealthy, disable live updates
        const aiHealthy = aiResult.data && (aiResult.data.status === 'healthy' || aiResult.data.status === 'operational');
        if (aiResult.fallback || !aiHealthy) {
          setAutoRefresh(false);
        }
        
        // Batch state updates to prevent multiple re-renders
        if (overviewResult.data) setOverview(overviewResult.data);
        if (predictionsResult.data) setPredictions(Array.isArray(predictionsResult.data) ? predictionsResult.data : []);
        if (aiResult.data) setAiStatus(aiResult.data);
        if (mssqlStatusResult.data) setMssqlStatus(mssqlStatusResult.data);
        if ((mssqlLatestResult.data as any)?.rows) setMssqlRows(((mssqlLatestResult.data as any).rows as any[]) || []);
        if (mssqlDerivedResult.data) setMssqlDerived(mssqlDerivedResult.data);
        if ((sensorHistoryResult.data as any)?.rows) setSensorHistoryRows(((sensorHistoryResult.data as any).rows as any[]) || []);
        else if (sensorHistoryResult.fallback) setSensorHistoryRows([]);
        if ((sensorHistory24hResult.data as any)?.rows) setSensorHistoryRows24h(((sensorHistory24hResult.data as any).rows as any[]) || []);
        else if (sensorHistory24hResult.fallback) setSensorHistoryRows24h([]);
        if ((sensorHistoryDailyResult.data as any)?.rows) setSensorHistoryDailyRows(((sensorHistoryDailyResult.data as any).rows as any[]) || []);
        else if (sensorHistoryDailyResult.fallback) setSensorHistoryDailyRows([]);
        if (machinesStatsResult.data) setMachinesStats(machinesStatsResult.data);
        if (sensorsStatsResult.data) setSensorsStats(sensorsStatsResult.data);
        if (predictionsStatsResult.data) setPredictionsStats(predictionsStatsResult.data);
        // Update machine states first (for reference, but dashboard/current takes priority)
        let machineStateFromStatesAPI: string | null = null;
        if (machineStatesResult.data) {
          setMachineStates(machineStatesResult.data);
          
          // Find state for selected machine
          let selectedMachineState = 'IDLE'; // default
          const states = machineStatesResult.data;
          
          // Try to find machine ID from machines list
          let selectedMachineId: string | null = null;
          if (machinesResult.data && Array.isArray(machinesResult.data)) {
            const machine = machinesResult.data.find((m: any) => 
              m.name === selectedMachine || m.id === selectedMachine || String(m.id) === selectedMachine
            );
            if (machine) {
              selectedMachineId = String(machine.id);
              console.log(`Found machine: ${machine.name} (ID: ${selectedMachineId})`);
            }
          }
          
          // If we found a machine ID, use it to find the state
          if (selectedMachineId && states[selectedMachineId]) {
            const stateInfo = states[selectedMachineId] as any;
            if (stateInfo && stateInfo.state) {
              selectedMachineState = String(stateInfo.state).trim().toUpperCase();
              console.log(`✅ Found state for machine ${selectedMachineId}: ${selectedMachineState}`);
            }
          } else {
            // Fallback: use first machine's state if only one machine exists
            const machineIds = Object.keys(states);
            if (machineIds.length === 1) {
              const firstState = states[machineIds[0]] as any;
              if (firstState && firstState.state) {
                selectedMachineState = String(firstState.state).trim().toUpperCase();
                console.log(`Using first (and only) machine state: ${selectedMachineState}`);
              }
            } else if (machineIds.length > 0) {
              // Multiple machines - use the first one as fallback
              const firstState = states[machineIds[0]] as any;
              if (firstState && firstState.state) {
                selectedMachineState = String(firstState.state).trim().toUpperCase();
                console.log(`Using first available machine state: ${selectedMachineState} (from ${machineIds.length} machines)`);
              }
            } else {
              console.warn('No machine states found in API response');
            }
          }
          
          machineStateFromStatesAPI = selectedMachineState;
        }
        
        // Update machine state from current dashboard data (THIS IS THE SOURCE OF TRUTH for baseline learning)
        // Dashboard/current API state takes priority because it's what the baseline learning message is based on
        if (currentDashboardResult.data) {
          setCurrentDashboardData(currentDashboardResult.data);
          if (currentDashboardResult.data.machine_state) {
            const state = String(currentDashboardResult.data.machine_state).trim().toUpperCase();
            setMachineState(state);
            console.log('Machine state from dashboard/current (PRIORITY):', state);
          } else if (machineStateFromStatesAPI) {
            // Fallback to machine-state API if dashboard doesn't provide state
            setMachineState(machineStateFromStatesAPI);
            console.log('Machine state from machine-state API (fallback):', machineStateFromStatesAPI);
          }
        } else if (machineStateFromStatesAPI) {
          // Fallback if dashboard data is not available
          setMachineState(machineStateFromStatesAPI);
          console.log('Machine state from machine-state API (fallback, no dashboard data):', machineStateFromStatesAPI);
        }
        
        // Update material changes for chart markers
        if (materialChangesResult.data && materialChangesResult.data.material_changes) {
          setMaterialChanges(materialChangesResult.data.material_changes || []);
        }
        
      } catch (error) {
        console.error('Dashboard fetch error:', error);
        if (mountedRef.current) {
          setIsFallback(true);
        }
      } finally {
        if (mountedRef.current) {
          setIsLoading(false);
        }
      }
    };
    
    // Initial fetch
    fetchDashboardData(true);
    
    // Real-time updates: Refresh every REFRESH_INTERVAL when online
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
    }
    
    intervalRef.current = setInterval(() => {
      if (mountedRef.current && backendStatus === 'online') {
        fetchDashboardData(false);
      }
    }, REFRESH_INTERVAL);
    
    return () => {
      mountedRef.current = false;
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [backendStatus]);

  // Load melt-pressure configuration linked to the active profile (per material / optional batch).
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const profileId = (currentDashboardData as any)?.profile_id;
      if (!profileId) {
        setPressureConfig(null);
        return;
      }
      const res = await safeApi.get(`/profiles/${profileId}/pressure-config`);
      if (cancelled) return;
      if (!res.fallback && res.data) setPressureConfig(res.data);
      else setPressureConfig(null);
    })();
    return () => {
      cancelled = true;
    };
  }, [currentDashboardData?.profile_id]);

  // Calculate anomalies count
  const anomaliesCount = predictions?.filter((p: any) => p.prediction === 'anomaly').length || 0;
  
  if (isLoading) {
    return (
      <div className={gradientClass}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <DashboardSkeleton />
        </div>
      </div>
    );
  }
  
  return (
    <div className={gradientClass}>
      <BackendOnlineBanner />
      <div className="max-w-[1920px] mx-auto px-6 py-6">
        {/* Top Header Section */}
        <div className="mb-8">
          <div className="flex flex-col sm:flex-row sm:justify-between sm:items-start gap-4 mb-6">
            <div>
              <h1 className="text-3xl text-slate-900 mb-2">
                Extruder Überwachungsdashboard
              </h1>
              <p className="text-slate-600 text-sm">
                Predictive Maintenance für Kunststoffextrusion
              </p>
            </div>
          </div>
          
          {/* Status Cards Row */}
          <div className="flex flex-wrap gap-4">
            <div className="bg-white/95 backdrop-blur-sm border border-slate-200/80 rounded-xl px-5 py-3 shadow-md hover:shadow-lg transition-all duration-300 hover:scale-[1.02] flex-1 min-w-[180px]">
              <div className="flex items-center gap-2 mb-1">
                <div className={`w-2 h-2 rounded-full ${
                  !aiStatus || aiStatus.status === 'unavailable' ? 'bg-rose-500' :
                  (aiStatus.status === 'healthy' || aiStatus.status === 'operational' ? 'bg-emerald-500 animate-pulse' : 'bg-amber-500')
                }`}></div>
                <span className="text-xs text-slate-500">KI-DIENST</span>
              </div>
              <div className={`text-base ${
                !aiStatus || aiStatus.status === 'unavailable' ? 'text-rose-600' :
                (aiStatus.status === 'healthy' || aiStatus.status === 'operational' ? 'text-emerald-600' : 'text-amber-600')
              }`}>
                {!aiStatus || aiStatus.status === 'unavailable'
                  ? 'Offline'
                  : (aiStatus.status === 'healthy' || aiStatus.status === 'operational' ? 'Betriebsbereit' : 'Beeinträchtigt')}
              </div>
            </div>
            <div className="bg-white/95 backdrop-blur-sm border border-slate-200/80 rounded-xl px-5 py-3 shadow-md hover:shadow-lg transition-all duration-300 hover:scale-[1.02] flex-1 min-w-[180px]">
              <div className="flex items-center gap-2 mb-1">
                <div className={`w-2 h-2 rounded-full ${
                  !mssqlStatus ? 'bg-slate-400' :
                  (!mssqlStatus.configured ? 'bg-amber-500' : 
                  (mssqlStatus.last_error ? 'bg-rose-500' : 'bg-emerald-500 animate-pulse'))
                }`}></div>
                <span className="text-xs text-slate-500">MSSQL</span>
              </div>
              <div className={`text-base ${
                !mssqlStatus ? 'text-slate-600' :
                (!mssqlStatus.configured ? 'text-amber-600' : 
                (mssqlStatus.last_error ? 'text-rose-600' : 'text-emerald-600'))
              }`}>
                {!mssqlStatus
                  ? 'Unbekannt'
                  : (!mssqlStatus.configured ? 'Nicht konfiguriert' : (mssqlStatus.last_error ? 'Fehler' : 'Verbunden'))}
              </div>
              {mssqlStatus?.last_error ? (
                <div className="text-xs text-rose-700 mt-2 max-w-[260px] truncate" title={String(mssqlStatus.last_error)}>
                  {String(mssqlStatus.last_error)}
                </div>
              ) : null}
            </div>
            <div className="bg-white/95 backdrop-blur-sm border border-slate-200/80 rounded-xl px-5 py-3 shadow-md hover:shadow-lg transition-all duration-300 hover:scale-[1.02] flex-1 min-w-[180px]">
              <div className="flex items-center gap-2 mb-1">
                <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse"></div>
                <span className="text-xs text-slate-500">SYSTEMSTATUS</span>
              </div>
              <div className="text-base text-emerald-600">Alle Systeme betriebsbereit</div>
            </div>
          </div>
        </div>

        {/* Machine State Display */}
        <div className="bg-white/95 backdrop-blur-sm rounded-2xl p-6 border border-slate-200/80 shadow-lg hover:shadow-xl transition-all duration-300 mb-8">
          <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-6">
            <div className="flex-1">
              <h3 className="text-xl text-slate-900 mb-2">
                Maschinenzustand
              </h3>
              <div className="text-sm text-slate-600 leading-relaxed">
                Zustandsdefinitionen: AUS (Maschine aus/kalt) | BEREIT (Warm/bereit) | AUFHEIZEN (Aufwärmen) | PRODUKTION (Prozess läuft) | ABKÜHLEN (Abkühlen)
              </div>
            </div>
            <div className="text-right lg:text-right">
              <div className="text-sm text-slate-500 mb-2">Aktueller Zustand</div>
              <div className={`inline-block text-xl px-4 py-2 rounded-lg ${
                machineState === 'PRODUCTION' ? 'bg-emerald-100 text-emerald-800 border border-emerald-300' :
                machineState === 'HEATING' ? 'bg-amber-100 text-amber-800 border border-amber-300' :
                machineState === 'COOLING' ? 'bg-blue-100 text-blue-800 border border-blue-300' :
                machineState === 'IDLE' ? 'bg-slate-100 text-slate-800 border border-slate-300' :
                machineState === 'OFF' ? 'bg-red-100 text-red-800 border border-red-300' :
                'bg-gray-100 text-gray-800 border border-gray-300'
              }`}>
                {machineState}
              </div>
              <div className="text-sm text-slate-600 mt-3 space-y-1">
                {machineState === 'PRODUCTION' && <div className="flex items-center gap-2"><span className="text-emerald-600">🟢</span> Prozess aktiv - Ampelbewertung aktiviert</div>}
                {machineState === 'HEATING' && <div className="flex items-center gap-2"><span className="text-amber-600">🔥</span> Aufwärmen - Vorbereitung auf Produktion</div>}
                {machineState === 'COOLING' && <div className="flex items-center gap-2"><span className="text-blue-600">❄️</span> Abkühlen - Nachproduktionszyklus</div>}
                {machineState === 'IDLE' && <div className="flex items-center gap-2"><span className="text-slate-600">⏸️</span> Bereit - Warten auf Produktionsstart</div>}
                {machineState === 'OFF' && <div className="flex items-center gap-2"><span className="text-red-600">🔴</span> Maschine aus - Keine Heizung aktiv</div>}
              </div>
              {/* Baseline Status */}
              {currentDashboardData?.baseline_status && currentDashboardData.baseline_status !== 'learning' && (
                <div className="mt-3 text-sm">
                  <span className="text-slate-600">Baseline:</span> 
                  <span className={`ml-2 ${
                    currentDashboardData.baseline_status === 'ready' ? 'text-emerald-600' : 
                    currentDashboardData.baseline_status === 'not_ready' ? 'text-amber-600' : 
                    'text-rose-600'
                  }`}>
                    {currentDashboardData.baseline_status === 'ready' ? '✅ Ready' : 
                     currentDashboardData.baseline_status === 'not_ready' ? '⏳ Not Ready' : 
                     '❌ Not Available'}
                  </span>
                </div>
              )}
              {/* Profile Status */}
              {currentDashboardData?.profile_status && (
                <div className="mt-2 text-sm">
                  <span className="text-slate-600">Profile:</span> 
                  <span className={`ml-2 ${
                    currentDashboardData.profile_status === 'active' ? 'text-emerald-600' : 'text-rose-600'
                  }`}>
                    {currentDashboardData.profile_status === 'active' ? '✅ Active' : '❌ Not Available'}
                  </span>
                  {currentDashboardData.profile_id && (
                    <span className="ml-2 text-xs text-slate-400">(ID: {currentDashboardData.profile_id.substring(0, 8)}...)</span>
                  )}
                </div>
              )}
              {/* Profile Not Available - Create Profile Banner */}
              {currentDashboardData?.profile_status === 'not_available' && (
                <div className="mt-4 px-4 py-3 bg-amber-50 border border-amber-200 rounded-lg">
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1">
                      <div className="text-sm text-amber-800 mb-1">
                        <span>⚠️ No Profile Found</span>
                      </div>
                      <div className="text-xs text-amber-700">
                        A profile is required for baseline learning and evaluation. Create a profile for "{selectedMaterial}" to enable baseline tracking.
                      </div>
                    </div>
                    <button
                      onClick={() => setShowCreateProfileModal(true)}
                      className="px-4 py-2 bg-amber-600 hover:bg-amber-700 text-white text-sm rounded-md transition-colors duration-200 whitespace-nowrap"
                    >
                      Create Profile
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Machine and Material Selection - Static Display */}
        <div className="bg-white/95 backdrop-blur-sm rounded-2xl p-6 border border-slate-200/80 shadow-lg hover:shadow-xl transition-all duration-300 mb-8">
          <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-6">
            <div className="flex flex-col lg:flex-row lg:items-center gap-4 lg:gap-8">
              <div>
                <h2 className="text-lg text-slate-900">
                  Produktionskonfiguration
                </h2>
              </div>
              <div className="px-4 py-2 bg-purple-50 rounded-lg border border-purple-200">
                <p className="text-base text-purple-900">Kunststoffwerk ZITTA GmbH</p>
              </div>
            </div>
            <div className="flex flex-col sm:flex-row gap-4 sm:gap-6">
              <div className="min-w-[140px] sm:min-w-[160px]">
                <label className="block text-xs text-slate-600 mb-2">Maschine</label>
                <div className="bg-white border border-slate-300 rounded-md px-4 py-2 text-sm text-slate-900 whitespace-nowrap text-center">
                  {selectedMachine}
                </div>
              </div>
              <div className="min-w-[140px] sm:min-w-[160px]">
                <label className="block text-xs text-slate-600 mb-2">Material</label>
                <select
                  value={selectedMaterial}
                  onChange={async (e) => {
                    const newMaterial = e.target.value;
                    const oldMaterial = selectedMaterial;
                    
                    // Log material change to backend
                    try {
                      await safeApi.post(`/dashboard/material/change?material_id=${encodeURIComponent(newMaterial)}${oldMaterial ? `&previous_material=${encodeURIComponent(oldMaterial)}` : ''}`);
                    } catch (error) {
                      console.error('Failed to log material change:', error);
                      // Continue even if logging fails
                    }
                    
                    setPreviousMaterial(oldMaterial);
                    setSelectedMaterial(newMaterial);
                    // Trigger data reload when material changes
                    lastFetchRef.current = 0;
                    fetchDashboardData(false);
                  }}
                  className="bg-white border border-slate-300 rounded-md px-4 py-2 text-sm text-slate-900 w-full cursor-pointer hover:border-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all duration-200"
                >
                  {availableMaterials.map((material) => (
                    <option key={material} value={material}>
                      {material}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          </div>
        </div>

        {/* KPI Cards Section */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6 mb-8">
          {/* Schneckendrehzahl */}
          <div className="bg-white/95 backdrop-blur-sm rounded-2xl p-6 border border-slate-200/80 shadow-lg hover:shadow-2xl transition-all duration-300 hover:scale-[1.02] relative overflow-hidden group">
            <div className="absolute inset-0 bg-gradient-to-br from-emerald-50/50 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300"></div>
            <div className="relative z-10">
              <div className="text-sm text-slate-600 mb-3">Schneckendrehzahl (ScrewSpeed_rpm)</div>
              <div className="text-5xl mb-3">
                <span className={
                  machineState === 'PRODUCTION' ? 
                  (currentDashboardData?.metrics?.ScrewSpeed_rpm?.severity === 2 ? 'text-rose-600' :
                   currentDashboardData?.metrics?.ScrewSpeed_rpm?.severity === 1 ? 'text-amber-600' :
                   currentDashboardData?.metrics?.ScrewSpeed_rpm?.severity === 0 ? 'text-emerald-600' :
                   'text-slate-400') :
                  'text-slate-400'  // Neutral color when not in production
                }>
                  {currentDashboardData?.metrics?.ScrewSpeed_rpm?.current_value !== undefined 
                    ? currentDashboardData.metrics.ScrewSpeed_rpm.current_value.toFixed(1) 
                    : (mssqlRows?.[0]?.ScrewSpeed_rpm ? parseFloat(mssqlRows[0].ScrewSpeed_rpm).toFixed(1) : '--')}
                </span>
                <span className="text-2xl text-slate-500 ml-2">rpm</span>
              </div>
              {/* Baseline Mean */}
              {machineState === 'PRODUCTION' && currentDashboardData?.metrics?.ScrewSpeed_rpm?.baseline_mean !== undefined && (
                <div className="text-xs text-slate-600 mb-1">
                  Baseline Mean: {currentDashboardData.metrics.ScrewSpeed_rpm.baseline_mean.toFixed(1)} rpm
                  {currentDashboardData?.metrics?.ScrewSpeed_rpm?.baseline?.baseline_material ? (
                    <span className="text-slate-500"> (Material: {currentDashboardData.metrics.ScrewSpeed_rpm.baseline.baseline_material})</span>
                  ) : null}
                </div>
              )}
              {/* Green Band */}
              {machineState === 'PRODUCTION' && currentDashboardData?.metrics?.ScrewSpeed_rpm?.green_band && (
                <div className="text-xs text-slate-600 mb-1">
                  Green Band: {currentDashboardData.metrics.ScrewSpeed_rpm.green_band.min.toFixed(1)} - {currentDashboardData.metrics.ScrewSpeed_rpm.green_band.max.toFixed(1)} rpm
                </div>
              )}
              {/* Explanation */}
              {machineState === 'PRODUCTION' && currentDashboardData?.metrics?.ScrewSpeed_rpm?.explanation && (
                <div className="text-xs text-slate-600 mt-2">
                  {String(currentDashboardData.metrics.ScrewSpeed_rpm.explanation)}
                </div>
              )}
              {/* Deviation */}
              {machineState === 'PRODUCTION' && currentDashboardData?.metrics?.ScrewSpeed_rpm?.deviation !== undefined && (
                <div className={`text-xs mb-1 ${
                  Math.abs(currentDashboardData.metrics.ScrewSpeed_rpm.deviation) > 5 ? 'text-amber-600' : 'text-slate-600'
                }`}>
                  Deviation: {currentDashboardData.metrics.ScrewSpeed_rpm.deviation > 0 ? '+' : ''}{currentDashboardData.metrics.ScrewSpeed_rpm.deviation.toFixed(1)} rpm
                </div>
              )}
              <div className="text-xs text-slate-500 mb-1">
                Berechnung: Direkte Messung vom Drehzahlsensor
              </div>
              <div className="text-xs text-slate-500 mb-2">
                Referenz: {machineState === 'PRODUCTION' ? 'Materialabhängiger optimaler Bereich aus Baseline-Daten' : 'Prozessbewertung nur in PRODUCTION Zustand'}
              </div>
              <div className="text-xs text-slate-600">
                {machineState === 'PRODUCTION' ? (
                  <>
                    {currentDashboardData?.metrics?.ScrewSpeed_rpm?.severity === 0 && 
                      "🟢 Schneckendrehzahl stabil. Ruhiger Materialdurchsatz im optimalen Bereich für dieses Material."}
                    {currentDashboardData?.metrics?.ScrewSpeed_rpm?.severity === 1 && 
                      "🟠 Schneckendrehzahl weicht vom Referenzbereich ab. Mögliche Veränderung des Materialdurchsatzes oder beginnende Prozessinstabilität."}
                    {currentDashboardData?.metrics?.ScrewSpeed_rpm?.severity === 2 && 
                      "🔴 Schneckendrehzahl außerhalb des materialabhängigen Betriebsfensters. Risiko für Druckinstabilität, Qualitätsschwankungen oder Werkzeugbelastung."}
                    {currentDashboardData?.metrics?.ScrewSpeed_rpm?.severity === undefined && 
                      "⏳ Bewertung wird berechnet..."}
                  </>
                ) : (
                  `⏸️ Maschine im ${machineState} Zustand - keine Prozessbewertung`
                )}
              </div>
            </div>
          </div>

          {/* Schmelzedruck */}
          <div className="bg-white/95 backdrop-blur-sm rounded-2xl p-6 border border-slate-200/80 shadow-lg hover:shadow-2xl transition-all duration-300 hover:scale-[1.02] relative overflow-hidden group">
            <div className="absolute inset-0 bg-gradient-to-br from-blue-50/50 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300"></div>
            <div className="relative z-10">
              <div className="flex items-center justify-between gap-3 mb-3">
                <div className="text-sm text-slate-600">Schmelzedruck (Pressure_bar)</div>
                {currentDashboardData?.profile_id ? (
                  <button
                    type="button"
                    onClick={() => {
                      setPressureConfigForm(pressureConfig ? {
                        cool_from: pressureConfig.ranges?.cool?.from ?? 330,
                        cool_to: pressureConfig.ranges?.cool?.to ?? 360,
                        medium_from: pressureConfig.ranges?.medium?.from ?? 360,
                        medium_to: pressureConfig.ranges?.medium?.to ?? 380,
                        hot_from: pressureConfig.ranges?.hot?.from ?? 380,
                        hot_to: pressureConfig.ranges?.hot?.to ?? 395,
                        critical_from: pressureConfig.ranges?.critical?.from ?? 395,
                        critical_to: pressureConfig.ranges?.critical?.to ?? 410,
                        warning_threshold: pressureConfig.thresholds?.warning ?? 380,
                        critical_warning_threshold: pressureConfig.thresholds?.critical ?? 395,
                        low_pressure_warning_threshold: pressureConfig.thresholds?.low ?? 340,
                        send_email_on_warning: pressureConfig.emails?.on_warning ?? true,
                        send_email_on_critical: pressureConfig.emails?.on_critical ?? true,
                        send_email_on_production_start: pressureConfig.emails?.on_production_start ?? false,
                        send_email_on_production_stop: pressureConfig.emails?.on_production_stop ?? false,
                      } : {
                        cool_from: 330, cool_to: 360,
                        medium_from: 360, medium_to: 380,
                        hot_from: 380, hot_to: 395,
                        critical_from: 395, critical_to: 410,
                        warning_threshold: 380, critical_warning_threshold: 395, low_pressure_warning_threshold: 340,
                        send_email_on_warning: true, send_email_on_critical: true,
                        send_email_on_production_start: false, send_email_on_production_stop: false,
                      });
                      setPressureConfigError(null);
                      setShowPressureConfigModal(true);
                    }}
                    className="text-sm px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-slate-700 hover:bg-slate-100 hover:border-slate-400 shadow-sm"
                  >
                    Edit melt pressure ranges
                  </button>
                ) : (
                  <span className="text-xs text-slate-400">Configure ranges (requires active profile)</span>
                )}
              </div>
              <div className="text-5xl mb-3">
                <span className={
                  machineState === 'PRODUCTION' ? 
                  (currentDashboardData?.metrics?.Pressure_bar?.severity === 2 ? 'text-rose-600' :
                   currentDashboardData?.metrics?.Pressure_bar?.severity === 1 ? 'text-amber-600' :
                   currentDashboardData?.metrics?.Pressure_bar?.severity === 0 ? 'text-emerald-600' :
                   'text-slate-400') :
                  'text-slate-400'  // Neutral color when not in production
                }>
                  {currentDashboardData?.metrics?.Pressure_bar?.current_value !== undefined 
                    ? currentDashboardData.metrics.Pressure_bar.current_value.toFixed(1) 
                    : (mssqlRows?.[0]?.Pressure_bar ? parseFloat(mssqlRows[0].Pressure_bar).toFixed(1) : '--')}
                </span>
                <span className="text-2xl text-slate-500 ml-2">bar</span>
              </div>
              {/* Baseline Mean */}
              {machineState === 'PRODUCTION' && currentDashboardData?.metrics?.Pressure_bar?.baseline_mean !== undefined && (
                <div className="text-xs text-slate-600 mb-1">
                  Baseline Mean: {currentDashboardData.metrics.Pressure_bar.baseline_mean.toFixed(1)} bar
                  {currentDashboardData?.metrics?.Pressure_bar?.baseline?.baseline_material ? (
                    <span className="text-slate-500"> (Material: {currentDashboardData.metrics.Pressure_bar.baseline.baseline_material})</span>
                  ) : null}
                </div>
              )}
              {/* Green Band */}
              {machineState === 'PRODUCTION' && currentDashboardData?.metrics?.Pressure_bar?.green_band && (
                <div className="text-xs text-slate-600 mb-1">
                  Green Band: {currentDashboardData.metrics.Pressure_bar.green_band.min.toFixed(1)} - {currentDashboardData.metrics.Pressure_bar.green_band.max.toFixed(1)} bar
                </div>
              )}
              {/* Explanation */}
              {machineState === 'PRODUCTION' && currentDashboardData?.metrics?.Pressure_bar?.explanation && (
                <div className="text-xs text-slate-600 mt-2">
                  {String(currentDashboardData.metrics.Pressure_bar.explanation)}
                </div>
              )}
              {/* Deviation */}
              {machineState === 'PRODUCTION' && currentDashboardData?.metrics?.Pressure_bar?.deviation !== undefined && (
                <div className={`text-xs mb-1 ${
                  Math.abs(currentDashboardData.metrics.Pressure_bar.deviation) > 10 ? 'text-amber-600' : 'text-slate-600'
                }`}>
                  Deviation: {currentDashboardData.metrics.Pressure_bar.deviation > 0 ? '+' : ''}{currentDashboardData.metrics.Pressure_bar.deviation.toFixed(1)} bar
                </div>
              )}
              <div className="text-xs text-slate-500 mb-1">
                Berechnung: Direkte Messung vom Drucksensor im Extruder
              </div>
              <div className="text-xs text-slate-500 mb-2">
                Referenz: {machineState === 'PRODUCTION' ? 'Materialabhängiger optimaler Druckbereich aus historischen Prozessdaten' : 'Prozessbewertung nur in PRODUCTION Zustand'}
              </div>
              <div className="text-xs text-slate-600">
                {machineState === 'PRODUCTION' ? (
                  <>
                    {currentDashboardData?.metrics?.Pressure_bar?.severity === 0 && 
                      "🟢 Prozessdruck stabil. Gleichmäßiger Materialfluss ohne Anzeichen von Verstopfung oder Überlast."}
                    {currentDashboardData?.metrics?.Pressure_bar?.severity === 1 && 
                      "🟠 Abweichender Prozessdruck. Mögliche Änderungen in Materialviskosität, Temperaturverteilung oder beginnende Ablagerungen."}
                    {currentDashboardData?.metrics?.Pressure_bar?.severity === 2 && 
                      "🔴 Kritische Druckabweichung. Erhöhtes Risiko für Werkzeugüberlast, Materialabbau oder Produktionsstopp."}
                    {currentDashboardData?.metrics?.Pressure_bar?.severity === undefined && 
                      "⏳ Bewertung wird berechnet..."}
                  </>
                ) : (
                  `⏸️ Maschine im ${machineState} Zustand - keine Prozessbewertung`
                )}
              </div>
              {/* Druckzonen- und Warnhinweise basierend auf absoluten Grenzwerten */}
              {machineState === 'PRODUCTION' && (
                <div className="text-xs text-slate-700 mt-3 space-y-1">
                  {(() => {
                    const rawValue =
                      currentDashboardData?.metrics?.Pressure_bar?.current_value !== undefined
                        ? currentDashboardData.metrics.Pressure_bar.current_value
                        : (mssqlRows?.[0]?.Pressure_bar
                            ? parseFloat(mssqlRows[0].Pressure_bar)
                            : NaN);
                    const v = typeof rawValue === 'number' ? rawValue : NaN;
                    const coolFrom = pressureConfig?.ranges?.cool?.from ?? 330;
                    const coolTo = pressureConfig?.ranges?.cool?.to ?? 360;
                    const mediumFrom = pressureConfig?.ranges?.medium?.from ?? 360;
                    const mediumTo = pressureConfig?.ranges?.medium?.to ?? 380;
                    const hotFrom = pressureConfig?.ranges?.hot?.from ?? 380;
                    const hotTo = pressureConfig?.ranges?.hot?.to ?? 395;
                    const criticalFrom = pressureConfig?.ranges?.critical?.from ?? 395;
                    const criticalTo = pressureConfig?.ranges?.critical?.to ?? 410;
                    if (!Number.isFinite(v)) {
                      return null;
                    }
                    if (v >= criticalFrom) {
                      return (
                        <>
                          <div className="font-semibold text-rose-700">Druck über Grenzwert</div>
                          <div className="text-rose-700">Kritisch hoher Druck ({criticalFrom}–{criticalTo} bar)</div>
                        </>
                      );
                    }
                    if (v < coolFrom) {
                      return (
                        <>
                          <div className="font-semibold text-teal-700">Druck unter zulässigem Bereich</div>
                          <div className="text-teal-700">Unter {coolFrom} bar</div>
                        </>
                      );
                    }
                    if (v >= mediumFrom && v < mediumTo) {
                      return (
                        <div className="text-emerald-700">
                          Normaler Produktionsbereich ({mediumFrom}–{mediumTo} bar)
                        </div>
                      );
                    }
                    if (v >= coolFrom && v < coolTo) {
                      return (
                        <div className="text-teal-700">
                          Unterer Bereich ({coolFrom}–{coolTo} bar)
                        </div>
                      );
                    }
                    if (v >= hotFrom && v < hotTo) {
                      return (
                        <div className="text-amber-700">
                          Erhöhter Druck ({hotFrom}–{hotTo} bar)
                        </div>
                      );
                    }
                    return null;
                  })()}
                </div>
              )}
            </div>
          </div>

          {/* Durchschnittstemperatur */}
          <div className="bg-white/95 backdrop-blur-sm rounded-2xl p-6 border border-slate-200/80 shadow-lg hover:shadow-2xl transition-all duration-300 hover:scale-[1.02] relative overflow-hidden group">
            <div className="absolute inset-0 bg-gradient-to-br from-amber-50/50 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300"></div>
            <div className="relative z-10">
              <div className="text-sm text-slate-600 mb-3">Durchschnittstemperatur (Temp_Avg)</div>
              <div className="text-5xl mb-3">
                <span className={
                  machineState === 'PRODUCTION' ? 
                  (currentDashboardData?.metrics?.Temp_Avg?.severity === 2 ? 'text-rose-600' :
                   currentDashboardData?.metrics?.Temp_Avg?.severity === 1 ? 'text-amber-600' :
                   currentDashboardData?.metrics?.Temp_Avg?.severity === 0 ? 'text-emerald-600' :
                   mssqlDerived?.risk?.overall === 'red' ? 'text-rose-600' :
                   mssqlDerived?.risk?.overall === 'yellow' ? 'text-amber-600' :
                   mssqlDerived?.risk?.overall === 'green' ? 'text-emerald-600' :
                   'text-slate-400') :
                  'text-slate-400'  // Neutral color when not in production
                }>
                  {currentDashboardData?.metrics?.Temp_Avg?.current_value !== undefined 
                    ? currentDashboardData.metrics.Temp_Avg.current_value.toFixed(1) 
                    : (mssqlDerived?.derived?.Temp_Avg?.current?.toFixed(1) || '--')}
                </span>
                <span className="text-2xl text-slate-500 ml-2">°C</span>
              </div>
              <div className="text-xs text-slate-500 mb-1">
                Berechnung: (Zone1 + Zone2 + Zone3 + Zone4) ÷ 4
              </div>
              <div className="text-xs text-slate-500 mb-2">
                Referenz: {machineState === 'PRODUCTION' ? 'Materialabhängiger optimaler Temperaturbereich aus Baseline-Daten' : 'Prozessbewertung nur in PRODUCTION Zustand'}
              </div>
              <div className="text-xs text-slate-600">
                {machineState === 'PRODUCTION' ? (
                  <>
                    {currentDashboardData?.metrics?.Temp_Avg?.severity === 0 && 
                      "🟢 Gesamte Temperatur im optimalen Bereich. Gleichmäßige Plastifizierung sichergestellt."}
                    {currentDashboardData?.metrics?.Temp_Avg?.severity === 1 && 
                      "🟠 Temperatur außerhalb des optimalen Bereichs. Anpassung der Heizzone empfohlen."}
                    {currentDashboardData?.metrics?.Temp_Avg?.severity === 2 && 
                      "🔴 Kritische Temperaturabweichung. Risiko für Materialabbau oder unvollständige Plastifizierung."}
                    {currentDashboardData?.metrics?.Temp_Avg?.severity === undefined && 
                      (mssqlDerived?.derived?.Temp_Avg?.current ? (
                        <>
                          {mssqlDerived?.derived?.Temp_Avg?.current >= 180 && mssqlDerived?.derived?.Temp_Avg?.current <= 220 &&
                            "🟢 Gesamte Temperatur im optimalen Bereich. Gleichmäßige Plastifizierung sichergestellt."}
                          {((mssqlDerived?.derived?.Temp_Avg?.current < 180) || (mssqlDerived?.derived?.Temp_Avg?.current > 220)) &&
                            "🟠 Temperatur außerhalb des optimalen Bereichs. Anpassung der Heizzone empfohlen."}
                          {((mssqlDerived?.derived?.Temp_Avg?.current < 160) || (mssqlDerived?.derived?.Temp_Avg?.current > 240)) &&
                            "🔴 Kritische Temperaturabweichung. Risiko für Materialabbau oder unvollständige Plastifizierung."}
                        </>
                      ) : "⏳ Bewertung wird berechnet...")}
                  </>
                ) : (
                  `⏸️ Maschine im ${machineState} Zustand - keine Prozessbewertung`
                )}
              </div>
            </div>
          </div>

          {/* Temperaturspreizung */}
          <div className="bg-white/95 backdrop-blur-sm rounded-2xl p-6 border border-slate-200/80 shadow-lg hover:shadow-2xl transition-all duration-300 hover:scale-[1.02] relative overflow-hidden group">
            <div className="absolute inset-0 bg-gradient-to-br from-rose-50/50 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300"></div>
            <div className="relative z-10">
              <div className="text-sm text-slate-600 mb-3">Temperaturspreizung (Temp_Spread)</div>
              <div className="text-5xl mb-3">
                <span className={
                  (machineState === 'PRODUCTION' && currentDashboardData?.spread_status === 'red') ? 'text-rose-600' :
                  (machineState === 'PRODUCTION' && currentDashboardData?.spread_status === 'orange') ? 'text-amber-600' :
                  (machineState === 'PRODUCTION' && currentDashboardData?.spread_status === 'green') ? 'text-emerald-600' :
                  machineState === 'PRODUCTION' ? 
                  ((mssqlDerived?.derived?.Temp_Spread?.current || 0) > 8 ? 'text-rose-600' :
                   (mssqlDerived?.derived?.Temp_Spread?.current || 0) > 5 ? 'text-amber-600' :
                   'text-emerald-600') :
                  'text-slate-400'  // Neutral color when not in production
                }>
                  {currentDashboardData?.metrics?.Temp_Spread?.current_value !== undefined 
                    ? currentDashboardData.metrics.Temp_Spread.current_value.toFixed(1) 
                    : (mssqlDerived?.derived?.Temp_Spread?.current?.toFixed(1) || '--')}
                </span>
                <span className="text-2xl text-slate-500 ml-2">°C</span>
              </div>
              <div className="text-xs text-slate-500 mb-1">
                Berechnung: Max(Zone1-4) - Min(Zone1-4)
              </div>
              <div className="text-xs text-slate-500 mb-2">
                Referenz: {machineState === 'PRODUCTION' ? '≤5°C optimal, ≤8°C akzeptabel, >8°C kritisch' : 'Prozessbewertung nur in PRODUCTION Zustand'}
              </div>
              <div className="text-xs text-slate-600">
                {machineState === 'PRODUCTION' ? (
                  <>
                    {(currentDashboardData?.spread_status === 'green' || (currentDashboardData?.metrics?.Temp_Spread?.current_value || 0) <= 5) && 
                      "🟢 Homogene Temperaturverteilung. Saubere und gleichmäßige Plastifizierung."}
                    {(currentDashboardData?.spread_status === 'orange' || ((currentDashboardData?.metrics?.Temp_Spread?.current_value || 0) > 5 && (currentDashboardData?.metrics?.Temp_Spread?.current_value || 0) <= 8)) && 
                      "🟠 Temperaturzonen beginnen zu divergieren. Mögliche Heiz- oder Regelabweichungen."}
                    {(currentDashboardData?.spread_status === 'red' || (currentDashboardData?.metrics?.Temp_Spread?.current_value || 0) > 8) && 
                      "🔴 Starke Temperaturspreizung. Hohe Wahrscheinlichkeit für Prozessinstabilität, Sensor- oder Heizprobleme."}
                    {currentDashboardData?.spread_status === 'unknown' && 
                      "⏳ Bewertung wird berechnet..."}
                  </>
                ) : (
                  `⏸️ Maschine im ${machineState} Zustand - keine Prozessbewertung`
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Sensor Charts Section - UI Contract Implementation */}
        {/* Moved below Temperaturzonen (Zone 1–4) as per UI requirement */}

        <div className="mb-8">
          <h2 className="text-xl text-slate-900 mb-4">
            Temperaturzonen (Zone 1–4)
          </h2>
          <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
            {['Zone1_C', 'Zone2_C', 'Zone3_C', 'Zone4_C'].map((zone, index) => (
              <div key={zone} className="bg-white/95 backdrop-blur-sm rounded-2xl p-5 border border-slate-200/80 shadow-lg hover:shadow-xl transition-all duration-300 hover:scale-[1.02]">
                <div className="text-sm text-slate-600 mb-2">Zone {index + 1}</div>
                <div className="text-5xl mb-3">
                  <span className={
                    machineState === 'PRODUCTION' ? 
                    (mssqlDerived?.risk?.sensors[`Temp_${zone}`] === 'red' ? 'text-rose-600' :
                     mssqlDerived?.risk?.sensors[`Temp_${zone}`] === 'yellow' ? 'text-amber-600' :
                     mssqlDerived?.risk?.sensors[`Temp_${zone}`] === 'green' ? 'text-emerald-600' :
                     'text-slate-400') :
                    'text-slate-400'  // Neutral color when not in production
                  }>
                    {mssqlRows?.[0]?.[`Temp_${zone}`] ? parseFloat(mssqlRows[0][`Temp_${zone}`]).toFixed(1) : '--'}
                  </span>
                  <span className="text-3xl text-slate-500 ml-2">°C</span>
                </div>
                <div className="text-xs text-slate-500 mb-1">
                  Berechnung: Direkte Messung von Temperatursensor Zone {index + 1}
                </div>
                <div className="text-xs text-slate-600">
                  {machineState === 'PRODUCTION' ? (
                    <>
                      {mssqlDerived?.risk?.sensors[`Temp_${zone}`] === 'green' && 
                        "🟢 Temperaturzone im materialgerechten Bereich. Saubere Erwärmung ohne Auffälligkeiten."}
                      {mssqlDerived?.risk?.sensors[`Temp_${zone}`] === 'yellow' && 
                        "🟠 Temperaturabweichung festgestellt. Mögliche Änderungen im Heizverhalten oder Materialfluss."}
                      {mssqlDerived?.risk?.sensors[`Temp_${zone}`] === 'red' && 
                        "🔴 Kritische Temperaturabweichung. Risiko für unvollständige Plastifizierung, Materialabbau oder Qualitätsprobleme."}
                    </>
                  ) : (
                    `⏸️ Maschine im ${machineState} Zustand - keine Prozessbewertung`
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>

        {(() => {
          // Data is live from TimescaleDB. For 1m use daily-aggregated API (last 30 days, incl. previous month). For All use full daily. Else raw for 1h/1d/1w.
          const sourceRows: any[] = (() => {
            const getTime = (r: any) => {
              const t = r?.TrendDate ?? r?.trend_date;
              if (!t) return 0;
              return new Date(t).getTime();
            };
            if (sensorHistoryDailyRows && sensorHistoryDailyRows.length > 0) {
              const sortedDaily = [...sensorHistoryDailyRows].sort((a, b) => getTime(a) - getTime(b));
              if (chartTimeframe === 'all') {
                return sortedDaily;
              }
              if (chartTimeframe === '1m') {
                // Last 30 days (up to 30 points); can include previous month
                return sortedDaily.slice(-30);
              }
            }
            // For 1d use dedicated 24h fetch so the chart has full last 24h of real data
            if (chartTimeframe === '1d' && sensorHistoryRows24h && sensorHistoryRows24h.length > 0) {
              return [...sensorHistoryRows24h].sort((a, b) => getTime(a) - getTime(b));
            }
            const raw =
              (sensorHistoryRows && sensorHistoryRows.length > 0)
                ? sensorHistoryRows
                : ((mssqlDerived as any)?.rows && Array.isArray((mssqlDerived as any).rows) && (mssqlDerived as any).rows.length > 0
                    ? (mssqlDerived as any).rows
                    : (mssqlRows || []));
            return [...raw].sort((a, b) => getTime(a) - getTime(b));
          })();

          // Prepare historical data: keep all valid numbers (include 0) so charts display like Temp_Spread
          const validPoint = (d: { value: number }) =>
            typeof d.value === 'number' && !isNaN(d.value);

          // Robust read: support both API shapes (TrendDate/Temp_Zone1_C and trend_date/temp_zone1_c)
          const num = (row: any, ...keys: string[]) => {
            for (const k of keys) {
              const v = row?.[k];
              if (v !== undefined && v !== null && v !== '') {
                const n = parseFloat(v);
                if (!isNaN(n)) return n;
              }
            }
            return 0;
          };

          // Parse timestamp consistently: naive ISO (no Z) as UTC so day boundaries match backend
          const parseTimestamp = (ts: string | Date): Date => {
            if (typeof ts !== 'string') return ts;
            let iso = String(ts).trim();
            if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?$/.test(iso)) iso += 'Z';
            return new Date(iso);
          };

          // UTC date string YYYY-MM-DD for grouping and comparison
          const toUtcDateKey = (d: Date) => {
            const y = d.getUTCFullYear();
            const m = (d.getUTCMonth() + 1).toString().padStart(2, '0');
            const day = d.getUTCDate().toString().padStart(2, '0');
            return `${y}-${m}-${day}`;
          };

          // For longer ranges (1w, 1m, all) we want a single point per day = daily maximum (UTC day).
          const aggregateDailyMax = (series: { timestamp: string | Date; value: number }[]) => {
            if (chartTimeframe === '1h' || chartTimeframe === '1d') {
              return series;
            }
            const buckets = new Map<string, { timestamp: Date; value: number }>();
            for (const p of series) {
              const d = parseTimestamp(p.timestamp);
              if (Number.isNaN(d.getTime())) continue;
              const dayKey = toUtcDateKey(d);
              const y = d.getUTCFullYear();
              const month = d.getUTCMonth();
              const day = d.getUTCDate();
              const bucketTs = new Date(Date.UTC(y, month, day, 12, 0, 0));
              const existing = buckets.get(dayKey);
              if (!existing || p.value > existing.value) {
                buckets.set(dayKey, { timestamp: bucketTs, value: p.value });
              }
            }
            return Array.from(buckets.values()).sort(
              (a, b) => a.timestamp.getTime() - b.timestamp.getTime()
            );
          };
          const screwSpeedHistorical = sourceRows.map((row: any, index: number) => ({
            timestamp: row.TrendDate ?? row.trend_date ?? new Date(Date.now() - ((sourceRows.length || 0) - index) * 60000),
            value: num(row, 'ScrewSpeed_rpm', 'screw_rpm'),
          })).filter(validPoint);

          // Prepare historical data for Pressure_bar
          const pressureHistorical = sourceRows.map((row: any, index: number) => ({
            timestamp: row.TrendDate ?? row.trend_date ?? new Date(Date.now() - ((sourceRows.length || 0) - index) * 60000),
            value: num(row, 'Pressure_bar', 'pressure_bar'),
          })).filter(validPoint);

          // Prepare historical data for Temp_Avg
          const tempAvgHistorical = sourceRows.map((row: any, index: number) => {
            const t1 = num(row, 'Temp_Zone1_C', 'temp_zone1_c');
            const t2 = num(row, 'Temp_Zone2_C', 'temp_zone2_c');
            const t3 = num(row, 'Temp_Zone3_C', 'temp_zone3_c');
            const t4 = num(row, 'Temp_Zone4_C', 'temp_zone4_c');
            const vals = [t1, t2, t3, t4].filter((t) => !isNaN(t));
            const avg = vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : 0;
            return {
              timestamp: row.TrendDate ?? row.trend_date ?? new Date(Date.now() - ((sourceRows.length || 0) - index) * 60000),
              value: avg,
            };
          }).filter(validPoint);

          // Prepare historical data for Temp_Spread (no baseline band, fixed thresholds)
          const tempSpreadHistorical = sourceRows.map((row: any, index: number) => {
            const t1 = num(row, 'Temp_Zone1_C', 'temp_zone1_c');
            const t2 = num(row, 'Temp_Zone2_C', 'temp_zone2_c');
            const t3 = num(row, 'Temp_Zone3_C', 'temp_zone3_c');
            const t4 = num(row, 'Temp_Zone4_C', 'temp_zone4_c');
            const temps = [t1, t2, t3, t4].filter((t) => !isNaN(t));
            const spread = temps.length >= 2 ? Math.max(...temps) - Math.min(...temps) : 0;
            return {
              timestamp: row.TrendDate ?? row.trend_date ?? new Date(Date.now() - ((sourceRows.length || 0) - index) * 60000),
              value: spread,
            };
          }).filter(validPoint);

          const screwSpeedAggregated = aggregateDailyMax(screwSpeedHistorical);
          const pressureAggregated = aggregateDailyMax(pressureHistorical);
          const tempAvgAggregated = aggregateDailyMax(tempAvgHistorical);
          const tempSpreadAggregated = aggregateDailyMax(tempSpreadHistorical);

          // Timeframe filter for charts (1h, 1d, 1w, 1m, all).
          // Use the latest data timestamp as reference so 1h/1d show the last hour/day of actual data.
          const latestSourceTs = (() => {
            if (!sourceRows.length) return new Date();
            const last = sourceRows[sourceRows.length - 1];
            const raw = last?.TrendDate ?? last?.trend_date;
            const d = raw ? parseTimestamp(raw) : new Date();
            return Number.isNaN(d.getTime()) ? new Date() : d;
          })();
          const latestDateUtc = new Date(Date.UTC(latestSourceTs.getUTCFullYear(), latestSourceTs.getUTCMonth(), latestSourceTs.getUTCDate()));
          const oneDayMs = 24 * 60 * 60 * 1000;
          // For 1w/1m/all, filter aggregated (daily) points: only days in range; only days that have data (no dummy points).
          // 1m = last 30 days (up to 30 points, one per day that has data). New/recent data from TimescaleDB appears as we refetch.
          const withinTimeframeForDaily = (ts: string | Date | undefined | null) => {
            if (!ts) return false;
            if (chartTimeframe === 'all') return true;
            const d = parseTimestamp(ts);
            if (Number.isNaN(d.getTime())) return false;
            const pointDateUtc = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
            const pointMs = pointDateUtc.getTime();
            const latestMs = latestDateUtc.getTime();
            if (pointMs > latestMs) return false; // no future dates
            if (chartTimeframe === '1w') return pointMs >= latestMs - 6 * oneDayMs; // last 7 days
            if (chartTimeframe === '1m') return pointMs >= latestMs - 29 * oneDayMs; // last 30 days — only days with data shown
            return true;
          };
          const timeframeMs =
            chartTimeframe === '1h'
              ? 60 * 60 * 1000
              : chartTimeframe === '1d'
              ? 24 * 60 * 60 * 1000
              : chartTimeframe === '1w'
              ? 7 * 24 * 60 * 60 * 1000
              : chartTimeframe === '1m'
              ? 30 * 24 * 60 * 60 * 1000
              : Number.MAX_SAFE_INTEGER; // all

          const withinTimeframe = (ts: string | Date | undefined | null) => {
            if (!ts) return false;
            if (chartTimeframe === 'all') return true; // show everything we have
            const d = parseTimestamp(ts);
            return latestSourceTs.getTime() - d.getTime() <= timeframeMs;
          };

          // For 1d: resample to one point per 30 minutes (max per bucket). Only buckets that have data — no zero-fill.
          const THIRTY_MINS_MS = 30 * 60 * 1000;
          const ONE_HOUR_MS = 60 * 60 * 1000;
          const resampleTo30Min = (series: { timestamp: string | Date; value: number }[]) => {
            const buckets = new Map<number, { timestamp: Date; value: number }>();
            for (const p of series) {
              const d = parseTimestamp(p.timestamp);
              if (Number.isNaN(d.getTime())) continue;
              const bucketKey = Math.floor(d.getTime() / THIRTY_MINS_MS) * THIRTY_MINS_MS;
              const bucketTs = new Date(bucketKey + THIRTY_MINS_MS / 2); // bucket center for label
              const existing = buckets.get(bucketKey);
              if (!existing || p.value > existing.value) {
                buckets.set(bucketKey, { timestamp: bucketTs, value: p.value });
              }
            }
            return Array.from(buckets.values()).sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
          };

          // For 1d: exactly 24 points — one per hour (last 24h), each = max value in that hour. Missing hours = linear interpolation between known values (no flat repeats, no zeros).
          const resampleTo24HoursHourlyMax = (series: { timestamp: string | Date; value: number }[]) => {
            const windowEndMs = latestSourceTs.getTime();
            const windowStartMs = windowEndMs - 24 * ONE_HOUR_MS;
            const hourToValue = new Map<number, number>();
            for (const p of series) {
              const d = parseTimestamp(p.timestamp);
              if (Number.isNaN(d.getTime())) continue;
              const t = d.getTime();
              if (t < windowStartMs || t >= windowEndMs) continue;
              const hourKey = Math.floor((t - windowStartMs) / ONE_HOUR_MS) * ONE_HOUR_MS + windowStartMs;
              if (hourKey < windowStartMs || hourKey >= windowEndMs) continue;
              const existing = hourToValue.get(hourKey);
              if (existing === undefined || p.value > existing) hourToValue.set(hourKey, p.value);
            }
            const hourIndices = Array.from({ length: 24 }, (_, i) => windowStartMs + i * ONE_HOUR_MS);
            const knownHours = hourIndices.filter((h) => hourToValue.has(h));
            const out: { timestamp: Date; value: number }[] = [];
            for (let i = 0; i < 24; i++) {
              const hourStartMs = windowStartMs + i * ONE_HOUR_MS;
              const ts = new Date(hourStartMs + ONE_HOUR_MS / 2);
              let value: number;
              if (hourToValue.has(hourStartMs)) {
                value = hourToValue.get(hourStartMs)!;
              } else {
                const prevKnown = knownHours.filter((h) => h < hourStartMs).pop();
                const nextKnown = knownHours.find((h) => h > hourStartMs);
                const vPrev = prevKnown !== undefined ? hourToValue.get(prevKnown)! : (nextKnown !== undefined ? hourToValue.get(nextKnown)! : 0);
                const vNext = nextKnown !== undefined ? hourToValue.get(nextKnown)! : vPrev;
                if (prevKnown !== undefined && nextKnown !== undefined) {
                  const frac = (hourStartMs - prevKnown) / (nextKnown - prevKnown);
                  value = Math.round((vPrev + frac * (vNext - vPrev)) * 100) / 100;
                } else {
                  value = vPrev;
                }
              }
              out.push({ timestamp: ts, value });
            }
            return out.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
          };

          // For 1w: exactly 7 points — one per calendar day (last 7 days), each = daily max. Missing days = linear interpolation.
          const resampleTo7DaysDailyMax = (series: { timestamp: string | Date; value: number }[]) => {
            const latestMs = latestDateUtc.getTime();
            const dayToValue = new Map<number, number>();
            for (const p of series) {
              const d = parseTimestamp(p.timestamp);
              if (Number.isNaN(d.getTime())) continue;
              const dayStartMs = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
              if (dayStartMs < latestMs - 6 * oneDayMs || dayStartMs > latestMs) continue;
              const existing = dayToValue.get(dayStartMs);
              if (existing === undefined || p.value > existing) dayToValue.set(dayStartMs, p.value);
            }
            const dayIndices = Array.from({ length: 7 }, (_, i) => latestMs - (6 - i) * oneDayMs);
            const knownDays = dayIndices.filter((ms) => dayToValue.has(ms));
            const knownValues = knownDays.map((ms) => dayToValue.get(ms) as number).filter((v) => typeof v === 'number' && !Number.isNaN(v));
            const meanKnown = knownValues.length > 0 ? knownValues.reduce((a, b) => a + b, 0) / knownValues.length : 0;
            const out: { timestamp: Date; value: number }[] = [];
            for (let i = 0; i < 7; i++) {
              const dayStartMs = latestMs - (6 - i) * oneDayMs;
              const y = new Date(dayStartMs).getUTCFullYear();
              const m = new Date(dayStartMs).getUTCMonth();
              const day = new Date(dayStartMs).getUTCDate();
              const ts = new Date(Date.UTC(y, m, day, 12, 0, 0));
              let value: number;
              if (dayToValue.has(dayStartMs)) {
                value = dayToValue.get(dayStartMs)!;
              } else {
                const prevKnown = knownDays.filter((ms) => ms < dayStartMs).pop();
                const nextKnown = knownDays.find((ms) => ms > dayStartMs);
                const vPrev = prevKnown !== undefined ? dayToValue.get(prevKnown)! : (nextKnown !== undefined ? dayToValue.get(nextKnown)! : meanKnown);
                const vNext = nextKnown !== undefined ? dayToValue.get(nextKnown)! : vPrev;
                if (prevKnown !== undefined && nextKnown !== undefined) {
                  const frac = (dayStartMs - prevKnown) / (nextKnown - prevKnown);
                  value = Math.round((vPrev + frac * (vNext - vPrev)) * 100) / 100;
                } else {
                  value = vPrev;
                }
              }
              out.push({ timestamp: ts, value });
            }
            return out.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
          };

          // Real data only: short range = raw points (1h) or 24×hourly max (1d); long range = daily aggregated or 7×daily max (1w).
          const isShortRange = chartTimeframe === '1h' || chartTimeframe === '1d';
          const screwSpeedShort = screwSpeedHistorical.filter((p) => withinTimeframe(p.timestamp));
          const pressureShort = pressureHistorical.filter((p) => withinTimeframe(p.timestamp));
          const tempAvgShort = tempAvgHistorical.filter((p) => withinTimeframe(p.timestamp));
          const tempSpreadShort = tempSpreadHistorical.filter((p) => withinTimeframe(p.timestamp));

          // 1d: exactly 24 points (one per hour, max per hour). 1w: exactly 7 points (one per day, daily max). 1h/1m/all: as before.
          const screwSpeedDisplay = isShortRange
            ? (chartTimeframe === '1d' ? resampleTo24HoursHourlyMax(screwSpeedShort) : screwSpeedShort)
            : chartTimeframe === '1w'
            ? resampleTo7DaysDailyMax(screwSpeedAggregated.filter((p) => withinTimeframeForDaily(p.timestamp)))
            : screwSpeedAggregated.filter((p) => withinTimeframeForDaily(p.timestamp));
          const pressureDisplay = isShortRange
            ? (chartTimeframe === '1d' ? resampleTo24HoursHourlyMax(pressureShort) : pressureShort)
            : chartTimeframe === '1w'
            ? resampleTo7DaysDailyMax(
                // Treat pressure < 10 as missing for 1w so we still show 7 days (interpolated),
                // but we don't plot meaningless low-pressure points.
                pressureAggregated
                  .filter((p) => withinTimeframeForDaily(p.timestamp))
                  .filter((p) => p.value >= 10)
              )
            : pressureAggregated.filter((p) => withinTimeframeForDaily(p.timestamp));

          // For 1m and All: exclude pressure points with value < 10 (only show meaningful pressure).
          // For 1w we already removed <10 before interpolation so we keep all 7 days.
          const pressureDisplayForChart =
            chartTimeframe === '1m' || chartTimeframe === 'all'
              ? pressureDisplay.filter((p) => p.value >= 10)
              : pressureDisplay;
          const tempAvgDisplay = isShortRange
            ? (chartTimeframe === '1d' ? resampleTo24HoursHourlyMax(tempAvgShort) : tempAvgShort)
            : chartTimeframe === '1w'
            ? resampleTo7DaysDailyMax(tempAvgAggregated.filter((p) => withinTimeframeForDaily(p.timestamp)))
            : tempAvgAggregated.filter((p) => withinTimeframeForDaily(p.timestamp));
          const tempSpreadDisplay = isShortRange
            ? (chartTimeframe === '1d' ? resampleTo24HoursHourlyMax(tempSpreadShort) : tempSpreadShort)
            : chartTimeframe === '1w'
            ? resampleTo7DaysDailyMax(tempSpreadAggregated.filter((p) => withinTimeframeForDaily(p.timestamp)))
            : tempSpreadAggregated.filter((p) => withinTimeframeForDaily(p.timestamp));

          // Latest values for line color and KPI (from real data only)
          const latestSpread = tempSpreadDisplay.length > 0 ? tempSpreadDisplay[tempSpreadDisplay.length - 1].value : null;
          let tempSpreadColor = '#94a3b8';
          if (machineState === 'PRODUCTION' && latestSpread !== null) {
            tempSpreadColor = '#10b981';
            if (latestSpread > 8) tempSpreadColor = '#ef4444';
            else if (latestSpread > 5) tempSpreadColor = '#f59e0b';
          }
          const tempColor = (v: number | null) => {
            if (v === null || v === undefined) return '#94a3b8';
            if (v >= 170 && v <= 180) return '#10b981';
            if (v > 180 && v <= 185) return '#f59e0b';
            if (v > 185) return '#ef4444';
            return '#94a3b8';
          };
          const lastTempAvg = tempAvgDisplay.length > 0 ? tempAvgDisplay[tempAvgDisplay.length - 1].value : null;
          const pressureColor = (v: number | null) => {
            if (v === null || v === undefined) return '#94a3b8';
            const coolFrom = pressureConfig?.ranges?.cool?.from ?? 330;
            const coolTo = pressureConfig?.ranges?.cool?.to ?? 360;
            const mediumFrom = pressureConfig?.ranges?.medium?.from ?? 360;
            const mediumTo = pressureConfig?.ranges?.medium?.to ?? 380;
            const hotFrom = pressureConfig?.ranges?.hot?.from ?? 380;
            const hotTo = pressureConfig?.ranges?.hot?.to ?? 395;
            const criticalFrom = pressureConfig?.ranges?.critical?.from ?? 395;
            const criticalTo = pressureConfig?.ranges?.critical?.to ?? 410;

            if (v < coolFrom) return '#14b8a6';
            if (v >= coolFrom && v < coolTo) return '#14b8a6';
            if (v >= mediumFrom && v < mediumTo) return '#10b981';
            if (v >= hotFrom && v < hotTo) return '#eab308';
            if (v >= criticalFrom && v <= criticalTo) return '#ef4444';
            if (v > criticalTo) return '#ef4444';
            return '#94a3b8';
          };
          const pressureLegendRanges = {
            cool: { from: pressureConfig?.ranges?.cool?.from ?? 330, to: pressureConfig?.ranges?.cool?.to ?? 360 },
            medium: { from: pressureConfig?.ranges?.medium?.from ?? 360, to: pressureConfig?.ranges?.medium?.to ?? 380 },
            hot: { from: pressureConfig?.ranges?.hot?.from ?? 380, to: pressureConfig?.ranges?.hot?.to ?? 395 },
            critical: { from: pressureConfig?.ranges?.critical?.from ?? 395, to: pressureConfig?.ranges?.critical?.to ?? 410 },
          };
          const pressureLegendText = `Bewertung ohne Baseline: ${pressureLegendRanges.cool.from}–${pressureLegendRanges.cool.to} 🔵, ${pressureLegendRanges.medium.from}–${pressureLegendRanges.medium.to} 🟢, ${pressureLegendRanges.hot.from}–${pressureLegendRanges.hot.to} 🟡, ${pressureLegendRanges.critical.from}–${pressureLegendRanges.critical.to} 🔴`;
          const lastPressure = pressureDisplay.length > 0 ? pressureDisplay[pressureDisplay.length - 1].value : null;
          const lastPressureForChart = pressureDisplayForChart.length > 0 ? pressureDisplayForChart[pressureDisplayForChart.length - 1].value : null;
          const screwColor = (v: number | null) => {
            if (v === null || v === undefined) return '#94a3b8';
            if (v >= 8 && v <= 14) return '#10b981';
            if (v > 14 && v <= 18) return '#f59e0b';
            if (v > 18) return '#ef4444';
            return '#94a3b8';
          };
          const lastScrew = screwSpeedDisplay.length > 0 ? screwSpeedDisplay[screwSpeedDisplay.length - 1].value : null;

          return (
            <div className="mb-8">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl text-slate-900">
                  Sensor Charts (Baseline Comparison)
                </h2>
                <div className="flex gap-2 text-xs">
                  {['1h', '1d', '1w', '1m', 'all'].map((tf) => (
                    <button
                      key={tf}
                      onClick={() => setChartTimeframe(tf as '1h' | '1d' | '1w' | '1m' | 'all')}
                      className={`px-2 py-1 rounded-full border ${
                        chartTimeframe === tf
                          ? 'bg-slate-900 text-white border-slate-900'
                          : 'bg-white text-slate-700 border-slate-300 hover:bg-slate-100'
                      }`}
                    >
                      {tf === 'all' ? 'All' : tf}
                    </button>
                  ))}
                </div>
              </div>
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* 1. Schneckendrehzahl (ScrewSpeed_rpm) */}
                <SimpleLiveChart
                  key="ScrewSpeed_rpm"
                  title="Schneckendrehzahl (ScrewSpeed_rpm)"
                  legend="Bewertung ohne Baseline: 8–14 rpm 🟢, 14–18 rpm 🟠, >18 rpm 🔴"
                  data={screwSpeedDisplay}
                  unit="rpm"
                  lineColor={machineState === 'PRODUCTION' ? screwColor(lastScrew) : '#10b981'}
                  height={300}
                  timeFormat={chartTimeframe === 'all' || chartTimeframe === '1w' || chartTimeframe === '1m' ? 'datetime' : 'time'}
                />

                {/* 2. Schmelzedruck (Pressure_bar) */}
                <div className="relative">
                  {currentDashboardData?.profile_id && (
                    <button
                      type="button"
                      onClick={() => {
                        setPressureConfigForm(pressureConfig ? {
                            cool_from: pressureConfig.ranges?.cool?.from ?? 330,
                            cool_to: pressureConfig.ranges?.cool?.to ?? 360,
                            medium_from: pressureConfig.ranges?.medium?.from ?? 360,
                            medium_to: pressureConfig.ranges?.medium?.to ?? 380,
                            hot_from: pressureConfig.ranges?.hot?.from ?? 380,
                            hot_to: pressureConfig.ranges?.hot?.to ?? 395,
                            critical_from: pressureConfig.ranges?.critical?.from ?? 395,
                            critical_to: pressureConfig.ranges?.critical?.to ?? 410,
                            warning_threshold: pressureConfig.thresholds?.warning ?? 380,
                            critical_warning_threshold: pressureConfig.thresholds?.critical ?? 395,
                            low_pressure_warning_threshold: pressureConfig.thresholds?.low ?? 340,
                            send_email_on_warning: pressureConfig.emails?.on_warning ?? true,
                            send_email_on_critical: pressureConfig.emails?.on_critical ?? true,
                            send_email_on_production_start: pressureConfig.emails?.on_production_start ?? false,
                            send_email_on_production_stop: pressureConfig.emails?.on_production_stop ?? false,
                          } : {
                            cool_from: 330, cool_to: 360,
                            medium_from: 360, medium_to: 380,
                            hot_from: 380, hot_to: 395,
                            critical_from: 395, critical_to: 410,
                            warning_threshold: 380, critical_warning_threshold: 395, low_pressure_warning_threshold: 340,
                            send_email_on_warning: true, send_email_on_critical: true,
                            send_email_on_production_start: false, send_email_on_production_stop: false,
                          });
                        setPressureConfigError(null);
                        setShowPressureConfigModal(true);
                      }}
                      className="absolute top-2 right-2 z-10 text-xs px-2 py-1 rounded border border-slate-300 bg-white text-slate-600 hover:bg-slate-100"
                    >
                      Configure ranges & alerts
                    </button>
                  )}
                <SimpleLiveChart
                  key="Pressure_bar"
                  title="Schmelzedruck (Pressure_bar)"
                  legend={
                    chartTimeframe === '1w' || chartTimeframe === '1m' || chartTimeframe === 'all'
                      ? `${pressureLegendText} — Tagesmaximum (Daily max). Druck < 10 bar nicht angezeigt.`
                      : pressureLegendText
                  }
                  data={pressureDisplayForChart}
                  unit="bar"
                  lineColor={
                    machineState === 'PRODUCTION'
                      ? pressureColor(lastPressureForChart)
                      : '#10b981'
                  }
                  height={300}
                  timeFormat={chartTimeframe === 'all' || chartTimeframe === '1w' || chartTimeframe === '1m' ? 'datetime' : 'time'}
                  bands={[
                    { from: Number.NEGATIVE_INFINITY, to: (pressureConfig?.ranges?.cool?.from ?? 330), color: '#99f6e4', opacity: 0.6 },
                    { from: (pressureConfig?.ranges?.critical?.to ?? 410), to: Number.POSITIVE_INFINITY, color: '#fecaca', opacity: 0.6 },
                    { from: (pressureConfig?.ranges?.cool?.from ?? 330), to: (pressureConfig?.ranges?.cool?.to ?? 360), color: '#99f6e4', opacity: 0.6 },
                    { from: (pressureConfig?.ranges?.medium?.from ?? 360), to: (pressureConfig?.ranges?.medium?.to ?? 380), color: '#bbf7d0', opacity: 0.6 },
                    { from: (pressureConfig?.ranges?.hot?.from ?? 380), to: (pressureConfig?.ranges?.hot?.to ?? 395), color: '#fef08a', opacity: 0.6 },
                    { from: (pressureConfig?.ranges?.critical?.from ?? 395), to: (pressureConfig?.ranges?.critical?.to ?? 410), color: '#fecaca', opacity: 0.6 },
                  ]}
                />
                </div>

                {/* 3–6. Temperature Zones */}
                {['Zone1_C', 'Zone2_C', 'Zone3_C', 'Zone4_C'].map((zone, index) => {
                  const zoneKey = `Temp_${zone}`;
                  const altKey = `temp_zone${index + 1}_c`;
                  const zoneSeries = sourceRows
                    .map((row: any, idx: number) => ({
                      timestamp: row.TrendDate ?? row.trend_date ?? new Date(Date.now() - ((sourceRows.length || 0) - idx) * 60000),
                      value: num(row, zoneKey, altKey),
                    }))
                    .filter(validPoint);
                  const zoneAggregated = aggregateDailyMax(zoneSeries);
                  const zoneFiltered = zoneSeries.filter((p) => withinTimeframe(p.timestamp));
                  const zoneResampled = chartTimeframe === '1d' ? resampleTo30Min(zoneFiltered) : zoneFiltered;
                  const zoneFilteredDaily = zoneAggregated.filter((p) => withinTimeframeForDaily(p.timestamp));
                  const zoneDisplay = isShortRange
                    ? (chartTimeframe === '1d' ? resampleTo24HoursHourlyMax(zoneFiltered) : zoneFiltered)
                    : chartTimeframe === '1w'
                    ? resampleTo7DaysDailyMax(zoneFilteredDaily)
                    : zoneFilteredDaily;
                  const lastZone = zoneDisplay.length > 0 ? zoneDisplay[zoneDisplay.length - 1].value : null;
                  return (
                    <SimpleLiveChart
                      key={zoneKey}
                      title={`Temperatur Zone ${index + 1} (${zoneKey})`}
                      legend="Bewertung ohne Baseline: 170–180°C 🟢, 180–185°C 🟠, >185°C 🔴"
                      data={zoneDisplay}
                      unit="°C"
                      lineColor={machineState === 'PRODUCTION' ? tempColor(lastZone) : '#10b981'}
                      height={300}
                      timeFormat={chartTimeframe === 'all' || chartTimeframe === '1w' || chartTimeframe === '1m' ? 'datetime' : 'time'}
                    />
                  );
                })}

                {/* 7. Durchschnittstemperatur (Temp_Avg) */}
                <SimpleLiveChart
                  key="Temp_Avg"
                  title="Durchschnittstemperatur (Temp_Avg)"
                  legend="Bewertung ohne Baseline: 170–180°C 🟢, 180–185°C 🟠, >185°C 🔴"
                  data={tempAvgDisplay}
                  unit="°C"
                  lineColor={machineState === 'PRODUCTION' ? tempColor(lastTempAvg) : '#10b981'}
                  height={300}
                  timeFormat={chartTimeframe === 'all' || chartTimeframe === '1w' || chartTimeframe === '1m' ? 'datetime' : 'time'}
                />

                {/* 8. Temperaturspreizung (Temp_Spread) */}
                <SimpleLiveChart
                  key="Temp_Spread"
                  title="Temperaturspreizung (Temp_Spread)"
                  legend="Bewertung ohne Baseline: ≤5°C 🟢, 5–8°C 🟠, >8°C 🔴"
                  data={tempSpreadDisplay}
                  unit="°C"
                  lineColor={tempSpreadColor}
                  height={300}
                  timeFormat={chartTimeframe === 'all' || chartTimeframe === '1w' || chartTimeframe === '1m' ? 'datetime' : 'time'}
                />
              </div>
            </div>
          );
        })()}

        {/* Stabilität */}
        <div className="mb-8">
          <h2 className="text-xl text-slate-900 mb-4">
            Stabilität (Time Spread / Fluktuation)
          </h2>
          <div className="bg-white/95 backdrop-blur-sm rounded-2xl p-6 border border-slate-200/80 shadow-lg">
            <div className="text-xs text-slate-500 mb-4">
              Calculation: stability_ratio = window_std / baseline_std
              <br />
              <span className="ml-2">window_std = Standard Deviation over Sliding Window</span>
              <br />
              <span className="ml-2">baseline_std = Learned Basic Standard Deviation</span>
            </div>
            <div className="text-xs text-slate-500 mb-4">
              Referenz:
              <br />
              <span className="ml-2">🟢 Stable: stability_ratio ≤ 1.2</span>
              <br />
              <span className="ml-2">🟠 Fluctuating: 1.2 &lt; stability_ratio ≤ 1.6</span>
              <br />
              <span className="ml-2">🔴 Unstable: stability_ratio &gt; 1.6</span>
              <br />
              <span className="ml-2 italic text-slate-400">
                Tooltip: Increased fluctuation compared to baseline
              </span>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div>
                <div className="text-sm text-slate-500 mb-2">Prozessstabilität</div>
                <div className="text-2xl mb-2">
                  <span className={
                    machineState === 'PRODUCTION' ? 
                    (anomaliesCount > 2 ? 'text-rose-600' :
                     anomaliesCount > 0 ? 'text-amber-600' :
                     'text-emerald-600') :
                    'text-slate-400'  // Neutral color when not in production
                  }>
                    {anomaliesCount > 2 ? '🔴 Stark schwankend' :
                     anomaliesCount > 0 ? '🟠 Erhöhte Varianz' :
                     '🟢 Geringe Varianz'}
                  </span>
                </div>
                <div className="text-xs text-slate-600">
                  {machineState === 'PRODUCTION' ? (
                    <>
                      {anomaliesCount === 0 && 
                        "🟢 Prozess stabil. Keine ungewöhnlichen Schwankungen."}
                      {anomaliesCount > 0 && anomaliesCount <= 2 && 
                        "🟠 Erhöhte Prozessunruhe. Frühindikator für mögliche Abweichungen."}
                      {anomaliesCount > 2 && 
                        "🔴 Instabiler Prozess. Hohe Wahrscheinlichkeit für Qualitätsprobleme oder Störungen."}
                    </>
                  ) : (
                    `⏸️ Maschine im ${machineState} Zustand - keine Prozessbewertung`
                  )}
                </div>
              </div>
              <div>
                <div className="text-sm text-slate-500 mb-2">Anomalien in letzter Zeit</div>
                <div className="text-2xl mb-2">{anomaliesCount}</div>
                <div className="text-xs text-slate-600">
                  Anzahl der erkannten Abweichungen im Analysefenster
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Prozessbewertung */}
        <div className="mb-8">
          <h2 className="text-xl text-slate-900 mb-4">
            Prozessbewertung (Gesamttext)
          </h2>
          <div className="bg-white rounded-2xl p-6 border border-slate-200 shadow-md">
            <div className={`text-lg mb-4 px-4 py-3 rounded-lg ${
              machineState === 'PRODUCTION' ? (
                mssqlDerived?.risk?.overall === 'green' ? 'bg-emerald-100 text-emerald-800 border border-emerald-300' :
                mssqlDerived?.risk?.overall === 'yellow' ? 'bg-amber-100 text-amber-800 border border-amber-300' :
                mssqlDerived?.risk?.overall === 'red' ? 'bg-rose-100 text-rose-800 border border-rose-300' :
                'bg-slate-100 text-slate-800 border border-slate-300'
              ) : 'bg-slate-100 text-slate-800 border border-slate-300'
            }`}>
              {machineState === 'PRODUCTION' ? (
                <>
                  {mssqlDerived?.risk?.overall === 'green' && "🟢 GRÜNER PROZESSZUSTAND"}
                  {mssqlDerived?.risk?.overall === 'yellow' && "🟠 ORANGER PROZESSZUSTAND"}
                  {mssqlDerived?.risk?.overall === 'red' && "🔴 ROTER PROZESSZUSTAND"}
                </>
              ) : (
                `⏸️ Maschine im ${machineState} Zustand - keine Prozessbewertung`
              )}
            </div>
            <div className="text-slate-700 leading-relaxed">
              {machineState === 'PRODUCTION' ? (
                <>
                  {mssqlDerived?.risk?.overall === 'green' && 
                    "Der Extrusionsprozess ist stabil. Alle wesentlichen Parameter liegen im materialabhängigen Referenzbereich. Kein Handlungsbedarf."}
                  {mssqlDerived?.risk?.overall === 'yellow' && 
                    "Der Prozess zeigt Abweichungen vom optimalen Betriebszustand. Empfehlung: Überwachung verstärken und mögliche Ursachen prüfen."}
                  {mssqlDerived?.risk?.overall === 'red' && 
                    "Kritischer Prozesszustand. Hohes Risiko für Chargenverlust oder Anlagenbelastung. Eingriff empfohlen."}
                </>
              ) : (
                `⏸️ Maschine im ${machineState} Zustand - keine Prozessbewertung`
              )}
            </div>
          </div>
        </div>
        
        {/* Bottom Widgets - Removed */}
        
        {/* Live Data Table - Removed */}
        {/* OPC UA Status - Removed */}
        
        {/* Create Profile Modal */}
        {showCreateProfileModal && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <div className="bg-white rounded-xl shadow-2xl max-w-md w-full p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-xl text-slate-900">Create Profile</h3>
                <button
                  onClick={() => {
                    setShowCreateProfileModal(false);
                    setCreateProfileError(null);
                  }}
                  className="text-slate-400 hover:text-slate-600 transition-colors"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
              
              <div className="space-y-4">
                <div>
                  <label className="block text-sm text-slate-700 mb-2">Material ID</label>
                  <input
                    type="text"
                    value={selectedMaterial}
                    disabled
                    className="w-full px-3 py-2 border border-slate-300 rounded-md bg-slate-50 text-slate-600"
                  />
                  <p className="text-xs text-slate-500 mt-1">Profile will be created for this material</p>
                </div>
                
                <div>
                  <label className="block text-sm text-slate-700 mb-2">Version (Optional)</label>
                  <input
                    type="text"
                    id="profile-version"
                    defaultValue="1.0"
                    className="w-full px-3 py-2 border border-slate-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="1.0"
                  />
                </div>

                {createProfileError && (
                  <div className="px-3 py-2 bg-rose-50 border border-rose-200 rounded-md text-sm text-rose-700">
                    {createProfileError}
                  </div>
                )}

                <div className="flex gap-3 pt-2">
                  <button
                    onClick={async () => {
                      setIsCreatingProfile(true);
                      setCreateProfileError(null);
                      try {
                        // Get machine ID from current dashboard data or machines list
                        let machineId: string | null = null;
                        if (currentDashboardData?.machine_id) {
                          machineId = currentDashboardData.machine_id;
                        } else {
                          // Try to get from machines list
                          const machinesResult = await safeApi.get('/machines');
                          if (machinesResult.data && Array.isArray(machinesResult.data)) {
                            const machine = machinesResult.data.find((m: any) => 
                              m.name === selectedMachine || m.id === selectedMachine
                            );
                            if (machine) {
                              machineId = machine.id;
                            }
                          }
                        }

                        const versionInput = document.getElementById('profile-version') as HTMLInputElement;
                        const version = versionInput?.value || "1.0";

                        const payload: any = {
                          material_id: selectedMaterial,
                          version: version,
                        };
                        
                        // Only include machine_id if we found one (otherwise create material default profile)
                        if (machineId) {
                          payload.machine_id = machineId;
                        }

                        await safeApi.post('/profiles', payload);
                        
                        // Close modal and refresh data
                        setShowCreateProfileModal(false);
                        setCreateProfileError(null);
                        // Trigger data refresh by reloading page
                        window.location.reload();
                      } catch (error: any) {
                        console.error('Failed to create profile:', error);
                        setCreateProfileError(
                          error?.response?.data?.detail || 
                          error?.message || 
                          'Failed to create profile. Please try again.'
                        );
                      } finally {
                        setIsCreatingProfile(false);
                      }
                    }}
                    disabled={isCreatingProfile}
                    className="flex-1 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-md transition-colors duration-200"
                  >
                    {isCreatingProfile ? 'Creating...' : 'Create Profile'}
                  </button>
                  <button
                    onClick={() => {
                      setShowCreateProfileModal(false);
                      setCreateProfileError(null);
                    }}
                    disabled={isCreatingProfile}
                    className="px-4 py-2 bg-slate-200 hover:bg-slate-300 disabled:bg-slate-100 text-slate-700 rounded-md transition-colors duration-200"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}
        {/* Pressure Config Modal */}
        {showPressureConfigModal && currentDashboardData?.profile_id && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4 overflow-y-auto">
            <div className="bg-white rounded-xl shadow-2xl max-w-lg w-full my-8 p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-xl text-slate-900">Melt pressure ranges & alerts</h3>
                <button
                  type="button"
                  onClick={() => {
                    setShowPressureConfigModal(false);
                    setPressureConfigError(null);
                  }}
                  className="text-slate-400 hover:text-slate-600 transition-colors"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>

              <div className="space-y-6">
                <div>
                  <h4 className="text-sm font-medium text-slate-700 mb-3">Ranges (bar)</h4>
                  <div className="grid grid-cols-2 gap-3 text-sm">
                    {(['cool', 'medium', 'hot', 'critical'] as const).map((band) => (
                      <div key={band} className="flex items-center gap-2">
                        <span className="capitalize w-16 text-slate-600">{band}</span>
                        <input
                          type="number"
                          step={1}
                          value={pressureConfigForm[`${band}_from` as keyof typeof pressureConfigForm]}
                          onChange={(e) => setPressureConfigForm((prev) => ({ ...prev, [`${band}_from`]: Number(e.target.value) }))}
                          className="w-20 px-2 py-1 border border-slate-300 rounded"
                        />
                        <span className="text-slate-400">–</span>
                        <input
                          type="number"
                          step={1}
                          value={pressureConfigForm[`${band}_to` as keyof typeof pressureConfigForm]}
                          onChange={(e) => setPressureConfigForm((prev) => ({ ...prev, [`${band}_to`]: Number(e.target.value) }))}
                          className="w-20 px-2 py-1 border border-slate-300 rounded"
                        />
                      </div>
                    ))}
                  </div>
                </div>

                <div>
                  <h4 className="text-sm font-medium text-slate-700 mb-3">Alert thresholds (bar)</h4>
                  <div className="grid grid-cols-1 gap-2 text-sm">
                    <div className="flex items-center gap-2">
                      <label className="w-40 text-slate-600">Warning</label>
                      <input
                        type="number"
                        step={1}
                        value={pressureConfigForm.warning_threshold}
                        onChange={(e) => setPressureConfigForm((prev) => ({ ...prev, warning_threshold: Number(e.target.value) }))}
                        className="w-24 px-2 py-1 border border-slate-300 rounded"
                      />
                    </div>
                    <div className="flex items-center gap-2">
                      <label className="w-40 text-slate-600">Critical warning</label>
                      <input
                        type="number"
                        step={1}
                        value={pressureConfigForm.critical_warning_threshold}
                        onChange={(e) => setPressureConfigForm((prev) => ({ ...prev, critical_warning_threshold: Number(e.target.value) }))}
                        className="w-24 px-2 py-1 border border-slate-300 rounded"
                      />
                    </div>
                    <div className="flex items-center gap-2">
                      <label className="w-40 text-slate-600">Low pressure warning</label>
                      <input
                        type="number"
                        step={1}
                        value={pressureConfigForm.low_pressure_warning_threshold}
                        onChange={(e) => setPressureConfigForm((prev) => ({ ...prev, low_pressure_warning_threshold: Number(e.target.value) }))}
                        className="w-24 px-2 py-1 border border-slate-300 rounded"
                      />
                    </div>
                  </div>
                </div>

                <div>
                  <h4 className="text-sm font-medium text-slate-700 mb-3">Email notifications</h4>
                  <div className="space-y-2">
                    {[
                      { key: 'send_email_on_warning', label: 'Send email on warning' },
                      { key: 'send_email_on_critical', label: 'Send email on critical' },
                      { key: 'send_email_on_production_start', label: 'Send email on production start' },
                      { key: 'send_email_on_production_stop', label: 'Send email on production stop' },
                    ].map(({ key, label }) => (
                      <label key={key} className="flex items-center gap-2 text-sm text-slate-700">
                        <input
                          type="checkbox"
                          checked={!!pressureConfigForm[key as keyof typeof pressureConfigForm]}
                          onChange={(e) => setPressureConfigForm((prev) => ({ ...prev, [key]: e.target.checked }))}
                          className="rounded border-slate-300"
                        />
                        {label}
                      </label>
                    ))}
                  </div>
                </div>

                {pressureConfigError && (
                  <div className="px-3 py-2 bg-rose-50 border border-rose-200 rounded-md text-sm text-rose-700">
                    {pressureConfigError}
                  </div>
                )}

                <div className="flex gap-3 pt-2">
                  <button
                    type="button"
                    onClick={async () => {
                      setIsSavingPressureConfig(true);
                      setPressureConfigError(null);
                      try {
                        const profileId = currentDashboardData?.profile_id;
                        if (!profileId) return;
                        await safeApi.put(`/profiles/${profileId}/pressure-config`, {
                          batch_id: null,
                          is_active: true,
                          cool_from: pressureConfigForm.cool_from,
                          cool_to: pressureConfigForm.cool_to,
                          medium_from: pressureConfigForm.medium_from,
                          medium_to: pressureConfigForm.medium_to,
                          hot_from: pressureConfigForm.hot_from,
                          hot_to: pressureConfigForm.hot_to,
                          critical_from: pressureConfigForm.critical_from,
                          critical_to: pressureConfigForm.critical_to,
                          warning_threshold: pressureConfigForm.warning_threshold,
                          critical_warning_threshold: pressureConfigForm.critical_warning_threshold,
                          low_pressure_warning_threshold: pressureConfigForm.low_pressure_warning_threshold,
                          send_email_on_warning: pressureConfigForm.send_email_on_warning,
                          send_email_on_critical: pressureConfigForm.send_email_on_critical,
                          send_email_on_production_start: pressureConfigForm.send_email_on_production_start,
                          send_email_on_production_stop: pressureConfigForm.send_email_on_production_stop,
                        });
                        setShowPressureConfigModal(false);
                        setPressureConfig({
                          ...pressureConfig,
                          ranges: {
                            cool: { from: pressureConfigForm.cool_from, to: pressureConfigForm.cool_to },
                            medium: { from: pressureConfigForm.medium_from, to: pressureConfigForm.medium_to },
                            hot: { from: pressureConfigForm.hot_from, to: pressureConfigForm.hot_to },
                            critical: { from: pressureConfigForm.critical_from, to: pressureConfigForm.critical_to },
                          },
                          thresholds: {
                            warning: pressureConfigForm.warning_threshold,
                            critical: pressureConfigForm.critical_warning_threshold,
                            low: pressureConfigForm.low_pressure_warning_threshold,
                          },
                          emails: {
                            on_warning: pressureConfigForm.send_email_on_warning,
                            on_critical: pressureConfigForm.send_email_on_critical,
                            on_production_start: pressureConfigForm.send_email_on_production_start,
                            on_production_stop: pressureConfigForm.send_email_on_production_stop,
                          },
                        });
                      } catch (error: any) {
                        setPressureConfigError(
                          error?.response?.data?.detail?.toString?.() ||
                          error?.message ||
                          'Failed to save pressure config.'
                        );
                      } finally {
                        setIsSavingPressureConfig(false);
                      }
                    }}
                    disabled={isSavingPressureConfig}
                    className="flex-1 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-md transition-colors duration-200"
                  >
                    {isSavingPressureConfig ? 'Saving...' : 'Save'}
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setShowPressureConfigModal(false);
                      setPressureConfigError(null);
                    }}
                    disabled={isSavingPressureConfig}
                    className="px-4 py-2 bg-slate-200 hover:bg-slate-300 disabled:bg-slate-100 text-slate-700 rounded-md transition-colors duration-200"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}