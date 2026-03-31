import React, { useMemo, useState, useEffect } from 'react';
import {
  LineChart,
  Line,
  ReferenceLine,
  ReferenceArea,
  XAxis,
  YAxis,
  ResponsiveContainer,
} from 'recharts';

interface SensorChartProps {
  /** Sensor/metric name */
  sensorName: string;
  /** Current value */
  currentValue: number | null | undefined;
  /** Baseline mean */
  baselineMean: number | null | undefined;
  /** Green band (min/max) */
  greenBand: { min: number; max: number } | null | undefined;
  /** Historical data points for live curve */
  historicalData: Array<{ timestamp: string | Date; value: number }>;
  /** Current severity: 0=green, 1=orange, 2=red, -1=unknown */
  severity: number | null | undefined;
  /** Deviation from baseline (absolute or percentage) */
  deviation: number | null | undefined;
  /** Baseline material ID */
  baselineMaterial: string | null | undefined;
  /** Baseline confidence (0.0 - 1.0) */
  baselineConfidence?: number | null | undefined;
  /** Plain-language explanation for the current status */
  explanation?: string | null | undefined;
  /** Stability state: "green" | "orange" | "red" | "unknown" */
  stability?: string | null | undefined;
  /** Material change events for vertical markers */
  materialChanges?: Array<{ material_id: string; timestamp: string }>;
  /** Unit for display */
  unit: string;
  /** Whether baseline is ready */
  baselineReady: boolean;
  /** Whether machine is in PRODUCTION state */
  isInProduction: boolean;
  /** Deviation vs. setup in percent for badge logic (e.g. +20) */
  setupDeviation?: number | null | undefined;
  /** Chart height */
  height?: number;
  /** Optional format for x-axis time label (e.g., 'time' to only show time, 'datetime' to show both) */
  timeFormat?: 'time' | 'datetime';
}

export const SensorChart: React.FC<SensorChartProps> = ({
  sensorName,
  currentValue,
  baselineMean,
  greenBand,
  historicalData,
  severity,
  deviation,
  baselineMaterial,
  baselineConfidence,
  explanation,
  stability,
  materialChanges = [],
  unit,
  baselineReady,
  isInProduction,
  setupDeviation,
  height = 300,
  timeFormat = 'datetime',
}) => {
  // Prepare chart data: segment-based coloring (green in baseline, red out) for smooth animated line
  const chartData = useMemo(() => {
    if (!historicalData || historicalData.length === 0) {
      return [];
    }

    return historicalData.map((point) => {
      let timestampDate: Date;
      if (typeof point.timestamp === 'string') {
        let iso = String(point.timestamp).trim();
        if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?$/.test(iso)) {
          iso += 'Z';
        }
        timestampDate = new Date(iso);
      } else {
        timestampDate = point.timestamp;
      }
      const valid = !Number.isNaN(timestampDate.getTime());
      const hasBaseline = baselineReady && !!greenBand;
      const inBaseline =
        hasBaseline &&
        point.value >= greenBand!.min &&
        point.value <= greenBand!.max;

      const datePart = valid
        ? timestampDate.toLocaleDateString('de-DE', {
            day: '2-digit',
            month: '2-digit',
            timeZone: 'Europe/Vienna',
          })
        : '–';
      const timePart = valid
        ? timestampDate.toLocaleTimeString('de-DE', {
            hour: '2-digit',
            minute: '2-digit',
            timeZone: 'Europe/Vienna',
          })
        : '–';

      return {
        timestamp: typeof point.timestamp === 'string' ? point.timestamp : point.timestamp.toISOString(),
        value: point.value,
        timeLabel: timeFormat === 'time' ? timePart : `${datePart} ${timePart}`,
        timestampDate: timestampDate,
        inBaseline: hasBaseline ? inBaseline : true,
        greenSegment: hasBaseline ? (inBaseline ? point.value : null) : point.value,
        redSegment: hasBaseline && !inBaseline ? point.value : null,
      };
    });
  }, [historicalData, baselineReady, greenBand, timeFormat]);
  
  // Prepare material change markers (find closest data points)
  const materialChangeMarkers = useMemo(() => {
    if (!materialChanges || materialChanges.length === 0 || chartData.length === 0) {
      return [];
    }
    
    return materialChanges
      .map((change) => {
        const changeTime = new Date(change.timestamp);
        // Find the closest data point
        let closestPoint = chartData[0];
        let minDiff = Math.abs(changeTime.getTime() - closestPoint.timestampDate.getTime());
        
        for (const point of chartData) {
          const diff = Math.abs(changeTime.getTime() - point.timestampDate.getTime());
          if (diff < minDiff) {
            minDiff = diff;
            closestPoint = point;
          }
        }
        
        // Only include if within reasonable time range (e.g., within 1 hour of chart data)
        const maxDiff = 60 * 60 * 1000; // 1 hour in milliseconds
        if (minDiff <= maxDiff) {
          return {
            x: closestPoint.timeLabel,
            material_id: change.material_id,
            timestamp: change.timestamp,
          };
        }
        return null;
      })
      .filter((marker): marker is { x: string; material_id: string; timestamp: string } => marker !== null);
  }, [materialChanges, chartData]);

  // Whether baseline visuals should be shown (show in all states if baseline is ready)
  // Must be defined before statusText useMemo that uses it
  const showBaseline = baselineReady && !!baselineMean && !!greenBand;

  // Status text (show baseline info even when not in production, but with neutral evaluation)
  const statusText = useMemo(() => {
    // If not in production but baseline is available, show baseline info with neutral status
    if (!isInProduction && showBaseline) {
      return '⚪ Baseline verfügbar (Bewertung nur im PRODUKTIONS‑Modus)';
    }
    // Normal status when in production or when baseline is not available
    if (severity === 2) return '🔴 ROT – Kritische Abweichung';
    if (severity === 1) return '🟠 ORANGE – Abweichung vom Baseline‑Bereich';
    if (severity === 0) return '🟢 GRÜN – Im Baseline‑Bereich';
    if (showBaseline) return '⚪ Baseline verfügbar (keine Bewertung)';
    return '⚪ UNBEKANNT – Keine Bewertung';
  }, [severity, isInProduction, showBaseline]);

  // Deviation text (only meaningful when baseline is available)
  const deviationText = useMemo(() => {
    if (deviation === null || deviation === undefined) return null;
    if (!baselineReady || !baselineMean) return null;
    const absDev = Math.abs(deviation);
    const pctDev = baselineMean && baselineMean !== 0 
      ? ((absDev / baselineMean) * 100).toFixed(1) 
      : null;
    
    if (pctDev) {
      return `${deviation > 0 ? '+' : ''}${deviation.toFixed(2)} ${unit} (${pctDev}%)`;
    }
    return `${deviation > 0 ? '+' : ''}${deviation.toFixed(2)} ${unit}`;
  }, [deviation, baselineMean, unit, baselineReady]);

  // Setup deviation badge text ("SETUP-ABWEICHUNG +20%")
  const setupDeviationBadge = useMemo(() => {
    if (setupDeviation === null || setupDeviation === undefined) return null;
    const rounded = Math.round(setupDeviation);
    const sign = rounded > 0 ? '+' : '';
    return `SETUP-ABWEICHUNG ${sign}${rounded}%`;
  }, [setupDeviation]);

  // Flash LIVE value when it changes (for subtle update animation)
  const [liveKey, setLiveKey] = useState(0);
  useEffect(() => {
    if (currentValue !== null && currentValue !== undefined) {
      setLiveKey((k) => k + 1);
    }
  }, [currentValue]);

  // Chart domain calculation
  const allValues = [
    ...chartData.map(d => d.value),
    currentValue,
    ...(showBaseline ? [baselineMean as number, greenBand!.min, greenBand!.max] : []),
  ].filter((v): v is number => v !== null && v !== undefined && !isNaN(v));

  // Ensure we have valid values for domain calculation
  let yDomain: [number, number];
  if (allValues.length === 0) {
    // Fallback: use baseline values if no other data
    const fallbackValues = showBaseline
      ? [baselineMean as number, greenBand!.min, greenBand!.max].filter((v): v is number => v !== null && v !== undefined && !isNaN(v))
      : chartData.map(d => d.value).filter((v): v is number => v !== null && v !== undefined && !isNaN(v));
    if (fallbackValues.length > 0) {
      const minVal = Math.min(...fallbackValues);
      const maxVal = Math.max(...fallbackValues);
      const range = maxVal - minVal || 1; // Prevent division by zero
      const padding = range * 0.1;
      yDomain = [minVal - padding, maxVal + padding];
    } else {
      // Ultimate fallback
      yDomain = [0, 100];
    }
  } else {
    const minValue = Math.min(...allValues);
    const maxValue = Math.max(...allValues);
    const range = maxValue - minValue || 1; // Prevent division by zero
    const padding = range * 0.1; // 10% padding
    yDomain = [minValue - padding, maxValue + padding];
  }

  return (
    <div className="bg-white/95 backdrop-blur-sm border-2 border-slate-200/80 rounded-2xl p-6 shadow-lg hover:shadow-xl transition-all duration-300">
      {/* Chart Header */}
      <div className="mb-5">
        <div className="flex items-center gap-3 mb-2">
          <h3 className="text-xl font-bold text-slate-900">{sensorName}</h3>
          {/* Stability Dot Indicator */}
          {stability && stability !== 'unknown' && (
            <div
              className="w-3 h-3 rounded-full"
              style={{
                backgroundColor:
                  stability === 'green' ? '#10b981' :
                  stability === 'orange' ? '#f59e0b' :
                  stability === 'red' ? '#ef4444' : '#94a3b8',
              }}
              title={`Stability: ${stability}`}
            />
          )}
        </div>
        {baselineMaterial && (
          <p className="text-xs text-slate-600">
            Baseline (Material: <span className="font-medium">{baselineMaterial}</span>
            {typeof baselineConfidence === 'number' && !Number.isNaN(baselineConfidence) && (
              <>
                {', '}
                Confidence:{' '}
                <span className="font-medium">
                  {(baselineConfidence * 100).toFixed(0)}%
                </span>
              </>
            )}
            )
          </p>
        )}
        {!baselineMaterial && (
          <p className="text-xs text-slate-600">
            Baseline
            {typeof baselineConfidence === 'number' && !Number.isNaN(baselineConfidence) && (
              <>
                {': '}
                Confidence{' '}
                <span className="font-medium">
                  {(baselineConfidence * 100).toFixed(0)}%
                </span>
              </>
            )}
          </p>
        )}
      </div>

      {/* Status and Deviation Info */}
      <div className="mb-5 flex flex-wrap items-center justify-between gap-4 text-sm bg-slate-50/50 rounded-lg p-3 border border-slate-200/50">
        {/* Left: colored dot + exact German status text (smooth transition, pulse on anomaly) */}
        <div className="flex items-center gap-3">
          <span
            className={`w-3 h-3 rounded-full transition-all duration-500 ${
              severity === 2 ? 'animate-pulse' : ''
            }`}
            style={{
              backgroundColor:
                severity === 2 ? '#ef4444' :
                severity === 1 ? '#f59e0b' :
                severity === 0 ? '#10b981' :
                '#9ca3af',
            }}
          />
          <span className="font-semibold text-slate-800 transition-all duration-300">
            {statusText}
          </span>
        </div>
        {deviationText && showBaseline && (
          <div className="flex items-center gap-2">
            <span className="font-semibold text-slate-700">Abweichung:</span>
            <span
              className={`font-bold px-2 py-1 rounded-md ${
                isInProduction && Math.abs(deviation || 0) > (baselineMean! * 0.1)
                  ? 'bg-amber-100 text-amber-700 border border-amber-300'
                  : 'bg-slate-100 text-slate-600 border border-slate-300'
              }`}
            >
              {deviationText}
            </span>
          </div>
        )}
        {currentValue !== null && currentValue !== undefined && (
          <div className="flex items-center gap-3">
            <span className="text-xs font-bold uppercase tracking-wide px-3 py-1 rounded-full bg-emerald-600 text-white shadow-sm transition-all duration-300">
              LIVE
            </span>
            <span
              key={liveKey}
              className="font-extrabold px-3 py-1 rounded-md border border-emerald-600 text-emerald-700 bg-emerald-50 transition-all duration-300"
            >
              {currentValue.toLocaleString('de-DE', {
                minimumFractionDigits: 1,
                maximumFractionDigits: 1,
              })}{' '}
              {unit}
            </span>
          </div>
        )}
        {explanation && (
          <div className="w-full text-xs text-slate-600">
            <span className="font-semibold text-slate-700">Explanation:</span>{" "}
            <span>{explanation}</span>
          </div>
        )}
      </div>

      {/* Chart */}
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData} margin={{ top: 5, right: 12, left: 4, bottom: 35 }}>
            <XAxis
              dataKey="timeLabel"
              tick={{ fill: '#9ca3af', fontSize: 11, angle: -35, textAnchor: 'end', dy: 10 }}
              height={60}
              tickLine={false}
              axisLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              domain={yDomain}
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              tickLine={false}
              axisLine={false}
              label={{
                value: unit,
                angle: -90,
                position: 'insideLeft',
                style: { textAnchor: 'middle', fill: '#9ca3af' },
              }}
            />

            {/* Green Baseline Band (shaded area between min and max) */}
            {showBaseline && greenBand && (
              <ReferenceArea
                y1={greenBand.min}
                y2={greenBand.max}
                fill="#10b981"
                fillOpacity={0.15}
                stroke="none"
              />
            )}

            {/* Dashed Baseline Bounds */}
            {showBaseline && greenBand && (
              <>
                <ReferenceLine
                  y={greenBand.min}
                  stroke="#10b981"
                  strokeDasharray="4 4"
                  strokeWidth={1}
                />
                <ReferenceLine
                  y={greenBand.max}
                  stroke="#10b981"
                  strokeDasharray="4 4"
                  strokeWidth={1}
                />
              </>
            )}

            {/* Dashed Baseline Mean Line */}
            {showBaseline && (
              <ReferenceLine
                y={baselineMean}
                stroke="#059669"
                strokeWidth={2}
                strokeDasharray="5 5"
                label={{
                  value: baselineMaterial ? `Baseline (${baselineMaterial})` : 'Baseline',
                  position: 'right',
                  fill: '#059669',
                  fontSize: 11,
                  fontStyle: 'italic',
                }}
              />
            )}

            {/* Vertical Markers for Material Changes */}
            {materialChangeMarkers.map((marker, index) => (
              <ReferenceLine
                key={`material-change-${index}-${marker.timestamp}`}
                x={marker.x}
                stroke="#6366f1"
                strokeWidth={2}
                strokeDasharray="3 3"
                label={{ 
                  value: `Material: ${marker.material_id}`, 
                  position: 'top', 
                  fill: '#6366f1', 
                  fontSize: 9,
                  offset: 5
                }}
              />
            ))}

            {/* Green segment (in baseline) – smooth line with animation */}
            <Line
              type="monotone"
              dataKey="greenSegment"
              stroke="#10b981"
              strokeWidth={2.5}
              dot={false}
              activeDot={{ r: 5 }}
              connectNulls
              isAnimationActive={true}
              animationDuration={700}
              animationEasing="ease-out"
            />
            {/* Red segment (out of baseline) – smooth line, highlight only on hover/click */}
            <Line
              type="monotone"
              dataKey="redSegment"
              stroke="#ef4444"
              strokeWidth={2.5}
              dot={false}
              activeDot={{ r: 5 }}
              connectNulls
              isAnimationActive={true}
              animationDuration={700}
              animationEasing="ease-out"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart Footer Info */}
      {showBaseline && greenBand && (
        <div className="mt-5 pt-4 border-t border-slate-200 text-xs text-slate-600 space-y-2">
          <div className="flex items-center gap-2">
            <span className="w-3 h-3 rounded bg-emerald-500/15 border border-emerald-500/40"></span>
            <span className="font-semibold">Baseline-Bereich:</span> 
            <span className="font-bold text-slate-800">{greenBand.min.toFixed(2)} - {greenBand.max.toFixed(2)} {unit}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-3 h-3 rounded border-2 border-dashed border-slate-400"></span>
            <span className="font-semibold">Baseline-Mittelwert:</span> 
            <span className="font-bold text-slate-800">{baselineMean.toFixed(2)} {unit}</span>
          </div>
          {setupDeviationBadge && (
            <div className="pt-1">
              <span className="inline-flex items-center px-3 py-1 rounded-full border border-amber-500/60 bg-amber-50 text-amber-700 text-[11px] font-extrabold tracking-wide uppercase transition-all duration-300">
                {setupDeviationBadge}
              </span>
            </div>
          )}
        </div>
      )}
      {!showBaseline && (
        <div className="mt-5 pt-4 border-t border-slate-200 text-xs text-slate-500">
          <p className="font-medium">
            Keine Baseline verfügbar{baselineMaterial ? ` für Material ${baselineMaterial}` : ''}
          </p>
        </div>
      )}
    </div>
  );
};
