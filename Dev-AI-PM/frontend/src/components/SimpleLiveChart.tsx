import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceArea,
} from 'recharts';

export interface SimpleLiveChartDataPoint {
  timestamp: string | Date;
  value: number;
}

export interface SimpleLiveChartProps {
  /** Chart title (e.g. "Temperaturspreizung (Temp_Spread)") */
  title: string;
  /** Optional legend/subtitle (e.g. "Bewertung ohne Baseline: ≤5°C 🟢, 5–8°C 🟠, >8°C 🔴") */
  legend?: string;
  /** Data points with timestamp and value */
  data: SimpleLiveChartDataPoint[];
  /** Y-axis unit (e.g. "°C", "bar", "rpm") */
  unit: string;
  /** Line (and dots) color (default green) */
  lineColor?: string;
  /** Chart height in pixels */
  height?: number;
  /** Optional custom Y-axis domain [min, max]. When set, overrides auto domain (e.g. for pressure: [0, 460] to emphasize 300–450). */
  yDomain?: [number, number];
  /** Optional custom Y-axis tick positions (e.g. [0, 100, 200, 300, 400, 450] for pressure so low values stay in one line). */
  yTicks?: number[];
  /** Optional Y-axis tick label formatter (e.g. when using transformed scale to show real values). */
  yTickFormatter?: (value: number) => string;
  /** Optional tooltip value formatter (e.g. when data is in transformed scale, show real value in tooltip). */
  tooltipValueFormatter?: (value: number) => string;
  /** Optional format for x-axis time label (e.g., 'time' to only show time, 'datetime' to show both) */
  timeFormat?: 'time' | 'datetime';
  /** Optional horizontal background bands (used e.g. for pressure zones) */
  bands?: {
    from: number;
    to: number;
    color: string;
    opacity?: number;
  }[];
}

/**
 * Simple live line chart matching the Temperaturspreizung (Temp_Spread) style:
 * time on X-axis, value on Y-axis, continuous line with dots, grid, tooltip.
 * Use this for all sensor charts so they display consistently.
 */
export const SimpleLiveChart: React.FC<SimpleLiveChartProps> = ({
  title,
  legend,
  data,
  unit,
  lineColor = '#10b981',
  height = 300,
  yDomain: yDomainProp,
  yTicks,
  yTickFormatter,
  tooltipValueFormatter,
  timeFormat = 'datetime',
  bands,
}) => {
  const chartData = React.useMemo(() => {
    // Austria time for 1h/1d: display = Timestamp − 3h 14m (fixed offset)
    const AUSTRIA_OFFSET_MS = (3 * 60 + 14) * 60 * 1000;
    const VIENNA_TZ = 'Europe/Vienna';
    const parseTs = (timestamp: string | Date): Date => {
      if (typeof timestamp !== 'string') return timestamp;
      let iso = String(timestamp).trim();
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?$/.test(iso)) {
        iso += 'Z';
      }
      return new Date(iso);
    };
    const raw = data.map((d) => {
      const ts = parseTs(d.timestamp);
      const valid = !Number.isNaN(ts.getTime());
      // For 1h/1d: Austria time = Timestamp − 3h 14m (used only when timeFormat === 'time')
      const displayTs = valid ? new Date(ts.getTime() - AUSTRIA_OFFSET_MS) : new Date(NaN);
      const datePart = valid
        ? ts.toLocaleDateString('de-DE', {
            day: '2-digit',
            month: '2-digit',
            timeZone: VIENNA_TZ,
          })
        : '–';
      const timePart = valid
        ? displayTs.toLocaleTimeString('de-DE', {
            hour: '2-digit',
            minute: '2-digit',
            hour12: false,
            timeZone: VIENNA_TZ,
          })
        : '–';
      return {
        ...d,
        __ts: ts,
        __displayTs: displayTs,
        __datePart: datePart,
        __timePart: timePart,
      };
    });

    // If timeFormat === 'time' (1h/1d): labels show Austria time (Timestamp − 3h 14m).
    if (timeFormat === 'time') {
      return raw.map((d) => ({
        ...d,
        timeLabel: d.__timePart,
      }));
    }
    // For datetime (used by 1w/1m/all) show date without year; each distinct date gets a label once.
    const sorted = [...raw].sort((a, b) => a.__ts.getTime() - b.__ts.getTime());
    const seenDates = new Set<string>();
    return sorted.map((d) => {
      let label = '';
      if (!seenDates.has(d.__datePart)) {
        seenDates.add(d.__datePart);
        label = d.__datePart; // e.g. "06.02"
      }
      return {
        ...d,
        timeLabel: label, // empty string for repeated points on the same date
      };
    });
  }, [data, timeFormat]);

  // Y-axis domain: use prop if provided, else data range with minimum span so near-constant data doesn't look like a flat line at the edge
  const yDomain = React.useMemo((): [number, number] => {
    if (yDomainProp && yDomainProp.length === 2) return yDomainProp;
    const values = chartData.map((d) => Number(d.value)).filter((v) => typeof v === 'number' && !isNaN(v));
    if (values.length === 0) return [0, 100];
    const minVal = Math.min(...values);
    const maxVal = Math.max(...values);
    const range = maxVal - minVal;
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const minRange = Math.max(mean * 0.02, 1, range * 1.1);
    const half = minRange / 2;
    if (range < minRange * 0.5) {
      return [mean - half, mean + half];
    }
    const padding = range * 0.05 || 1;
    return [minVal - padding, maxVal + padding];
  }, [chartData, yDomainProp]);

  return (
    <div className="bg-white/95 backdrop-blur-sm border-2 border-slate-200/80 rounded-2xl p-6 shadow-lg hover:shadow-xl transition-all duration-300">
      <div className="mb-5">
        <div className="flex items-center gap-3 mb-2">
          <h3 className="text-xl font-bold text-slate-900">{title}</h3>
        </div>
        {legend && (
          <p className="text-xs text-slate-600">
            {legend}
          </p>
        )}
      </div>
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={chartData}
            margin={{ top: 5, right: 20, left: 10, bottom: 35 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            {/* Optional background bands (e.g. pressure zones) */}
            {bands && bands.length > 0 && (
              <>
                {bands.map((band, idx) => {
                  const [yMin, yMax] = yDomain;
                  // Skip bands completely outside the current domain
                  if (band.to < yMin || band.from > yMax) return null;
                  const y1 = Math.max(band.from, yMin);
                  const y2 = Math.min(band.to, yMax);
                  return (
                    <ReferenceArea
                      key={idx}
                      y1={y1}
                      y2={y2}
                      stroke="none"
                      fill={band.color}
                      fillOpacity={band.opacity ?? 0.15}
                    />
                  );
                })}
              </>
            )}
            <XAxis
              dataKey="timeLabel"
              tick={{ fill: '#64748b', fontSize: 11, angle: -35, textAnchor: 'end', dy: 10 }}
              height={60}
              // For long timeframes (1w, 1m, all) we want every date label once → show all ticks.
              interval={timeFormat === 'datetime' ? 0 : 'preserveStartEnd'}
            />
            <YAxis
              domain={yDomain}
              ticks={yTicks}
              tick={{ fill: '#64748b', fontSize: 11 }}
              tickFormatter={yTickFormatter ?? ((value: number) => Number(value).toFixed(0))}
              label={{
                value: unit,
                angle: -90,
                position: 'insideLeft',
                style: { textAnchor: 'middle', fill: '#64748b' },
              }}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#ffffff',
                border: '1px solid #cbd5e1',
                borderRadius: '6px',
                boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
              }}
              labelStyle={{ color: '#1e293b', fontWeight: '600' }}
              formatter={(value: number) => [
                (tooltipValueFormatter ? tooltipValueFormatter(value) : `${Number(value).toFixed(2)} ${unit}`),
                title,
              ]}
            />
            <Line
              type="monotone"
              dataKey="value"
              stroke={lineColor}
              strokeWidth={2.5}
              dot={false}
              activeDot={{ r: 5 }}
              name={title}
              isAnimationActive={true}
              animationDuration={700}
              animationEasing="ease-out"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};
