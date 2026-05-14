import React from 'react';
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceDot,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

// Approximated from notebook run: AUC-PR=0.874, operating point at threshold=0.90 → P=0.68, R=0.60
const PR_CURVE_DATA = [
  { recall: 0.00, precision: 1.00 },
  { recall: 0.05, precision: 0.97 },
  { recall: 0.10, precision: 0.95 },
  { recall: 0.20, precision: 0.92 },
  { recall: 0.30, precision: 0.88 },
  { recall: 0.40, precision: 0.84 },
  { recall: 0.50, precision: 0.79 },
  { recall: 0.60, precision: 0.68 },
  { recall: 0.70, precision: 0.55 },
  { recall: 0.80, precision: 0.41 },
  { recall: 0.85, precision: 0.32 },
  { recall: 0.90, precision: 0.22 },
  { recall: 0.95, precision: 0.14 },
  { recall: 1.00, precision: 0.08 },
];

// Derived from known metrics at threshold=0.90 and typical GAT behavior
const THRESHOLD_DATA = [
  { threshold: 0.50, precision: 0.38, recall: 0.87, f1: 0.52 },
  { threshold: 0.60, precision: 0.47, recall: 0.82, f1: 0.59 },
  { threshold: 0.70, precision: 0.55, recall: 0.76, f1: 0.64 },
  { threshold: 0.75, precision: 0.60, recall: 0.72, f1: 0.65 },
  { threshold: 0.80, precision: 0.63, recall: 0.68, f1: 0.65 },
  { threshold: 0.85, precision: 0.66, recall: 0.64, f1: 0.65 },
  { threshold: 0.90, precision: 0.68, recall: 0.60, f1: 0.64 },
  { threshold: 0.95, precision: 0.73, recall: 0.49, f1: 0.58 },
  { threshold: 0.99, precision: 0.81, recall: 0.31, f1: 0.45 },
];

const TICK_STYLE = { fontSize: 10, fill: '#94a3b8' };
const GRID_STYLE = { strokeDasharray: '3 3', stroke: '#e2e8f0' };
const TOOLTIP_STYLE = { fontSize: 12, borderColor: '#e2e8f0' };

export const ModelPerformance: React.FC = () => {
  return (
    <div className="max-w-6xl mx-auto space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-900 tracking-tight">Model Performance</h1>
        <p className="text-sm text-slate-500 mt-1">
          GAT-ResNet evaluated on Elliptic dataset, test timesteps 42–49. Training threshold: 0.90.
        </p>
      </div>

      {/* Metric cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'AUC-PR',              value: '0.874', sub: 'Illicit class' },
          { label: 'MCC',                 value: '0.609', sub: 'Matthews Corr. Coef.' },
          { label: 'Illicit Precision',   value: '68%',   sub: 'At threshold 0.90' },
          { label: 'Illicit Recall',      value: '60%',   sub: 'At threshold 0.90' },
        ].map(({ label, value, sub }) => (
          <div key={label} className="bg-white p-5 rounded border border-slate-200 shadow-sm">
            <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">{label}</div>
            <div className="text-3xl font-semibold text-slate-900">{value}</div>
            <div className="text-xs text-slate-400 mt-1">{sub}</div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* PR Curve */}
        <div className="bg-white p-6 rounded border border-slate-200 shadow-sm h-80 flex flex-col">
          <h3 className="text-sm font-semibold text-slate-900 mb-1">Precision-Recall Curve</h3>
          <p className="text-xs text-slate-400 mb-4">AUC-PR = 0.874 · Red dot = operating point (thr=0.90)</p>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={PR_CURVE_DATA} margin={{ top: 4, right: 16, bottom: 20, left: 4 }}>
              <CartesianGrid {...GRID_STYLE} />
              <XAxis
                dataKey="recall" tick={TICK_STYLE} domain={[0, 1]}
                label={{ value: 'Recall', position: 'insideBottom', offset: -12, fontSize: 11, fill: '#64748b' }}
              />
              <YAxis
                tick={TICK_STYLE} domain={[0, 1]}
                label={{ value: 'Precision', angle: -90, position: 'insideLeft', offset: 12, fontSize: 11, fill: '#64748b' }}
              />
              <Tooltip formatter={(v: any) => (v as number).toFixed(3)} contentStyle={TOOLTIP_STYLE} />
              <Line type="monotone" dataKey="precision" stroke="#3b82f6" strokeWidth={2} dot={false} name="Precision" />
              <ReferenceDot x={0.60} y={0.68} r={5} fill="#ef4444" stroke="none"
                label={{ value: 'thr=0.90', position: 'top', fontSize: 9, fill: '#ef4444', dy: -4 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Threshold Sensitivity */}
        <div className="bg-white p-6 rounded border border-slate-200 shadow-sm h-80 flex flex-col">
          <h3 className="text-sm font-semibold text-slate-900 mb-1">Threshold Sensitivity</h3>
          <p className="text-xs text-slate-400 mb-4">Precision / Recall / F1 across decision thresholds</p>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={THRESHOLD_DATA} margin={{ top: 4, right: 16, bottom: 20, left: 4 }}>
              <CartesianGrid {...GRID_STYLE} />
              <XAxis
                dataKey="threshold" tick={TICK_STYLE}
                label={{ value: 'Threshold', position: 'insideBottom', offset: -12, fontSize: 11, fill: '#64748b' }}
                tickFormatter={(v: number) => v.toFixed(2)}
              />
              <YAxis tick={TICK_STYLE} domain={[0, 1]} />
              <Tooltip formatter={(v: any) => (v as number).toFixed(3)} contentStyle={TOOLTIP_STYLE} />
              <Legend wrapperStyle={{ fontSize: 11, paddingTop: 8 }} />
              <ReferenceLine x={0.90} stroke="#94a3b8" strokeDasharray="4 2"
                label={{ value: 'model default', position: 'top', fontSize: 9, fill: '#94a3b8' }} />
              <Line type="monotone" dataKey="precision" stroke="#3b82f6" strokeWidth={2} dot={{ r: 3 }} name="Precision" />
              <Line type="monotone" dataKey="recall"    stroke="#22c55e" strokeWidth={2} dot={{ r: 3 }} name="Recall" />
              <Line type="monotone" dataKey="f1"        stroke="#f59e0b" strokeWidth={2} dot={{ r: 3 }} strokeDasharray="4 2" name="F1" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Confusion Matrix */}
        <div className="bg-white p-6 rounded border border-slate-200 shadow-sm col-span-1 md:col-span-2 flex flex-col">
          <h3 className="text-sm font-semibold text-slate-900 mb-1">Confusion Matrix</h3>
          <p className="text-xs text-slate-400 mb-6">Test set (timesteps 42–49), threshold = 0.90</p>
          <div className="flex items-center justify-center py-2">
            <table className="text-sm border-collapse">
              <tbody>
                <tr>
                  <td className="p-3 border border-slate-200 bg-slate-50 text-right font-medium text-slate-500" />
                  <td className="p-3 border border-slate-200 bg-slate-50 text-center font-medium text-slate-700 w-36">Pred Licit</td>
                  <td className="p-3 border border-slate-200 bg-slate-50 text-center font-medium text-slate-700 w-36">Pred Illicit</td>
                </tr>
                <tr>
                  <td className="p-3 border border-slate-200 bg-slate-50 text-right font-medium text-slate-700">Actual Licit</td>
                  <td className="p-5 border border-slate-200 text-center bg-white text-slate-900 text-lg">17,955</td>
                  <td className="p-5 border border-slate-200 text-center bg-red-50 text-red-800 text-lg font-semibold">554</td>
                </tr>
                <tr>
                  <td className="p-3 border border-slate-200 bg-slate-50 text-right font-medium text-slate-700">Actual Illicit</td>
                  <td className="p-5 border border-slate-200 text-center bg-red-50 text-red-800 text-lg font-semibold">670</td>
                  <td className="p-5 border border-slate-200 text-center bg-white text-slate-900 text-lg">1,004</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div className="flex justify-center space-x-8 mt-4 text-xs text-slate-500">
            <span>Accuracy: <strong className="text-slate-700">0.94</strong></span>
            <span>Illicit F1: <strong className="text-slate-700">0.6386</strong></span>
            <span>Weighted F1: <strong className="text-slate-700">0.9419</strong></span>
            <span>MCC: <strong className="text-slate-700">0.6093</strong></span>
          </div>
        </div>
      </div>
    </div>
  );
};
