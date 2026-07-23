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
import { MetricReadout, Panel, PanelHeader } from '../components/ui/Workbench';
import { useWebSocketContext } from '../context/WebSocketContext';
import { formatPercent } from '../utils/format';

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

const tick = { fontSize: 10, fill: '#788896', fontFamily: 'ui-monospace, monospace' };
const grid = { strokeDasharray: '2 3', stroke: '#27323c' };
const tooltip = {
  fontSize: 12,
  color: '#d8e0e7',
  background: '#18212a',
  border: '1px solid #34414d',
  borderRadius: 2,
};

export const ModelPerformance: React.FC = () => {
  const { threshold } = useWebSocketContext();

  return (
    <div className="page">
      <header className="workbench-heading">
        <div>
          <h1>Model Performance</h1>
          <p>Offline GAT-ResNet evaluation on Elliptic test timesteps 42–49.</p>
        </div>
        <div className="evaluation-context">
          <span>Offline evaluation</span>
          <code>TEST 42–49</code>
          <code>DEFAULT THR 0.90</code>
        </div>
      </header>

      <div className="metric-rail">
        <MetricReadout label="AUC–PR" value="0.874" detail="Illicit class" />
        <MetricReadout label="MCC" value="0.609" detail="Matthews correlation" />
        <MetricReadout label="Precision" value="68%" detail="Illicit @ 0.90" />
        <MetricReadout label="Recall" value="60%" detail="Illicit @ 0.90" />
        <MetricReadout
          label="Live threshold"
          value={formatPercent(threshold, 0)}
          detail={threshold === 0.9 ? 'Matches evaluation point' : 'Differs from offline point'}
          tone={threshold === 0.9 ? 'healthy' : 'warning'}
        />
      </div>

      <div className="evaluation-grid">
        <Panel className="chart-panel">
          <PanelHeader title="Precision–recall curve" meta="AUC–PR 0.874" />
          <div className="chart-body">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={PR_CURVE_DATA} margin={{ top: 18, right: 24, bottom: 22, left: 0 }}>
                <CartesianGrid {...grid} />
                <XAxis
                  dataKey="recall"
                  tick={tick}
                  domain={[0, 1]}
                  label={{ value: 'RECALL', position: 'insideBottom', offset: -13, fontSize: 9, fill: '#788896' }}
                />
                <YAxis
                  tick={tick}
                  domain={[0, 1]}
                  label={{ value: 'PRECISION', angle: -90, position: 'insideLeft', offset: 12, fontSize: 9, fill: '#788896' }}
                />
                <Tooltip formatter={(value: any) => Number(value).toFixed(3)} contentStyle={tooltip} />
                <Line type="monotone" dataKey="precision" stroke="#5aa9ed" strokeWidth={2} dot={false} isAnimationActive={false} />
                <ReferenceDot
                  x={0.60}
                  y={0.68}
                  r={5}
                  fill="#ef5b64"
                  stroke="#111820"
                  strokeWidth={2}
                  label={{ value: '0.90', position: 'top', fontSize: 9, fill: '#ef8b92' }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="chart-footnote">Red marker is the documented offline operating point: 68% precision / 60% recall.</div>
        </Panel>

        <Panel className="chart-panel">
          <PanelHeader title="Threshold sensitivity" meta="Offline evaluation series" />
          <div className="chart-body">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={THRESHOLD_DATA} margin={{ top: 18, right: 24, bottom: 22, left: 0 }}>
                <CartesianGrid {...grid} />
                <XAxis
                  dataKey="threshold"
                  tick={tick}
                  tickFormatter={(value: number) => value.toFixed(2)}
                  label={{ value: 'THRESHOLD', position: 'insideBottom', offset: -13, fontSize: 9, fill: '#788896' }}
                />
                <YAxis tick={tick} domain={[0, 1]} />
                <Tooltip formatter={(value: any) => Number(value).toFixed(3)} contentStyle={tooltip} />
                <Legend wrapperStyle={{ fontSize: 10, color: '#8e9daa', paddingTop: 5 }} />
                <ReferenceLine x={0.90} stroke="#7f8e9a" strokeDasharray="4 3" />
                <Line type="monotone" dataKey="precision" stroke="#5aa9ed" strokeWidth={1.8} dot={{ r: 2 }} name="Precision" isAnimationActive={false} />
                <Line type="monotone" dataKey="recall" stroke="#63b68d" strokeWidth={1.8} dot={{ r: 2 }} name="Recall" isAnimationActive={false} />
                <Line type="monotone" dataKey="f1" stroke="#d89b3c" strokeWidth={1.8} dot={{ r: 2 }} name="F1" isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="chart-footnote">Raising the threshold tends to improve precision while reducing recall; lower it to widen review coverage.</div>
        </Panel>

        <Panel className="confusion-panel">
          <PanelHeader title="Confusion matrix" meta="Threshold 0.90 · test split" />
          <div className="confusion-layout">
            <table className="confusion-matrix">
              <thead>
                <tr><th /><th>Predicted licit</th><th>Predicted illicit</th></tr>
              </thead>
              <tbody>
                <tr>
                  <th>Actual licit</th>
                  <td><strong>17,955</strong><span>True negative</span></td>
                  <td className="is-error"><strong>554</strong><span>False positive</span></td>
                </tr>
                <tr>
                  <th>Actual illicit</th>
                  <td className="is-error"><strong>670</strong><span>False negative</span></td>
                  <td className="is-correct"><strong>1,004</strong><span>True positive</span></td>
                </tr>
              </tbody>
            </table>
            <dl className="matrix-summary">
              <div><dt>Accuracy</dt><dd>0.94</dd></div>
              <div><dt>Illicit F1</dt><dd>0.6386</dd></div>
              <div><dt>Weighted F1</dt><dd>0.9419</dd></div>
              <div><dt>MCC</dt><dd>0.6093</dd></div>
            </dl>
          </div>
        </Panel>
      </div>
    </div>
  );
};
