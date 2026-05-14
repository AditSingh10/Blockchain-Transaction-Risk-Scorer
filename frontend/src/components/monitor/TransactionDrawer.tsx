import React from 'react';
import { Transaction } from '../../types';
import { SubgraphViewer } from './SubgraphViewer';

interface Props {
  transaction: Transaction | null;
  onClose: () => void;
}

export const TransactionDrawer: React.FC<Props> = ({ transaction, onClose }) => {
  if (!transaction) return null;

  return (
    <div className="fixed inset-y-0 right-0 w-[400px] bg-white border-l border-slate-200 shadow-2xl transform transition-transform duration-200 ease-in-out z-50 flex flex-col">
      <div className="px-6 py-4 border-b border-slate-200 flex justify-between items-center bg-slate-50">
        <h2 className="text-sm font-semibold text-slate-900 tracking-tight">Transaction Details</h2>
        <button onClick={onClose} className="text-slate-400 hover:text-slate-600">
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>
      
      <div className="flex-1 overflow-auto p-6 space-y-8">
        {/* Core Info */}
        <div className="space-y-5">
          <div>
            <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1.5">Tx Hash</div>
            <div className="text-sm font-mono text-slate-900 break-all bg-slate-50 p-2.5 rounded border border-slate-200">
              {transaction.tx_id}
            </div>
          </div>
          
          <div className="grid grid-cols-2 gap-4">
            <div>
              <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1.5">Illicit Probability</div>
              <div className={`text-2xl font-semibold tracking-tight ${transaction.flagged ? 'text-red-700' : 'text-slate-900'}`}>
                {(transaction.illicit_probability * 100).toFixed(1)}%
              </div>
              <div className="text-xs text-slate-500 mt-1">
                Threshold: {(transaction.threshold * 100).toFixed(0)}%
              </div>
            </div>
            <div>
              <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1.5">Status</div>
              <div className="mt-1">
                {transaction.flagged ? (
                  <span className="inline-flex items-center px-2.5 py-1 rounded text-xs font-medium bg-red-50 text-red-700 border border-red-200">
                    Flagged for Review
                  </span>
                ) : (
                  <span className="inline-flex items-center px-2.5 py-1 rounded text-xs font-medium bg-slate-50 text-slate-700 border border-slate-200">
                    Cleared
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>

        <hr className="border-slate-200" />

        {/* Graph Placeholder */}
        <div>
          <div className="flex items-center justify-between mb-3">
            <div className="text-xs font-medium text-slate-500 uppercase tracking-wider">Local Neighborhood Graph</div>
            <div className="text-xs text-slate-400">2-Hop Subgraph</div>
          </div>
          <SubgraphViewer txId={transaction.tx_id} height={260} />
        </div>

        {/* Features Placeholder */}
        <div>
          <div className="flex items-baseline space-x-2 mb-1">
            <div className="text-xs font-medium text-slate-500 uppercase tracking-wider">Top Feature Importance</div>
            <div className="text-xs text-slate-400">(illustrative)</div>
          </div>
          <div className="text-xs text-slate-400 mb-3">Representative feature weights — not SHAP values</div>
          <div className="space-y-4">
             <div>
               <div className="flex items-center justify-between text-sm mb-1.5">
                  <span className="text-slate-700 font-medium">Degree Centrality</span>
                  <span className="text-slate-500">0.82</span>
               </div>
               <div className="w-full bg-slate-100 h-1.5 rounded-sm overflow-hidden">
                  <div className="bg-slate-400 h-full" style={{ width: '82%' }}></div>
               </div>
             </div>
             
             <div>
               <div className="flex items-center justify-between text-sm mb-1.5">
                  <span className="text-slate-700 font-medium">1-Hop Illicit Ratio</span>
                  <span className="text-slate-500">0.65</span>
               </div>
               <div className="w-full bg-slate-100 h-1.5 rounded-sm overflow-hidden">
                  <div className="bg-slate-400 h-full" style={{ width: '65%' }}></div>
               </div>
             </div>

             <div>
               <div className="flex items-center justify-between text-sm mb-1.5">
                  <span className="text-slate-700 font-medium">Temporal Aggregation</span>
                  <span className="text-slate-500">0.41</span>
               </div>
               <div className="w-full bg-slate-100 h-1.5 rounded-sm overflow-hidden">
                  <div className="bg-slate-400 h-full" style={{ width: '41%' }}></div>
               </div>
             </div>
          </div>
        </div>
      </div>
    </div>
  );
};
