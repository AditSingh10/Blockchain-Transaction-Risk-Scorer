import React from 'react';
import { useNavigation } from '../../context/NavigationContext';
import { Transaction } from '../../types';
import { formatAmount, formatDateTime, formatPercent } from '../../utils/format';
import { Icon } from '../ui/Icon';
import {
  EmptyState,
  IconButton,
  InspectorSection,
  PanelHeader,
  RiskBadge,
} from '../ui/Workbench';
import { SubgraphViewer } from './SubgraphViewer';

interface Props {
  transaction: Transaction | null;
  onClose?: () => void;
  className?: string;
  threshold?: number;
}

export const TransactionInspector: React.FC<Props> = ({
  transaction,
  onClose,
  className = '',
  threshold,
}) => {
  const { navigateToEntity } = useNavigation();

  if (!transaction) {
    return (
      <aside className={`transaction-inspector is-empty ${className}`}>
        <PanelHeader title="Transaction inspector" />
        <EmptyState
          icon="target"
          title="No transaction selected"
          detail="Select a stream row or graph node to inspect its risk and neighborhood."
        />
      </aside>
    );
  }

  const operatingThreshold = threshold ?? transaction.threshold;
  const isFlagged = transaction.illicit_probability >= operatingThreshold;

  return (
    <aside className={`transaction-inspector ${className}`}>
      <PanelHeader
        title="Transaction inspector"
        meta={isFlagged ? 'Review required' : 'Below threshold'}
        actions={onClose ? <IconButton icon="close" label="Close inspector" onClick={onClose} /> : undefined}
      />

      <div className="inspector-scroll">
        <div className="inspector-risk-header">
          <div>
            <span className="eyebrow">Illicit probability</span>
            <strong className={isFlagged ? 'risk-number is-high' : 'risk-number'}>
              {formatPercent(transaction.illicit_probability, 2)}
            </strong>
          </div>
          <RiskBadge probability={transaction.illicit_probability} threshold={operatingThreshold} />
        </div>

        <InspectorSection title="Identity">
          <dl className="inspector-list">
            <div className="is-stacked">
              <dt>Transaction ID</dt>
              <dd className="mono break-value">{transaction.tx_id}</dd>
            </div>
            <div>
              <dt>Observed</dt>
              <dd className="mono">{formatDateTime(transaction.timestamp)}</dd>
            </div>
          </dl>
        </InspectorSection>

        <InspectorSection title="Transaction data">
          <dl className="inspector-list">
            <div><dt>Amount</dt><dd className="mono">{formatAmount(transaction.amount)} BTC</dd></div>
            <div><dt>Direct neighbors</dt><dd className="mono">{transaction.neighbors?.length ?? 0}</dd></div>
            <div><dt>Decision threshold</dt><dd className="mono">{formatPercent(operatingThreshold, 0)}</dd></div>
            <div><dt>Decision</dt><dd>{isFlagged ? 'Flag for review' : 'Clear'}</dd></div>
          </dl>
        </InspectorSection>

        <InspectorSection title="Inference">
          <dl className="inspector-list">
            <div><dt>Model</dt><dd className="mono">GAT-ResNet v1.2.0</dd></div>
            <div><dt>Latency</dt><dd className="mono">{transaction.inference_latency_ms.toFixed(2)} ms</dd></div>
            <div><dt>Graph context</dt><dd>2-hop neighborhood</dd></div>
          </dl>
        </InspectorSection>

        <InspectorSection title="Graph neighborhood">
          <SubgraphViewer txId={transaction.tx_id} height={230} compact />
        </InspectorSection>
      </div>

      <div className="inspector-actions">
        <button className="button button-primary" onClick={() => navigateToEntity(transaction.tx_id)}>
          <Icon name="graph" size={14} />
          Investigate graph
        </button>
        <button
          className="button"
          onClick={() => navigator.clipboard?.writeText(transaction.tx_id)}
        >
          Copy ID
        </button>
      </div>
    </aside>
  );
};

export const TransactionDrawer = TransactionInspector;
