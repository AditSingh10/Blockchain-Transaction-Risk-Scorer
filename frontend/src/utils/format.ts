export const formatTime = (timestamp: number, seconds = true) =>
  new Date(timestamp).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: seconds ? '2-digit' : undefined,
    hour12: false,
  });

export const formatDateTime = (timestamp: number) =>
  new Date(timestamp).toLocaleString([], {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });

export const formatAmount = (amount: number) =>
  amount.toLocaleString(undefined, {
    minimumFractionDigits: 4,
    maximumFractionDigits: 4,
  });

export const formatPercent = (value: number, digits = 1) =>
  `${(value * 100).toFixed(digits)}%`;

export const truncateId = (id: string, left = 8, right = 6) =>
  id.length > left + right + 2 ? `${id.slice(0, left)}…${id.slice(-right)}` : id;
