const defaultApiProtocol = window.location.protocol === 'https:' ? 'https:' : 'http:';

export const API_BASE_URL =
  process.env.REACT_APP_API_URL
  || `${defaultApiProtocol}//${window.location.hostname}:8000`;
