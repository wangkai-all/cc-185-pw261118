'use strict';

const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const DATA_DIR = process.env.DEMO_DATA_DIR || path.join(ROOT, 'data');

module.exports = {
  ROOT,
  DATA_DIR,
  BUSINESS_DB: path.join(DATA_DIR, 'business.db'),
  ANALYTICS_DB: path.join(DATA_DIR, 'analytics.db'),
  WEB_PORT: Number(process.env.DEMO_WEB_PORT || 4000),
  INGEST_PORT: Number(process.env.DEMO_INGEST_PORT || 4100),
  INGEST_URL: process.env.DEMO_INGEST_URL || 'http://127.0.0.1:4100',
  RELAY_TOKEN: process.env.DEMO_RELAY_TOKEN || 'local-demo-token',
  POLL_INTERVAL_MS: 250,
  MAX_BACKOFF_MS: 5000,
};
