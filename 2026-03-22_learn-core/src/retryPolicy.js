function computeRetryDelayMs(retryCount, baseMs, maxMs) {
  const safeRetry =
    Number.isFinite(retryCount) && retryCount > 0 ? retryCount : 1;
  const safeBase = Number.isFinite(baseMs) && baseMs > 0 ? baseMs : 2000;
  const safeMax = Number.isFinite(maxMs) && maxMs > 0 ? maxMs : 300000;

  const exp = Math.min(safeRetry - 1, 16);
  const delay = safeBase * 2 ** exp;
  return Math.min(delay, safeMax);
}

module.exports = { computeRetryDelayMs };
