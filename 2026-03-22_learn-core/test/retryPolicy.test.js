const test = require("node:test");
const assert = require("node:assert/strict");

const { computeRetryDelayMs } = require("../src/retryPolicy");

test("computeRetryDelayMs uses exponential backoff", () => {
  assert.equal(computeRetryDelayMs(1, 2000, 300000), 2000);
  assert.equal(computeRetryDelayMs(2, 2000, 300000), 4000);
  assert.equal(computeRetryDelayMs(3, 2000, 300000), 8000);
});

test("computeRetryDelayMs clamps by maxMs", () => {
  assert.equal(computeRetryDelayMs(20, 2000, 300000), 300000);
});

test("computeRetryDelayMs falls back to sane defaults", () => {
  assert.equal(computeRetryDelayMs(0, 0, 0), 2000);
});
