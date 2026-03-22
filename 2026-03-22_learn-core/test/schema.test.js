const test = require("node:test");
const assert = require("node:assert/strict");

const {
  extractVersion,
  shouldAcceptIncomingVersion,
} = require("../src/schema");

test("extractVersion picks numeric-like fields", () => {
  assert.equal(extractVersion({ version: 12 }), 12);
  assert.equal(extractVersion({ modificationDate: "15" }), 15);
  assert.equal(extractVersion({ modified: "17.5" }), 17.5);
  assert.equal(extractVersion({}), null);
});

test("shouldAcceptIncomingVersion allows null compatibility", () => {
  assert.equal(shouldAcceptIncomingVersion(null, 10), true);
  assert.equal(shouldAcceptIncomingVersion(10, null), true);
});

test("shouldAcceptIncomingVersion rejects stale versions", () => {
  assert.equal(shouldAcceptIncomingVersion(10, 9), false);
  assert.equal(shouldAcceptIncomingVersion(10, 10), false);
  assert.equal(shouldAcceptIncomingVersion(10, 11), true);
});
