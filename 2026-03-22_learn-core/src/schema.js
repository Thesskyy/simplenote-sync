function extractVersion(note) {
  const candidates = [
    note?.version,
    note?.modificationDate,
    note?.modified,
    note?.last_modified,
  ];
  for (const val of candidates) {
    if (typeof val === "number" && !Number.isNaN(val)) return val;
    if (
      typeof val === "string" &&
      val.trim() !== "" &&
      !Number.isNaN(Number(val))
    ) {
      return Number(val);
    }
  }
  return null;
}

function firstLineTitle(content) {
  const lines = String(content || "").split("\n");
  return (lines.find((x) => x.trim()) || "Untitled").slice(0, 100);
}

function shouldAcceptIncomingVersion(currentVersion, incomingVersion) {
  if (incomingVersion == null) return true;
  if (currentVersion == null) return true;
  return Number(incomingVersion) > Number(currentVersion);
}

function normalizeTimestampToIso(value) {
  if (value == null) return null;

  if (typeof value === "number" && Number.isFinite(value)) {
    const ms = value < 1e12 ? value * 1000 : value;
    const d = new Date(ms);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  }

  if (typeof value === "string") {
    const s = value.trim();
    if (!s) return null;

    const n = Number(s);
    if (!Number.isNaN(n)) {
      const ms = n < 1e12 ? n * 1000 : n;
      const d = new Date(ms);
      return Number.isNaN(d.getTime()) ? null : d.toISOString();
    }

    const d = new Date(s);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  }

  return null;
}

module.exports = {
  extractVersion,
  firstLineTitle,
  shouldAcceptIncomingVersion,
  normalizeTimestampToIso,
};
