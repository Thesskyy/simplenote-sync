function chunkText(text, size) {
  const s = String(text || "");
  const out = [];
  for (let i = 0; i < s.length; i += size) out.push(s.slice(i, i + size));
  return out;
}

function parseTags(tagsJson) {
  try {
    const tags = JSON.parse(tagsJson || "[]");
    if (!Array.isArray(tags)) return [];
    return tags.map((t) => String(t).slice(0, 50));
  } catch {
    return [];
  }
}

function buildNotionProperties(note, titlePropName, syncedAtIso) {
  const lines = String(note.content || "").split("\n");
  const title = (lines.find((x) => x.trim()) || "Untitled").slice(0, 80);
  const preview = String(note.content || "").slice(0, 1800);
  const tags = parseTags(note.tags_json);

  return {
    [titlePropName]: {
      title: [{ type: "text", text: { content: title } }],
    },
    "Simplenote ID": {
      rich_text: [{ type: "text", text: { content: note.note_id } }],
    },
    "Content Preview": {
      rich_text: [{ type: "text", text: { content: preview || "(empty)" } }],
    },
    Deleted: {
      checkbox: !!note.deleted,
    },
    Tags: {
      multi_select: tags.map((t) => ({ name: t })),
    },
    "Created Time": {
      date: note.source_created_at ? { start: note.source_created_at } : null,
    },
    "Synced At": {
      date: syncedAtIso ? { start: syncedAtIso } : null,
    },
  };
}

function buildNotionBlocks(content) {
  return chunkText(content || "", 2000).map((c) => ({
    object: "block",
    type: "paragraph",
    paragraph: {
      rich_text: [{ type: "text", text: { content: c } }],
    },
  }));
}

module.exports = {
  buildNotionProperties,
  buildNotionBlocks,
};
