const { Client } = require("@notionhq/client");
const config = require("../config");
const log = require("../logger");
const {
  buildNotionProperties,
  buildNotionBlocks,
} = require("../mapper/noteToNotion");

const notion = new Client({ auth: config.notionToken });

let notionConfig = {
  parentObj: null,
  titlePropName: "Name",
};

async function initNotionConfig() {
  const rawDbId = config.notionDatabaseId;
  const db = await notion.databases.retrieve({ database_id: rawDbId });

  let parentObj = { database_id: rawDbId };
  let properties = db.properties;

  if (db.data_sources && db.data_sources.length > 0) {
    const dsId = db.data_sources[0].id;
    const ds = await notion.request({
      path: `data_sources/${dsId}`,
      method: "get",
    });
    parentObj = { data_source_id: dsId };
    properties = ds.properties;
    log.info(`using data source: ${dsId}`);
  }

  let titlePropName = "Name";
  for (const [key, val] of Object.entries(properties)) {
    if (val.type === "title") {
      titlePropName = key;
      break;
    }
  }

  notionConfig = { parentObj, titlePropName };
  return notionConfig;
}

function buildProperties(note, syncedAtIso) {
  return buildNotionProperties(note, notionConfig.titlePropName, syncedAtIso);
}

async function listChildBlocks(blockId) {
  const results = [];
  let cursor = undefined;

  while (true) {
    const resp = await notion.blocks.children.list({
      block_id: blockId,
      start_cursor: cursor,
      page_size: 100,
    });
    results.push(...(resp.results || []));
    if (!resp.has_more) break;
    cursor = resp.next_cursor;
  }

  return results;
}

async function replacePageBlocks(pageId, content) {
  const existingBlocks = await listChildBlocks(pageId);
  for (const blk of existingBlocks) {
    await notion.blocks.delete({ block_id: blk.id });
  }

  const blocks = buildNotionBlocks(content || "");
  if (blocks.length === 0) return;

  for (let i = 0; i < blocks.length; i += 100) {
    const batch = blocks.slice(i, i + 100);
    await notion.blocks.children.append({
      block_id: pageId,
      children: batch,
    });
  }
}

async function findExistingPageIdByNoteId(noteId) {
  const isDataSource = Boolean(notionConfig.parentObj?.data_source_id);
  const parentId = isDataSource
    ? notionConfig.parentObj.data_source_id
    : notionConfig.parentObj?.database_id;

  if (!parentId) return null;

  try {
    const path = isDataSource
      ? `data_sources/${parentId}/query`
      : `databases/${parentId}/query`;

    const resp = await notion.request({
      path,
      method: "post",
      body: {
        filter: {
          property: "Simplenote ID",
          rich_text: {
            equals: String(noteId),
          },
        },
        page_size: 1,
      },
    });

    const page = Array.isArray(resp?.results) ? resp.results[0] : null;
    return page?.id || null;
  } catch (err) {
    log.warn(`notion query by Simplenote ID failed: ${err.message || err}`);
    return null;
  }
}

async function notionUpsert(note) {
  const syncedAtIso = new Date().toISOString();
  const properties = buildProperties(note, syncedAtIso);

  const existingPageId =
    note.notion_page_id || (await findExistingPageIdByNoteId(note.note_id));

  if (existingPageId) {
    const currentPage = await notion.pages.retrieve({
      page_id: existingPageId,
    });
    if (currentPage?.archived) {
      await notion.pages.update({
        page_id: existingPageId,
        archived: false,
      });
      log.info(`unarchived notion page before update: ${existingPageId}`);
    }

    await notion.pages.update({
      page_id: existingPageId,
      properties,
    });

    if (config.usePageBlocks) {
      await replacePageBlocks(existingPageId, note.content);
    }

    return existingPageId;
  }

  const createPayload = {
    parent: notionConfig.parentObj,
    properties,
  };

  if (config.usePageBlocks) {
    createPayload.children = buildNotionBlocks(note.content).slice(0, 100);
  }

  const page = await notion.pages.create(createPayload);

  return page.id;
}

module.exports = {
  initNotionConfig,
  notionUpsert,
};
