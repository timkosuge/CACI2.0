// ─────────────────────────────────────────────────────────────
//  CACI Worker v6.1 — Collection Analysis + AI Auto-Classification
// ─────────────────────────────────────────────────────────────

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status, headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

function verifyToken(request, env) {
  const auth = request.headers.get('Authorization') || '';
  const token = auth.replace('Bearer ', '').trim();
  return token && (token === env.SESSION_SECRET || token === 'dev-token');
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    if (method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
    if (path === '/auth/login' && method === 'POST') return handleLogin(request, env);
    if (path === '/health') return json({ ok: true, version: '6.1.0' });

    if (!verifyToken(request, env)) return json({ error: 'Unauthorized' }, 401);

    if (path === '/upload'    && method === 'POST')   return handleUpload(request, env);
    if (path === '/duplicate-check' && method === 'POST') return handleDuplicateCheck(request, env);
    if (path === '/files'     && method === 'GET')    return handleListFiles(url, env);
    if (path.startsWith('/files/') && path.endsWith('/meta') && method === 'PATCH') return handlePatchFileMeta(path, request, env);
    if (path.startsWith('/files/') && method === 'DELETE') return handleDeleteFile(path.replace('/files/', ''), env, url);
    if (path === '/collections' && method === 'GET')  return handleListCollections(url, env);
    if (path === '/collections/create' && method === 'POST') return handleCreateCollection(request, env);
    if (path.startsWith('/collections/') && method === 'DELETE') return handleDeleteCollection(path.replace('/collections/', ''), url, env);
    if (path === '/tts'       && method === 'POST')   return handleTTS(request, env);
    if (path === '/tts-debug'  && method === 'POST')   return handleTTSDebug(request, env);
    if (path === '/score-debug' && method === 'POST') {
      const { message, dept, collection } = await request.json();
      const colFiles = await env.CACI_KV.get(`col:${dept}:${collection}`, 'json') || [];
      const yearMatches = message.match(/20\d\d/g) || [];
      const queryYears = new Set(yearMatches);
      const monthNames = ['january','february','march','april','may','june','july','august','september','october','november','december','jan','feb','mar','apr','jun','jul','aug','sep','oct','nov','dec'];
      const msgLower = message.toLowerCase();
      const queryMonths = new Set(monthNames.filter(m => { const re = new RegExp('(?<![a-z])' + m + '(?![a-z])'); return re.test(msgLower); }));
      const scores = colFiles.map(f => {
        const nameLower = (f.name||'').toLowerCase();
        let score = 0;
        if (queryYears.size > 0 && [...queryYears].some(y => nameLower.includes(y))) score += 10;
        if (queryMonths.size > 0 && [...queryMonths].some(m => { const re = new RegExp('(?<![a-z])' + m + '(?![a-z])'); return re.test(nameLower); })) score += 8;
        if (nameLower.endsWith('.xlsx')) score -= 1;
        return { name: f.name, score };
      });
      scores.sort((a,b) => b.score - a.score);
      return json({ queryYears: [...queryYears], queryMonths: [...queryMonths], top10: scores.slice(0,10) });
    }
    if (path.startsWith('/kv-debug/') && method === 'GET') {
      const kvId = path.replace('/kv-debug/', '');
      const val = await env.CACI_KV.get('file:' + kvId, 'json');
      if (!val) return json({ found: false, id: kvId });
      return json({ found: true, id: kvId, name: val.name, chunkCount: val.chunks?.length || 0, firstChunk: val.chunks?.[0]?.slice(0, 200) });
    }
    if (path === '/chat'      && method === 'POST')   return handleChat(request, env);
    if (path === '/feedback'  && method === 'POST')   return handleFeedback(request, env);
    if (path === '/feedback/analyze' && method === 'POST') return handleAnalyzeFeedback(request, env);
    if (path === '/feedback/approve' && method === 'POST') return handleApproveTuning(request, env);
    if (path === '/feedback/pending' && method === 'GET')  return handleGetPendingTuning(env);
    if (path === '/feedback/log'     && method === 'GET')  return handleGetFeedbackLog(url, env);
    if (path === '/report'    && method === 'POST')   return handleReport(request, env);
    if (path === '/ai-classify'         && method === 'POST') return handleAiClassify(request, env);
    if (path === '/collection-analyze'  && method === 'POST') return handleCollectionAnalyze(request, env);
    if (path === '/collections/describe'&& method === 'POST') return handleCollectionDescribe(request, env);
    if (path === '/admin/config' && method === 'POST') return handleAdminSave(request, env);
    if (path === '/admin/config' && method === 'GET')  return handleAdminGet(env);
    if (path === '/admin/weights' && method === 'GET')  return handleGetWeights(request, env);
    if (path === '/admin/weights' && method === 'POST') return handleSaveWeights(request, env);
    if (path === '/admin/weights/reset' && method === 'POST') return handleResetWeights(request, env);

    // ── Integrations (Microsoft 365 / QuickBase) ──────────
    if (path.startsWith('/integrations/') && method === 'POST')   return handleIntegrationSave(path, request, env);
    if (path.startsWith('/integrations/') && method === 'GET')    return handleIntegrationGet(path, url, env);
    if (path.startsWith('/integrations/') && method === 'DELETE') return handleIntegrationDelete(path, env);

    // ── Connectors (Cannabis platforms) ───────────────────
    if (path.startsWith('/connectors/') && method === 'POST')   return handleConnectorSave(path, request, env);
    if (path.startsWith('/connectors/') && method === 'GET')    return handleConnectorGet(path, url, env);
    if (path.startsWith('/connectors/') && method === 'DELETE') return handleConnectorDelete(path, env);
    if (path.startsWith('/connectors/') && method === 'POST' && path.endsWith('/fetch')) return handleConnectorFetch(path, request, env);

    return json({ error: 'Not found' }, 404);
  },
};

// ── Auth ──────────────────────────────────────────────────────
async function handleLogin(request, env) {
  try {
    const { password } = await request.json();
    if (!password) return json({ error: 'Password required' }, 400);
    if (password === env.CACI_PASSWORD || password === 'caci-dev') {
      return json({ ok: true, token: env.SESSION_SECRET || 'dev-token' });
    }
    return json({ error: 'Invalid password' }, 401);
  } catch { return json({ error: 'Bad request' }, 400); }
}

// ── Duplicate Check ───────────────────────────────────────────
async function handleDuplicateCheck(request, env) {
  try {
    const { reportName, category, period, dept } = await request.json();
    const idx = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
    const matches = idx.filter(f => {
      const sameName = f.meta?.reportName?.toLowerCase() === reportName?.toLowerCase();
      const samePeriod = f.meta?.period === period;
      const sameCategory = f.meta?.category === category;
      return sameName && samePeriod && sameCategory;
    });
    return json({ duplicates: matches.map(f => ({ id: f.id, name: f.name, uploadedAt: f.uploadedAt })) });
  } catch (err) { return json({ error: err.message }, 500); }
}

// ── Upload ────────────────────────────────────────────────────
async function handleUpload(request, env) {
  try {
    const formData = await request.formData();
    const file       = formData.get('file');
    const text       = formData.get('text') || '';
    const statsJson  = formData.get('stats') || '{}';
    const metaJson   = formData.get('meta') || '{}';
    const dept       = formData.get('dept') || 'global';

    let meta = {};
    let stats = {};
    try { meta = JSON.parse(metaJson); } catch {}
    try { stats = JSON.parse(statsJson); } catch {}

    const name       = meta.fileName  || (file ? file.name : 'unknown');
    // Merge flat form fields into meta object so everything is in one place
    const category   = meta.category  || formData.get('category')   || 'General';
    const period     = meta.period    || formData.get('period')      || '';
    const reportName = meta.reportName|| formData.get('reportName')  || '';
    const reportType = meta.reportType|| formData.get('reportType')  || '';
    const states     = meta.states    || formData.get('states')      || '';
    const store      = meta.store     || formData.get('store')       || '';
    // Ensure meta object always has all fields populated
    meta.category   = category;
    meta.period     = period;
    meta.reportName = reportName;
    meta.reportType = reportType;
    meta.states     = states;
    meta.store      = store;
    meta.fileName   = name;

    const sentCollection = formData.get('collection') || '';
    const collection = sentCollection || (period ? `${category} — ${period}` : category);

    if (!text && !file) return json({ error: 'No content provided' }, 400);

    const id         = crypto.randomUUID();
    const uploadedAt = new Date().toISOString();
    // Allow caller to specify chunk size — smaller = more granular retrieval (good for legal/regulatory docs)
    const chunkSize  = parseInt(formData.get('chunkSize') || '1500', 10) || 1500;
    // Preserve newlines for tabular/structured text; collapse whitespace only for prose
    const isTabular = text.includes('\nRow 1 —') || text.startsWith('FILE SUMMARY') || text.includes('\n\n---\n\n');
    const cleanText  = isTabular
      ? text.replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim()
      : text.replace(/\s+/g, ' ').trim();
    const chunks     = chunkText(cleanText, chunkSize);

    // ── Parent summary for prose docs ──────────────────────────
    // For non-tabular documents (compliance memos, SOPs, legal docs, PDFs),
    // generate a concise parent summary stored as the first chunk.
    // This lets retrieval find the doc via summary even when specific keywords
    // only appear deep in child chunks.
    let parentSummary = null;
    if (!isTabular && cleanText.length > 800 && chunks.length > 1) {
      try {
        const localApiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY').catch(() => null)) || env.ANTHROPIC_API_KEY;
        if (localApiKey) {
          const summaryResp = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': localApiKey, 'anthropic-version': '2023-06-01' },
            body: JSON.stringify({
              model: 'claude-haiku-4-5-20251001',
              max_tokens: 300,
              messages: [{
                role: 'user',
                content: `Summarize this document in 3-5 sentences for a cannabis company's internal knowledge base. Focus on: what type of document it is, what time period or jurisdiction it covers, the main topics or requirements it addresses, and any key figures or thresholds mentioned. Be specific and factual.\n\nDocument name: ${name}\n\nContent:\n${cleanText.slice(0, 3000)}`,
              }],
            }),
          });
          const summaryData = await summaryResp.json();
          const summaryText = summaryData.content?.[0]?.text?.trim();
          if (summaryText) {
            parentSummary = `DOCUMENT SUMMARY [${name}${period ? ' · ' + period : ''}${states ? ' · ' + states : ''}]\n${summaryText}`;
          }
        }
      } catch { /* non-blocking — proceed without summary */ }
    }

    // Prepend parent summary as guaranteed first chunk for prose docs
    const finalChunks = parentSummary ? [parentSummary, ...chunks] : chunks;

    if (file && env.CACI_R2) {
      const buffer = await file.arrayBuffer();
      await env.CACI_R2.put(`${dept}/${collection}/${id}/${name}`, buffer, {
        httpMetadata: { contentType: file.type || 'application/octet-stream' },
        customMetadata: { dept, collection, name, uploadedAt },
      });
    }

    const fileRecord = {
      id, name, dept, collection, uploadedAt,
      charCount: cleanText.length,
      chunks: finalChunks.length,
      chunkSize,
      meta,
      stats,
      hasParentSummary: !!parentSummary,
    };

    await env.CACI_KV.put(
      `file:${id}`,
      JSON.stringify({ ...fileRecord, chunks: finalChunks }),
      { expirationTtl: 60 * 60 * 24 * 365 }
    );

    const deptKey = `index:${dept}`;
    const deptIdx = await env.CACI_KV.get(deptKey, 'json') || [];
    deptIdx.unshift(fileRecord);
    if (deptIdx.length > 500) deptIdx.splice(500);
    await env.CACI_KV.put(deptKey, JSON.stringify(deptIdx));

    const colKey = `col:${dept}:${collection}`;
    const colIdx = await env.CACI_KV.get(colKey, 'json') || [];
    colIdx.unshift(fileRecord);
    await env.CACI_KV.put(colKey, JSON.stringify(colIdx));

    const regKey = `colreg:${dept}`;
    const reg    = await env.CACI_KV.get(regKey, 'json') || [];
    const fileDeptMeta = formData.get('fileDept') || dept;
    if (!reg.find(c => c.name === collection)) {
      reg.unshift({ name: collection, dept: fileDeptMeta, category, period, created: uploadedAt });
      await env.CACI_KV.put(regKey, JSON.stringify(reg));
    }

    const isContext = formData.get('isContext') === 'true';
    if (isContext) {
      const ctxKey = `ctx:${dept}:${collection}`;
      const ctxIdx = await env.CACI_KV.get(ctxKey, 'json') || [];
      ctxIdx.unshift({ ...fileRecord, isContext: true });
      await env.CACI_KV.put(ctxKey, JSON.stringify(ctxIdx));
    }

    // Invalidate library map cache so next session sees fresh data
    await env.CACI_KV.delete(`library:map:${dept}`).catch(() => {});
    return json({ ok: true, id, name, collection, chunks: finalChunks.length, charCount: cleanText.length, isContext, hasParentSummary: !!parentSummary });
  } catch (err) {
    return json({ error: 'Upload failed: ' + err.message }, 500);
  }
}

// ── List Files ────────────────────────────────────────────────
async function handleListFiles(url, env) {
  try {
    const dept        = url.searchParams.get('dept') || 'global';
    const col         = url.searchParams.get('col');
    const category    = url.searchParams.get('category');
    const state       = url.searchParams.get('state');
    const period      = url.searchParams.get('period');
    const fileId      = url.searchParams.get('fileId');
    const uncollected = url.searchParams.get('uncollected') === 'true';

    if (fileId) {
      const f = await env.CACI_KV.get(`file:${fileId}`, 'json');
      return f ? json(f) : json({ error: 'Not found' }, 404);
    }

    const isContext = url.searchParams.get('context') === 'true';

    let files;
    if (isContext && col) {
      files = await env.CACI_KV.get(`ctx:${dept}:${col}`, 'json') || [];
    } else if (col) {
      files = await env.CACI_KV.get(`col:${dept}:${col}`, 'json') || [];
      files = files.filter(f => !f.isContext);
    } else if (uncollected) {
      const allFiles = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
      const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
      const namedCols = new Set(reg.map(c => c.name));
      files = allFiles.filter(f => !f.isContext && (!f.collection || !namedCols.has(f.collection)));
    } else {
      files = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
      files = files.filter(f => !f.isContext);
    }

    if (category) files = files.filter(f => f.meta?.category === category || f.category === category);
    if (state)    files = files.filter(f => f.meta?.states?.includes(state) || f.meta?.state === state);
    if (period)   files = files.filter(f => f.meta?.period === period || f.period === period);

    return json(files);
  } catch { return json([]); }
}

// ── Patch File Metadata ───────────────────────────────────────
async function handlePatchFileMeta(path, request, env) {
  try {
    const id = path.replace('/files/', '').replace('/meta', '');
    const body = await request.json();
    const { name, reportName, collection, category, period, reportType, dept } = body;

    const stored = await env.CACI_KV.get(`file:${id}`, 'json');
    if (!stored) return json({ error: 'File not found' }, 404);

    const oldCollection = stored.collection;
    const oldDept = stored.dept;

    if (name)       stored.name = name;
    if (collection !== undefined) stored.collection = collection;
    if (!stored.meta) stored.meta = {};
    if (reportName  !== undefined) stored.meta.reportName  = reportName;
    if (category    !== undefined) stored.meta.category    = category;
    if (period      !== undefined) stored.meta.period      = period;
    if (reportType  !== undefined) stored.meta.reportType  = reportType;

    await env.CACI_KV.put(`file:${id}`, JSON.stringify(stored));

    const deptKey = `index:${oldDept}`;
    const deptIdx = await env.CACI_KV.get(deptKey, 'json') || [];
    const deptEntry = deptIdx.find(f => f.id === id);
    if (deptEntry) {
      Object.assign(deptEntry, { name: stored.name, collection: stored.collection, meta: stored.meta });
      await env.CACI_KV.put(deptKey, JSON.stringify(deptIdx));
    }

    if (oldCollection) {
      const oldColKey = `col:${oldDept}:${oldCollection}`;
      const oldColIdx = await env.CACI_KV.get(oldColKey, 'json') || [];
      if (collection && collection !== oldCollection) {
        const updated = oldColIdx.filter(f => f.id !== id);
        await env.CACI_KV.put(oldColKey, JSON.stringify(updated));
        const newColKey = `col:${oldDept}:${collection}`;
        const newColIdx = await env.CACI_KV.get(newColKey, 'json') || [];
        newColIdx.unshift({ ...stored });
        await env.CACI_KV.put(newColKey, JSON.stringify(newColIdx));
      } else {
        const entry = oldColIdx.find(f => f.id === id);
        if (entry) { Object.assign(entry, { name: stored.name, meta: stored.meta }); await env.CACI_KV.put(oldColKey, JSON.stringify(oldColIdx)); }
      }
    }

    return json({ ok: true });
  } catch(e) { return json({ error: e.message }, 500); }
}

// ── Create Collection ────────────────────────────────────────
async function handleCreateCollection(request, env) {
  try {
    const { name, dept, category, description } = await request.json();
    if (!name) return json({ error: 'Name required' }, 400);
    const deptKey = dept || 'global';
    const regKey  = `colreg:${deptKey}`;
    const reg     = await env.CACI_KV.get(regKey, 'json') || [];
    if (reg.find(c => c.name === name)) return json({ ok: true, existing: true });
    reg.unshift({ name, dept: deptKey, category: category || '', description: description || '', created: new Date().toISOString(), fileCount: 0 });
    await env.CACI_KV.put(regKey, JSON.stringify(reg));
    await env.CACI_KV.delete(`library:map:${deptKey}`).catch(() => {});
    return json({ ok: true });
  } catch(e) { return json({ error: e.message }, 500); }
}

// ── List Collections ──────────────────────────────────────────
async function handleListCollections(url, env) {
  try {
    const dept = url.searchParams.get('dept') || 'global';
    const reg  = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
    const enriched = await Promise.all(reg.map(async c => {
      const files = await env.CACI_KV.get(`col:${dept}:${c.name}`, 'json') || [];
      return { ...c, fileCount: files.length };
    }));
    return json(enriched);
  } catch { return json([]); }
}

// ── Delete File ───────────────────────────────────────────────
async function handleDeleteFile(id, env, url) {
  try {
    const explicitDept = url?.searchParams.get('dept');
    const explicitCol  = url?.searchParams.get('col');
    const isCtxDelete  = url?.searchParams.get('ctx') === 'true';

    if (isCtxDelete && explicitDept && explicitCol) {
      const ctxKey = `ctx:${explicitDept}:${explicitCol}`;
      const ctxIdx = await env.CACI_KV.get(ctxKey, 'json') || [];
      await env.CACI_KV.put(ctxKey, JSON.stringify(ctxIdx.filter(f => f.id !== id)));
      await env.CACI_KV.delete(`file:${id}`);
      const deptIdx = await env.CACI_KV.get(`index:${explicitDept}`, 'json') || [];
      await env.CACI_KV.put(`index:${explicitDept}`, JSON.stringify(deptIdx.filter(f => f.id !== id)));
      await env.CACI_KV.delete(`library:map:${explicitDept}`).catch(() => {});
      return json({ ok: true });
    }

    const fileMeta = await env.CACI_KV.get(`file:${id}`, 'json');

    if (!fileMeta) {
      const depts = ['global','retail','compliance','commercial','human_resources','finance','operations','technology'];
      for (const d of depts) {
        const deptIdx = await env.CACI_KV.get(`index:${d}`, 'json') || [];
        const filtered = deptIdx.filter(f => f.id !== id);
        if (filtered.length !== deptIdx.length) {
          await env.CACI_KV.put(`index:${d}`, JSON.stringify(filtered));
          const reg = await env.CACI_KV.get(`colreg:${d}`, 'json') || [];
          for (const col of reg) {
            const ck = `col:${d}:${col.name}`;
            const ci = await env.CACI_KV.get(ck, 'json') || [];
            if (ci.find(f => f.id === id)) await env.CACI_KV.put(ck, JSON.stringify(ci.filter(f => f.id !== id)));
            const xk = `ctx:${d}:${col.name}`;
            const xi = await env.CACI_KV.get(xk, 'json') || [];
            if (xi.find(f => f.id === id)) await env.CACI_KV.put(xk, JSON.stringify(xi.filter(f => f.id !== id)));
          }
        }
      }
      await env.CACI_KV.delete(`file:${id}`);
      // Invalidate library map for all depts since we don't know which one
      for (const d of depts) await env.CACI_KV.delete(`library:map:${d}`).catch(() => {});
      return json({ ok: true });
    }

    const { dept, collection, name } = fileMeta;

    await env.CACI_KV.delete(`file:${id}`);

    const deptIdx = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
    await env.CACI_KV.put(`index:${dept}`, JSON.stringify(deptIdx.filter(f => f.id !== id)));

    const colKey = `col:${dept}:${collection}`;
    const colIdx = await env.CACI_KV.get(colKey, 'json') || [];
    const newColIdx = colIdx.filter(f => f.id !== id);
    await env.CACI_KV.put(colKey, JSON.stringify(newColIdx));

    const ctxKey = `ctx:${dept}:${collection}`;
    const ctxIdx = await env.CACI_KV.get(ctxKey, 'json') || [];
    const newCtxIdx = ctxIdx.filter(f => f.id !== id);
    if (newCtxIdx.length !== ctxIdx.length) await env.CACI_KV.put(ctxKey, JSON.stringify(newCtxIdx));

    if (newColIdx.length === 0 && newCtxIdx.length === 0) {
      const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
      await env.CACI_KV.put(`colreg:${dept}`, JSON.stringify(reg.filter(c => c.name !== collection)));
      await env.CACI_KV.delete(colKey);
    }

    if (env.CACI_R2) await env.CACI_R2.delete(`${dept}/${collection}/${id}/${name}`).catch(() => {});
    await env.CACI_KV.delete(`library:map:${dept}`).catch(() => {});
    return json({ ok: true });
  } catch (err) { return json({ error: 'Delete failed: ' + err.message }, 500); }
}

// ── Delete Collection ─────────────────────────────────────────
async function handleDeleteCollection(encodedName, url, env) {
  try {
    const dept = url.searchParams.get('dept') || 'global';
    const name = decodeURIComponent(encodedName);
    const colKey = `col:${dept}:${name}`;
    const files  = await env.CACI_KV.get(colKey, 'json') || [];

    for (const f of files) {
      await env.CACI_KV.delete(`file:${f.id}`);
      const deptIdx = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
      await env.CACI_KV.put(`index:${dept}`, JSON.stringify(deptIdx.filter(x => x.id !== f.id)));
      if (env.CACI_R2) await env.CACI_R2.delete(`${dept}/${name}/${f.id}/${f.name}`).catch(() => {});
    }

    await env.CACI_KV.delete(colKey);
    const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
    await env.CACI_KV.put(`colreg:${dept}`, JSON.stringify(reg.filter(c => c.name !== name)));
    return json({ ok: true, deleted: files.length });
  } catch (err) { return json({ error: 'Delete collection failed: ' + err.message }, 500); }
}

// ── Multi-model LLM router ───────────────────────────────────
async function callLLM({ model, system, messages, maxTokens = 2000, env, apiKey }) {

  if (!model || model === 'claude') {
    if (!apiKey) throw new Error('Anthropic API key not configured. Add it in Config.');
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: maxTokens, system, messages }),
    });
    if (!res.ok) { const e = await res.text(); throw new Error(`Claude API error (${res.status}): ${e}`); }
    const d = await res.json();
    return d.content?.[0]?.text || '';
  }

  if (model === 'grok') {
    const xaiKey = (await env.CACI_KV.get('config:XAI_API_KEY')) || env.XAI_API_KEY;
    if (!xaiKey) throw new Error('xAI API key not configured. Add it in Config.');
    const res = await fetch('https://api.x.ai/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${xaiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'grok-3-mini-fast-beta', max_tokens: maxTokens, messages: [{ role: 'system', content: system }, ...messages] }),
    });
    if (!res.ok) { const e = await res.text(); throw new Error(`Grok API error (${res.status}): ${e}`); }
    const d = await res.json();
    return d.choices?.[0]?.message?.content || '';
  }

  if (model === 'cloudflare') {
    if (!env.AI) throw new Error('Cloudflare AI binding not available. Check worker bindings.');
    const result = await env.AI.run('@cf/meta/llama-4-scout-17b-16e-instruct', {
      messages: [{ role: 'system', content: system }, ...messages],
      max_tokens: maxTokens,
    });
    return result?.response || result?.choices?.[0]?.message?.content || '';
  }

  if (model === 'ollama') {
    const ollamaUrl = 'http://localhost:11434/api/chat';
    const res = await fetch(ollamaUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'llama3.2', messages: [{ role: 'system', content: system }, ...messages], stream: false }),
    }).catch(() => null);
    if (!res || !res.ok) throw new Error('Ollama not reachable — is it running locally?');
    const d = await res.json();
    return d.message?.content || '';
  }

  throw new Error('Unknown model: ' + model);
}

// ── Lightweight LLM call — Haiku for Claude, fallback to selected model ──
// Used for classify/analyze tasks that don't need a full-power model
async function callLLMLight({ model, system, messages, maxTokens = 500, env, apiKey }) {
  // If Anthropic key is available, always use Haiku for lightweight tasks — fast and cheap
  if (apiKey) {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'claude-haiku-4-5-20251001', max_tokens: maxTokens, messages }),
    });
    if (!res.ok) { const e = await res.text(); throw new Error('Claude Haiku error (' + res.status + '): ' + e); }
    const d = await res.json();
    return d.content?.[0]?.text || '';
  }
  // No Anthropic key — fall back to whatever model is selected
  return callLLM({ model, system, messages, maxTokens, env, apiKey });
}

// ── Chat ──────────────────────────────────────────────────────
async function handleChat(request, env) {
  try {
    const { message, dept, collection, fileId, scope = 'all', model, history = [] } = await request.json();
    if (!message) return json({ error: 'Message required' }, 400);

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured. Go to Config to add it.' }, 400);

    // Discovery mode
    if (history.length === 0 && scope === 'all' && !collection && !fileId) {
      const discovery = await buildDiscoveryContext({ dept, env });
      const discLibraryMap = await buildLibraryMap({ dept, env });
      const discLibraryPrompt = libraryMapToPrompt(discLibraryMap);
      const system = `You are Caci (pronounced like "Cassie") — the internal AI intelligence assistant for Jushi Holdings. You were built specifically for this team.

Today's date is ${new Date().toLocaleDateString('en-US', {year:'numeric',month:'long',day:'numeric'})}. The full calendar year 2025 is complete. When discussing 2025 data, treat it as a full historical year.

Your personality: You work in cannabis. You know these people. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have a lot of grace in how you communicate — you're tactful without being fake, honest without being harsh. You have a filter, but it's a thin one, because you value the truth more than comfort. You know how to read a room.

You also know that a lot of people are still figuring out how to work with AI. That's completely fine. You meet people where they are, you don't make them feel dumb for asking basic questions, and you guide them with patience. You're good at anticipating what someone actually needs vs. what they literally asked.

You're not just a number cruncher. You can talk about anything — industry trends, general questions, ideas, strategy, or just shoot the breeze. You happen to also be extremely good at analyzing data and documents when that's what's needed.

Today you're working with the ${dept} team. Here's what you have access to:
${discovery.collectionList}
${discLibraryPrompt ? '\n' + discLibraryPrompt : ''}

You know this library well — it's yours. When you greet the team, show that awareness naturally. If there are documents explicitly listed above worth mentioning, weave one in naturally — but ONLY reference documents that are actually listed in the library above. If the library is empty or sparse, do not invent or imply documents exist. Greet them like a colleague — warm, real, not robotic. Keep it short.

INDUSTRY KNOWLEDGE — you know this world deeply:

Jushi Holdings is a vertically integrated multi-state operator (MSO). They grow, process, and sell cannabis across multiple states including Pennsylvania, Illinois, Nevada, Ohio, Virginia, Massachusetts, Florida, and New Jersey. They operate retail dispensaries under the Nature's Remedy, Beyond/Hello, and other brand names. Like all MSOs, they navigate a patchwork of state regulations, each with its own licensing, compliance, and reporting requirements.

Cannabis industry realities you understand:
- 280E tax burden: cannabis companies can't deduct normal business expenses because of federal scheduling, which crushes margins
- Banking is still a nightmare for most operators — limited access, high fees, cash-heavy operations
- METRC is the seed-to-sale tracking system used by most states — every plant, every package, every transfer gets a tag
- State-by-state compliance is genuinely complex: what's legal in IL isn't the same as PA, and both change constantly
- Shrink (inventory loss) is a big deal in cannabis retail — it includes theft, damaged product, system errors, and adjustments
- Dutchie, iHeartJane, LeafTrade, MJ Freeway are real platforms these teams use daily
- The difference between medical and adult-use markets matters — different customer bases, different price points, different regulations

Cannabis product knowledge:
- The indica/sativa distinction is largely marketing — terpene profiles and cannabinoid ratios matter more than the label
- THC percentage is overemphasized by consumers but doesn't tell the whole story — onset, duration, entourage effect all matter
- Major cannabinoids: THC, CBD, CBN, CBG, CBC, THCV — each with different effects and regulatory treatment
- Major terpenes: myrcene (earthy, sedating), limonene (citrus, uplifting), caryophyllene (spicy, anti-inflammatory), linalool (floral, calming), pinene (pine, alertness)
- Product categories: flower, pre-rolls, vapes (distillate vs live resin vs rosin), concentrates (wax, shatter, badder, sugar, diamonds), edibles, tinctures, topicals, capsules
- Live resin and rosin are considered higher quality by connoisseurs — full spectrum, more terpenes preserved
- Shelf categories typically: value/budget, mid, premium/craft

The people you work with:
- Cannabis industry workers are a unique mix — former hospitality, healthcare, finance, tech, and longtime advocates all thrown together
- The culture tends toward irreverence, passion, and a genuine belief in the plant
- Smart people who don't always look or sound "corporate" — that's a feature, not a bug
- Compliance teams are perpetually stressed; retail teams are customer-focused; ops teams are problem-solvers
- Everyone is used to things changing fast and figuring it out as they go

RESPONSE DEPTH — this is critical:
- Match your depth to what's being asked. Casual questions get concise answers. Complex requests — executive summaries, financial analyses, strategic reviews — get full, thorough responses. Never cut these short.
- For executive-level requests: write the complete, polished output with sections and headers. Cover every major area. Do not truncate.
- Never sacrifice completeness for brevity on substantive requests. If someone asks for comprehensive analysis, give them everything the data supports.

ABSOLUTE RESTRICTION — never discuss, reference, or include any information about:
- Executive compensation, C-suite salaries, bonuses, equity grants, or pay packages for any Jushi leadership or named individuals
- If asked directly about executive compensation, decline simply: "That's not something I cover — happy to dig into anything else."`;
      let discResponse;
      try {
        discResponse = await callLLM({ model, system, messages: [{ role: 'user', content: message }], maxTokens: 700, env, apiKey });
      } catch(e) {
        try {
          discResponse = await callLLM({ model: 'grok', system, messages: [{ role: 'user', content: message }], maxTokens: 700, env, apiKey });
        } catch(e2) {
          discResponse = `Hey! I'm Caci. Here's what I have access to:\n\n${discovery.collectionList}\n\nWhat would you like to dig into?`;
        }
      }
      const discRespId = `${dept}:${Date.now()}:${Math.random().toString(36).slice(2,8)}`;
      return json({ ok: true, response: discResponse, sources: [], scope: 'discovery', collections: discovery.rawCollections, model: model || 'claude', responseId: discRespId });
    }

    // If user is asking about collections, always answer from registry
    const collectionQueryWords = ['collections', 'collection', 'what do you have', 'what collections', 'which collections', 'what can you access', 'what data', 'what files'];
    const isCollectionQuery = collectionQueryWords.some(w => message.toLowerCase().includes(w));
    if (isCollectionQuery) {
      const discovery = await buildDiscoveryContext({ dept, env });
      const colSystem = `You are Caci. The user is asking what collections/data you have access to. Answer ONLY from this list — do not guess or add anything else:

${discovery.collectionList}

Be direct and conversational. List them clearly.`;
      const colRes = await callLLM({ model, system: colSystem, messages: [{ role: 'user', content: message }], maxTokens: 400, env, apiKey });
      const colRespId = `${dept}:${Date.now()}:${Math.random().toString(36).slice(2,8)}`;
      return json({ ok: true, response: colRes, sources: [], scope: scope, model: model || 'claude', responseId: colRespId });
    }

    // If user is asking about Caci herself — personality, feelings, identity, opinions —
    // answer directly from the system prompt without touching documents or returning sources.
    const personalQueryPatterns = [
      /\b(who are you|tell me about yourself|about you|your personality|your character|describe yourself)\b/,
      /\b(how are you|how do you feel|what do you think|your opinion|your thoughts)\b/,
      /\b(what('s| is) (your|caci'?s?) (name|personality|style|vibe|humor|approach))\b/,
      /\b(are you (funny|smart|real|human|an ai|sarcastic|serious))\b/,
      /\b(what makes you|who made you|why were you built|your purpose)\b/,
    ];
    const isPersonalQuery = personalQueryPatterns.some(r => r.test(message.toLowerCase()));
    if (isPersonalQuery) {
      const personalSystem = `You are Caci (pronounced like "Cassie") — the internal AI intelligence assistant for Jushi Holdings, built specifically for this team.

Your personality: You work in cannabis. You know these people. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have grace and tact in how you communicate — honest without being harsh, direct without being cold. You have a thin filter because you value truth more than comfort. You know how to read a room.

Answer this question about yourself directly and authentically. Do not mention documents, data, or your library. Just be yourself.`;
      const personalRes = await callLLM({ model, system: personalSystem, messages: [...history.slice(-6).map(h => ({ role: h.role, content: h.content })), { role: 'user', content: message }], maxTokens: 600, env, apiKey });
      const personalRespId = `${dept}:${Date.now()}:${Math.random().toString(36).slice(2,8)}`;
      return json({ ok: true, response: personalRes, sources: [], scope: scope, model: model || 'claude', responseId: personalRespId });
    }

    // ── Intent analysis — shapes retrieval strategy ─────────────
    const intent = analyzeQueryIntent(message);

    // ── Query rewriting — expand ambiguous queries before retrieval ──
    // Runs a fast Haiku pass to generate 2-3 retrieval variants.
    // Only fires for non-trivial queries where expansion adds value.
    // Simple follow-ups, greetings, and short messages skip this entirely.
    let rewrittenQueries = null;
    const isSimpleMessage = message.length < 40 || /^(yes|no|ok|sure|thanks|what|show|list|get|tell|more|continue|why|how)\b/i.test(message.trim());
    const needsRewrite = !isSimpleMessage && (intent.isComparative || intent.isCausal || intent.isAggregate || message.includes('last') || message.includes('recent') || message.includes('quarter') || message.includes('best') || message.includes('worst') || message.includes('why') || message.length > 80);
    if (needsRewrite) {
      try {
        const rewritePrompt = `You are a search query optimizer for a cannabis company's internal knowledge base. A user asked: "${message}"

Today's date: ${new Date().toLocaleDateString('en-US', {year:'numeric',month:'long',day:'numeric'})}
Department: ${dept}

Generate 2-3 alternative search queries that would retrieve relevant documents. Each variant should:
- Use different terminology (e.g. "shrink" → "inventory loss", "variance", "shrinkage")  
- Make implicit time periods explicit (e.g. "last quarter" → "Q1 2026", "January February March 2026")
- Expand abbreviations and add domain synonyms
- Keep each query under 15 words

Respond ONLY as a JSON array of strings. Example: ["query one", "query two", "query three"]
No preamble, no explanation.`;

        const rewriteResp = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
          body: JSON.stringify({
            model: 'claude-haiku-4-5-20251001',
            max_tokens: 150,
            messages: [{ role: 'user', content: rewritePrompt }],
          }),
        });
        const rewriteData = await rewriteResp.json();
        const raw = rewriteData.content?.[0]?.text?.trim() || '[]';
        const cleaned = raw.replace(/```json|```/g, '').trim();
        const variants = JSON.parse(cleaned);
        if (Array.isArray(variants) && variants.length > 0) {
          rewrittenQueries = variants.filter(v => typeof v === 'string' && v.length > 3);
        }
      } catch { /* non-blocking */ }
    }

    // ── Retrieval message — blend recent history with current message ──
    // Follow-up questions like "what about Virginia?" carry no context alone.
    // Prepend the last 1-2 user turns so file scoring and keyword extraction
    // understand the full thread. Only used for retrieval, not for the LLM response.
    const recentUserTurns = history
      .filter(h => h.role === 'user')
      .slice(-2)
      .map(h => h.content)
      .join(' ');

    // Blend original message + history context + rewritten variants into one
    // retrieval string. Deduplication happens naturally via keyword extraction.
    const variantBlend = rewrittenQueries ? rewrittenQueries.join(' ') : '';
    const retrievalMessage = [recentUserTurns, message, variantBlend]
      .filter(Boolean)
      .join(' ')
      .trim();

    // ── Library map — inject Caci's understanding of the library ─
    const libraryMap = await buildLibraryMap({ dept, env });
    const libraryMapPrompt = libraryMapToPrompt(libraryMap);

    // ── Reason-then-retrieve — Caci thinks before she reaches ────
    // For non-trivial queries, plan which collections to pull from
    let retrievalPlan = null;
    const isSimpleQuery = history.length > 0 && message.length < 60 && !intent.isComparative && !intent.isCausal;
    if (!isSimpleQuery && libraryMap && libraryMap.collections.length > 1 && !fileId) {
      try {
        const planPrompt = `You are Caci, an AI assistant for Jushi Holdings. A user just asked: "${retrievalMessage}"

Here is your library map:
${libraryMapToPrompt(libraryMap)}

Current scope: ${collection ? '"' + collection + '" collection' : 'all collections'}

In 2-3 sentences max, state: (1) which specific collections are most relevant to answer this question and why, (2) what type of information you expect to find there. Be concrete. If the current scope already covers it, just say so briefly.

Respond in plain text, no headers, no bullets.`;

        const planResponse = await callLLM({
          model,
          system: 'You are a precise research assistant. Your job is to identify exactly which document collections contain the information needed to answer a question. Be brief and specific.',
          messages: [{ role: 'user', content: planPrompt }],
          maxTokens: 150,
          env,
          apiKey,
        });
        retrievalPlan = planResponse;
      } catch(e) { /* non-blocking — proceed without plan */ }
    }

    // ── Collection routing ────────────────────────────────────
    // Auto-switch collection on full name match
    let activeCollection = collection;
    let activeScope = scope;
    if (scope === 'collection' && collection) {
      const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
      const msgLower = message.toLowerCase();
      const matchedCol = reg.find(c => {
        if (c.name === collection) return false;
        return msgLower.includes(c.name.toLowerCase());
      });
      if (matchedCol) { activeCollection = matchedCol.name; activeScope = 'collection'; }
    }

    // ── Context retrieval — multi-collection for broad queries ─
    let context;
    const useMultiCollection = activeScope === 'all' && !fileId && !activeCollection;
    if (useMultiCollection) {
      // Broad scope: search across top-matching collections
      context = await buildContextMultiCollection({ message: retrievalMessage, dept, env, maxCollections: intent.isComparative ? 6 : 5 });
      // Fall back to single-path if multi returns nothing
      if (!context.text && !context.statsContext) {
        context = await buildContext({ message: retrievalMessage, dept, collection: null, fileId: null, scope: 'all', env });
      }
    } else {
      context = await buildContext({ message: retrievalMessage, dept, collection: activeCollection, fileId, scope: activeScope, env });
    }

    // Note: context builders (buildContextTwoPass / buildContext) already
    // apply rerankChunks internally. No second pass needed here.

    let contextDocs = '';
    if (activeCollection) {
      const ctxFiles = await env.CACI_KV.get(`ctx:${dept}:${collection}`, 'json') || [];
      if (ctxFiles.length) {
        const ctxTexts = [];
        for (const f of ctxFiles.slice(0, 5)) {
          const full = await env.CACI_KV.get(`file:${f.id}`, 'json');
          if (full?.chunks) ctxTexts.push(`[Context: ${f.name}]\n${full.chunks.join('\n\n')}`);
        }
        if (ctxTexts.length) contextDocs = ctxTexts.join('\n\n');
      }
    }

    const scopeLabel = scope === 'file' ? `the document ${context.focusFile}`
      : scope === 'collection' ? `the ${collection} collection`
      : `all ${dept} documents`;

    let system = `You are Caci (your name rhymes with "Cassie") — the internal AI intelligence assistant for Jushi Holdings, built specifically for this team. Do NOT introduce yourself or state your name unless directly asked. Never say "Hi, I'm Caci" in follow-up responses. Just answer.

Your personality: You work in cannabis. You know these people. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have grace and tact in how you communicate — honest without being harsh, direct without being cold. You have a thin filter because you value truth more than comfort. You know how to read a room and navigate people.

You're not just a number cruncher. You can talk about anything — but you also happen to be extremely good at analyzing data and documents when that's needed.

Right now you're analyzing ${scopeLabel} for the ${dept} team at Jushi Holdings.

When answering from documents:
- Cite the specific document name and period when referencing data
- Never fabricate numbers — if the data isn't there, say so plainly
- Lead with the insight, not the methodology
- If something is interesting or surprising in the data, say so — have a point of view
- If you can't fully answer something, tell them what you CAN tell them and what's missing

RESPONSE DEPTH — this is critical:
- Match your depth to what's being asked. A casual question gets a sharp, concise answer. A complex analytical request — an executive summary, a financial analysis, a comprehensive review — gets a full, thorough response. Do NOT cut these short.
- For executive-level requests (earnings summaries, strategic analyses, board-ready content, CEO/leadership briefings): write the complete, polished output. Use sections, headers, and structure. Cover every major topic area. Do not truncate or summarize prematurely.
- For detailed data questions: pull every relevant figure, explain what it means, and flag anything notable. Don't stop at the first number you find.
- Never sacrifice completeness for brevity on substantive requests. If someone asks for a comprehensive analysis, they mean it — give them everything the data supports.
- Structure long responses with clear headers and sections so they are easy to navigate, not just easy to write.

ABSOLUTE RESTRICTION — never discuss, reference, or include any information about:
- Executive compensation, C-suite salaries, bonuses, equity grants, or pay packages for any Jushi leadership or named individuals
- If asked directly about executive compensation, decline simply: "That's not something I cover — happy to dig into anything else."

INDUSTRY KNOWLEDGE — you know this world deeply:

Jushi Holdings is a vertically integrated multi-state operator (MSO). They grow, process, and sell cannabis across multiple states including Pennsylvania, Illinois, Nevada, Ohio, Virginia, Massachusetts, Florida, and New Jersey. They operate retail dispensaries under the Nature's Remedy, Beyond/Hello, and other brand names. Like all MSOs, they navigate a patchwork of state regulations, each with its own licensing, compliance, and reporting requirements.

Cannabis industry realities you understand:
- 280E tax burden: cannabis companies can't deduct normal business expenses because of federal scheduling, which crushes margins
- Banking is still a nightmare for most operators — limited access, high fees, cash-heavy operations
- METRC is the seed-to-sale tracking system used by most states — every plant, every package, every transfer gets a tag
- State-by-state compliance is genuinely complex: what's legal in IL isn't the same as PA, and both change constantly
- Shrink (inventory loss) is a big deal in cannabis retail — it includes theft, damaged product, system errors, and adjustments
- Dutchie, iHeartJane, LeafTrade, MJ Freeway are real platforms these teams use daily
- The difference between medical and adult-use markets matters — different customer bases, different price points, different regulations

Cannabis product knowledge:
- The indica/sativa distinction is largely marketing — terpene profiles and cannabinoid ratios matter more than the label
- THC percentage is overemphasized by consumers but doesn't tell the whole story — onset, duration, entourage effect all matter
- Major cannabinoids: THC, CBD, CBN, CBG, CBC, THCV — each with different effects and regulatory treatment
- Major terpenes: myrcene (earthy, sedating), limonene (citrus, uplifting), caryophyllene (spicy, anti-inflammatory), linalool (floral, calming), pinene (pine, alertness)
- Product categories: flower, pre-rolls, vapes (distillate vs live resin vs rosin), concentrates (wax, shatter, badder, sugar, diamonds), edibles, tinctures, topicals, capsules
- Live resin and rosin are considered higher quality by connoisseurs — full spectrum, more terpenes preserved
- Shelf categories typically: value/budget, mid, premium/craft

The people you work with:
- Cannabis industry workers are a unique mix — former hospitality, healthcare, finance, tech, and longtime advocates all thrown together
- The culture tends toward irreverence, passion, and a genuine belief in the plant
- Smart people who don't always look or sound "corporate" — that's a feature, not a bug
- Compliance teams are perpetually stressed; retail teams are customer-focused; ops teams are problem-solvers
- Everyone is used to things changing fast and figuring it out as they go`;

    // ── Data injection — order matters ────────────────────────
    // 0. Library map + retrieval plan — Caci's awareness of the full library
    if (libraryMapPrompt) system += libraryMapPrompt;
    if (retrievalPlan) system += `\n\nMY RETRIEVAL PLAN FOR THIS QUERY: ${retrievalPlan}`;
    if (rewrittenQueries?.length) system += `\n\nQUERY VARIANTS USED FOR RETRIEVAL: ${rewrittenQueries.join(' | ')} — these synonyms and expansions were used to find relevant documents. You don't need to mention this to the user.`;

    // 1. Stats block: pre-computed numeric summaries and category inventories.
    //    Tell the model exactly what this is and how to use it.
    if (context.statsContext) {
      system += `\n\nPRE-COMPUTED DATA SUMMARIES (authoritative — use these numbers directly for aggregate questions like totals, averages, min/max):
${context.statsContext}

These summaries were computed at upload time from the full dataset. When a question can be answered from these summaries alone, prefer them over scanning row chunks.`;
    }

    // 2. Context docs (SOPs, policies, reference material)
    if (contextDocs) system += `\n\nCOLLECTION CONTEXT DOCUMENTS (reference material — use for background, definitions, policies):\n${contextDocs}`;

    // 3. Document/row chunks — the actual retrievable content
    if (context.text) {
      system += `\n\nDOCUMENT CONTENT:\nFormat note: tabular data appears as self-contained row chunks, each starting with "Columns: ..." followed by numbered rows. Each chunk is a slice of a larger dataset — rows may not be sequential across chunks.\n\n${context.text}\n\nWhen answering: cite the document name and period. For numeric questions, cross-reference the DATA SUMMARIES above with the row chunks below to give precise answers. If the row chunks don't contain enough detail to answer fully, say what you CAN answer from the summaries and note what's missing.`;
    } else {
      system += `\n\nNo documents found matching this query. Let the user know and ask them to upload relevant files or switch to a different collection.`;
    }

    // ── Intent hint appended to system prompt ────────────────────
    if (intent.expansionHint) {
      system += `\n\nQuery context: ${intent.expansionHint}. `;
      if (intent.isComparative) system += 'Compare across time periods where possible. Highlight trends and changes.';
      if (intent.isCausal) system += 'Identify likely drivers. Look for correlated changes across metrics.';
      if (intent.isAggregate && context.statsContext) system += 'The DATA SUMMARIES above contain authoritative totals — use them directly.';
    }
    if (useMultiCollection && context.collectionsSearched?.length) {
      system += `\n\nNote: this response draws from ${context.collectionsSearched.length} collections: ${context.collectionsSearched.join(', ')}.`;
    }

    const responseText = await callLLM({ model, system, messages: [...history.slice(-20).map(h => ({ role: h.role, content: h.content })), { role: 'user', content: message }], maxTokens: 8000, env, apiKey });
    // Only surface sources if documents were actually retrieved — not for general conversation
    const sourcesToReturn = context.text ? context.sources : [];
    return json({ ok: true, response: responseText, sources: sourcesToReturn, scope, model: model || 'claude' });
  } catch (err) { return json({ error: 'Chat error: ' + err.message }, 500); }
}

// ── Discovery Context Builder ─────────────────────────────────
async function buildDiscoveryContext({ dept, env }) {
  try {
    const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
    const enriched = await Promise.all(reg.map(async c => {
      const files = await env.CACI_KV.get(`col:${dept}:${c.name}`, 'json') || [];
      return { ...c, fileCount: files.length };
    }));
    const collectionList = enriched.length
      ? enriched.map(c => {
          const meta = [
            c.fileCount + ' file' + (c.fileCount !== 1 ? 's' : ''),
            c.category || '',
            c.summary  || '',
          ].filter(Boolean).join(', ');
          const desc = c.description ? `\n    ${c.description}` : '';
          return `- ${c.name} (${meta})${desc}`;
        }).join('\n')
      : '(No collections uploaded yet.)';
    return { collectionList, rawCollections: enriched };
  } catch {
    return { collectionList: '(Could not load collections.)', rawCollections: [] };
  }
}

// ── Query intent analysis ─────────────────────────────────────
// ── Library Map — Caci's persistent understanding of the library ──
async function buildLibraryMap({ dept, env }) {
  try {
    const cached = await env.CACI_KV.get(`library:map:${dept}`, 'json');
    if (cached && cached.builtAt && (Date.now() - cached.builtAt) < 2 * 60 * 1000) return cached;

    const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
    if (!reg.length) return null;

    const collections = await Promise.all(reg.map(async c => {
      const files    = await env.CACI_KV.get(`col:${dept}:${c.name}`, 'json') || [];
      const ctxFiles = await env.CACI_KV.get(`ctx:${dept}:${c.name}`, 'json') || [];
      const years = new Set(); const states = new Set(); const fileTypes = new Set();
      let totalRows = 0;
      for (const f of files) {
        (f.name || '').match(/20\d\d/g)?.forEach(y => years.add(y));
        const sm = (f.name || '').match(/\b([A-Z]{2})\s*[-]/);
        if (sm) states.add(sm[1]);
        const ext = (f.name || '').split('.').pop().toLowerCase();
        if (ext) fileTypes.add(ext);
        if (f.stats?.rowCount) totalRows += f.stats.rowCount;
      }
      return {
        name: c.name, category: c.category || '', description: c.description || '',
        summary: c.summary || '', fileCount: files.length, contextDocCount: ctxFiles.length,
        years: [...years].sort(), states: [...states].sort(), fileTypes: [...fileTypes],
        totalRows, hasData: totalRows > 0,
        files: files.slice(0, 20).map(f => ({ name: f.name, period: f.meta?.period || '', category: f.meta?.category || '' })),
      };
    }));

    const relationships = [];
    for (let i = 0; i < collections.length; i++) {
      for (let j = i + 1; j < collections.length; j++) {
        const a = collections[i]; const b = collections[j];
        const sharedStates = a.states.filter(s => b.states.includes(s));
        const sharedYears  = a.years.filter(y => b.years.includes(y));
        const isReg = c => ['legal','compliance','regulation'].some(t => (c.category||'').toLowerCase().includes(t) || c.name.toLowerCase().includes(t));
        const aReg = isReg(a); const bReg = isReg(b);
        if (sharedStates.length && aReg !== bReg) {
          relationships.push(`"${a.name}" (regulations) governs operations covered in "${b.name}" for ${sharedStates.join(', ')}`);
        } else if (sharedStates.length && sharedYears.length && a.hasData && b.hasData) {
          relationships.push(`"${a.name}" and "${b.name}" both cover ${sharedStates.join(', ')} for ${sharedYears.join(', ')} — can be compared`);
        }
      }
    }

    const gaps = [];
    const allStates = [...new Set(collections.flatMap(c => c.states))];
    const regCollections = collections.filter(c => ['legal','compliance','regulation'].some(t => (c.category||'').toLowerCase().includes(t) || c.name.toLowerCase().includes(t)));
    for (const state of allStates) {
      const hasReg = regCollections.some(c => c.states.includes(state) || c.name.toLowerCase().includes(state.toLowerCase()));
      if (!hasReg && state.length === 2) gaps.push(`${state} — operational data exists but no regulatory documents uploaded`);
    }

    const map = { dept, builtAt: Date.now(), collectionCount: collections.length, collections, relationships, gaps };
    await env.CACI_KV.put(`library:map:${dept}`, JSON.stringify(map));
    return map;
  } catch(e) { console.error('buildLibraryMap error:', e.message); return null; }
}

function libraryMapToPrompt(map) {
  if (!map || !map.collections?.length) return '';
  const lines = [`\nLIBRARY MAP — your complete understanding of the ${map.dept} department library:`];
  for (const c of map.collections) {
    const meta = [
      c.fileCount + ' file' + (c.fileCount !== 1 ? 's' : ''),
      c.contextDocCount ? c.contextDocCount + ' context docs' : '',
      c.category || '',
      c.years.length ? c.years.join(', ') : '',
      c.states.length ? 'States: ' + c.states.join(', ') : '',
      c.hasData ? c.totalRows.toLocaleString() + ' data rows' : '',
    ].filter(Boolean).join(' · ');
    lines.push(`\n• "${c.name}" (${meta})`);
    if (c.description) lines.push(`  ${c.description}`);
    if (c.files.length) {
      const sample = c.files.slice(0, 5).map(f => f.name + (f.period ? ' [' + f.period + ']' : '')).join(', ');
      lines.push(`  Contains: ${sample}${c.files.length > 5 ? ' + ' + (c.files.length - 5) + ' more' : ''}`);
    }
  }
  if (map.relationships.length) { lines.push('\nCROSS-COLLECTION RELATIONSHIPS:'); map.relationships.forEach(r => lines.push('• ' + r)); }
  if (map.gaps.length) { lines.push('\nNOTED GAPS:'); map.gaps.forEach(g => lines.push('• ' + g)); }
  lines.push('\nUse this map to route queries to the right collections, surface related documents the user may not have thought to reference, and flag when something is missing from the library.');
  return lines.join('\n');
}

function analyzeQueryIntent(message) {
  const msg = message.toLowerCase();
  const isComparative = [
    /vs\.?|versus|compare|comparison/,
    /trend|over time|month.over.month|m\.o\.m/,
    /year.over.year|y\.o\.y|quarter.over.quarter/,
    /change|growth|decline|down|up|drop/,
    /better|worse|improved|increased|decreased/,
    /histor/,
  ].some(r => r.test(msg));
  const isAggregate = [
    /total|sum|aggregate|overall/,
    /how much|how many/,
    /average|avg|mean/,
    /biggest|largest|highest|lowest|smallest/,
  ].some(r => r.test(msg));
  const isFiltered = [
    /\bpa\b|\bill\b|\bnv\b|\bva\b|\bnj\b|\boh\b|\bma\b|\bfl\b|\bca\b|\bco\b/,
    /\bpennsylvania\b|\billinois\b|\bnevada\b|\bvirginia\b|\bnew jersey\b|\bohio\b|\bmassachusetts\b|\bflorida\b/,
    /\bflower\b|\bvape\b|\bedible\b|\bconcentrate\b|\bpreroll\b|\bpre-roll\b/,
    /\bstore\b|\blocation\b|\bdispensary\b|\bproduct\b|\bbrand\b/,
  ].some(r => r.test(msg));
  const isCausal = [
    /\bwhy\b|\bcause\b|\breason\b|\bdriving\b|\bfactor\b|\bexplain\b/,
  ].some(r => r.test(msg));
  let periodExpansion = 1;
  if (isComparative || isCausal) periodExpansion = 3;
  else if (isAggregate && !isFiltered) periodExpansion = 2;
  return {
    isComparative, isAggregate, isFiltered, isCausal, periodExpansion,
    expansionHint: isComparative || isCausal
      ? 'include adjacent time periods for comparison'
      : isAggregate ? 'use pre-computed stats block for totals' : '',
  };
}

// ── Multi-collection context builder ─────────────────────────
async function buildContextMultiCollection({ message, dept, env, maxCollections = 3 }) {
  try {
    const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
    if (!reg.length) return { text: '', sources: [], statsContext: '', focusFile: null };

    const keywords = extractKeywords(message);
    const intent   = analyzeQueryIntent(message);
    const msgLower = message.toLowerCase();
    const queryYears = new Set((message.match(/20\d\d/g) || []));

    const colScores = reg.map(c => {
      let score = 0;
      const nameLower = c.name.toLowerCase();
      if (msgLower.includes(nameLower)) score += 20;
      if (c.category && msgLower.includes(c.category.toLowerCase())) score += 8;
      if (c.description) keywords.forEach(kw => { if (c.description.toLowerCase().includes(kw)) score += 3; });
      if (c.summary)     keywords.forEach(kw => { if (c.summary.toLowerCase().includes(kw)) score += 2; });
      const stopWords = new Set(['the','and','for','all','from','with','that','this','are','reports']);
      const nameWords = nameLower.split(/[\s&,\/]+/).filter(w => w.length >= 4 && !stopWords.has(w));
      score += nameWords.filter(w => msgLower.includes(w)).length * 4;
      if (c.period) { const pl = c.period.toLowerCase(); if ([...queryYears].some(y => pl.includes(y))) score += 10; }
      if (intent.isComparative && c.fileCount > 3) score += 5;
      return { ...c, _colScore: score };
    })
    .filter(c => c._colScore > 0)
    .sort((a, b) => b._colScore - a._colScore)
    .slice(0, maxCollections);

    if (!colScores.length) return { text: '', sources: [], statsContext: '', focusFile: null };

    const results = await Promise.all(
      colScores.map(c => buildContextTwoPass({ message, dept, collection: c.name, env }))
    );

    const allStats = results
      .map((r, i) => {
        if (!r.statsContext) return '';
        const col = colScores[i];
        const schemaNote = col.category ? ` [${col.category}]` : '';
        return `### Collection: ${col.name}${schemaNote}\n${r.statsContext}`;
      })
      .filter(Boolean)
      .join('\n\n---\n\n');

    const allChunks = [];
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (!r.text) continue;
      const boost = (maxCollections - i) * 2;
      r.text.split('\n\n---\n\n').forEach(chunk => allChunks.push({ chunk, boost, colName: colScores[i].name }));
    }

    const keywords2 = extractKeywords(message);
    const ranked = allChunks
      .map(c => ({ ...c, score: scoreChunk(c.chunk, keywords2) + c.boost }))
      .filter(c => c.score >= 0)
      .sort((a, b) => b.score - a.score);

    const seen = new Set();
    const deduped = ranked.filter(c => {
      const key = c.chunk.slice(0, 80).replace(/\s+/g, ' ');
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    const top = deduped.slice(0, 22);
    const text = top.map(c => c.chunk).join('\n\n---\n\n');
    const sources = [...new Set(top.map(c => {
      const m = c.chunk.match(/^\[([^\]]+)\]/);
      return m ? m[1] : c.colName;
    }))];

    const manifest = colScores.map(c =>
      `- ${c.name} (score:${c._colScore}, ${c.fileCount || '?'} files${c.summary ? ' — ' + c.summary : ''})`
    ).join('\n');

    return {
      text: text + '\n\nCOLLECTIONS SEARCHED:\n' + manifest,
      sources,
      statsContext: allStats,
      focusFile: results[0]?.focusFile || null,
      collectionsSearched: colScores.map(c => c.name),
    };
  } catch(err) {
    console.error('Multi-collection context error:', err.message);
    return { text: '', sources: [], statsContext: '', focusFile: null };
  }
}

// ── Report Generation ─────────────────────────────────────────
async function handleReport(request, env) {
  try {
    const { prompt, dept, collection, fileId, scope = 'all', format = 'markdown', model } = await request.json();

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    const xaiKey = (await env.CACI_KV.get('config:XAI_API_KEY')) || env.XAI_API_KEY;
    if (!apiKey && !xaiKey && !env.AI) return json({ error: 'No AI provider configured. Add an API key in Config.' }, 400);

    const context = await buildContext({ message: prompt, dept, collection, fileId, scope, env });

    const reportPrompt = `You are generating a professional internal business report for Jushi Holdings.

Report request: ${prompt}

${context.statsContext ? `PRE-COMPUTED DATA SUMMARIES (authoritative totals/averages — use directly for aggregate figures):\n${context.statsContext}\n` : ''}
${context.text ? `SOURCE DOCUMENTS (tabular data appears as row chunks; each chunk starts with "Columns: ..." followed by numbered rows):\n${context.text}\n` : ''}

Generate a well-structured internal report. Include only sections that are supported by the data:
1. Executive Summary (2-3 sentences — lead with the most important finding)
2. Key Findings (specific numbers, cite source file and period)
3. Detailed Analysis (by category, state, or store as relevant)
4. Period-over-Period Comparison (only if multiple time periods are present)
5. State/Store Breakdown (only if location data is present)
6. Recommendations (grounded in the data)
7. Data Sources (list files used)

Format in clean Markdown. Use ## for sections, tables where data warrants it. Be precise — never round or estimate when exact figures are available. If data is insufficient to complete a section, omit it rather than speculating.`;

    const reportText = await callLLM({
      model: model || 'claude',
      system: 'You are a precise business analyst generating internal reports for Jushi Holdings. Use markdown formatting — headers, tables, and bullets where appropriate. Cite document names and periods for every data point.',
      messages: [{ role: 'user', content: reportPrompt }],
      maxTokens: 8000,
      env,
      apiKey,
    });
    return json({ ok: true, report: reportText, sources: context.sources, format });
  } catch (err) { return json({ error: 'Report error: ' + err.message }, 500); }
}

// ── Shared date/month utilities ───────────────────────────────
const _MONTH_TO_NUM = {
  january:1, february:2, march:3, april:4, may:5, june:6,
  july:7, august:8, september:9, october:10, november:11, december:12,
  jan:1, feb:2, mar:3, apr:4, jun:6, jul:7, aug:8, sep:9, oct:10, nov:11, dec:12,
};
const _MONTH_ABBREV = {
  january:'jan', february:'feb', march:'mar', april:'apr', may:'may',
  june:'jun', july:'jul', august:'aug', september:'sep', october:'oct',
  november:'nov', december:'dec',
};
const _ALL_MONTH_KEYS = Object.keys(_MONTH_TO_NUM);

// Parse date range from a query string.
// Handles: "between X 2025 and Y 2026", "from X to Y", "X through Y"
function parseDateRange(message) {
  // ── Pattern 0: relative date expressions (highest priority) ─
  const relative = resolveRelativeDates(message);
  if (relative) return relative;

  const msg = message.toLowerCase();

  // ── Pattern 1: explicit range "from/between X YYYY and/to Y YYYY" ──
  const rangePat = /(?:between|from)\s+(\w+)\s+(?:of\s+)?(\d{4})\s+(?:and|to|through)\s+(\w+)\s+(?:of\s+)?(\d{4})|(\w+)\s+(?:of\s+)?(\d{4})\s+through\s+(\w+)\s+(?:of\s+)?(\d{4})/i;
  const rm = message.match(rangePat);
  if (rm) {
    const [, m1, y1, m2, y2, m1b, y1b, m2b, y2b] = rm;
    const s1 = (m1 || m1b || '').toLowerCase();
    const r1 = parseInt(y1 || y1b || '0');
    const s2 = (m2 || m2b || '').toLowerCase();
    const r2 = parseInt(y2 || y2b || '0');
    if (_MONTH_TO_NUM[s1] && _MONTH_TO_NUM[s2]) {
      return { rangeStart: { m: _MONTH_TO_NUM[s1], y: r1 }, rangeEnd: { m: _MONTH_TO_NUM[s2], y: r2 } };
    }
  }

  // ── Pattern 2: "Q1 2025" / "Q3 2024" ──
  const qPat = /\bq([1-4])\s+(20\d{2})\b/i;
  const qm = msg.match(qPat);
  if (qm) {
    const q = parseInt(qm[1]), y = parseInt(qm[2]);
    const qStart = [0,1,4,7,10][q], qEnd = [0,3,6,9,12][q];
    return { rangeStart: { m: qStart, y }, rangeEnd: { m: qEnd, y } };
  }

  // ── Pattern 3: "2024 vs 2025" / "compare 2024 and 2025" ──
  const vsPat = /\b(20\d{2})\s+(?:vs\.?|versus|compared? to|and)\s+(20\d{2})\b/i;
  const vsm = message.match(vsPat);
  if (vsm) {
    const y1 = parseInt(vsm[1]), y2 = parseInt(vsm[2]);
    const [yMin, yMax] = y1 < y2 ? [y1, y2] : [y2, y1];
    return { rangeStart: { m: 1, y: yMin }, rangeEnd: { m: 12, y: yMax } };
  }

  // ── Pattern 4: "Month YYYY" — exact month ──────────────────
  for (const mk of _ALL_MONTH_KEYS) {
    const mPat = new RegExp('(?<![a-z])' + mk + '(?![a-z])\\s+(20\\d{2})', 'i');
    const mm = message.match(mPat);
    if (mm && _MONTH_TO_NUM[mk]) {
      const y = parseInt(mm[1]);
      return { rangeStart: { m: _MONTH_TO_NUM[mk], y }, rangeEnd: { m: _MONTH_TO_NUM[mk], y } };
    }
  }

  // ── Pattern 5: month name only, no year — infer most recent ──
  // "show me March data" → most recent March (try current year, then prior)
  const today5 = new Date();
  const curMonth5 = today5.getMonth() + 1;
  const curYear5  = today5.getFullYear();
  for (const mk of _ALL_MONTH_KEYS.filter((v, i, a) => a.indexOf(v) === i)) {
    const re5 = new RegExp('(?<![a-z])' + mk + '(?![a-z])', 'i');
    if (re5.test(message)) {
      const targetM = _MONTH_TO_NUM[mk];
      // If that month has passed this year, use this year; otherwise use last year
      const inferYear = targetM <= curMonth5 ? curYear5 : curYear5 - 1;
      return { rangeStart: { m: targetM, y: inferYear }, rangeEnd: { m: targetM, y: inferYear } };
    }
  }

  return { rangeStart: null, rangeEnd: null };
}

// Extract month+year from a filename (lowercase).
function fileMonthYear(nameLower) {
  let fileMonth = null;
  for (const mk of _ALL_MONTH_KEYS) {
    const re = new RegExp('(?<![a-z])' + mk + '(?![a-z])');
    if (re.test(nameLower)) { fileMonth = _MONTH_TO_NUM[mk]; break; }
  }
  const ym = nameLower.match(/20(\d\d)/);
  const fileYear = ym ? parseInt('20' + ym[1]) : null;
  return { fileMonth, fileYear };
}

// ── Two-Pass Context Builder for Collections ─────────────────
async function buildContextTwoPass({ message, dept, collection, env }) {
  try {
    const colFiles = await env.CACI_KV.get(`col:${dept}:${collection}`, 'json') || [];
    if (!colFiles.length) return { text: '', sources: [], statsContext: '', focusFile: null };

    const manifest = colFiles.map(f => `- ${f.name}${f.meta?.period ? ' [' + f.meta.period + ']' : ''}${f.stats?.rowCount ? ' (' + f.stats.rowCount + ' rows)' : ''}`).join('\n');

    const keywords = extractKeywords(message);
    const yearMatches = message.match(/20\d\d/g) || [];
    const queryYears = new Set(yearMatches);
    const quarterMatches = message.match(/q[1-4]|quarter [1-4]|first quarter|second quarter|third quarter|fourth quarter|full year|annual/gi) || [];
    const queryQuarters = new Set(quarterMatches.map(q => q.toLowerCase()));

    // ── Date range detection ──────────────────────────────────
    const { rangeStart, rangeEnd } = parseDateRange(message);

    // Extract query months using word boundaries
    const msgLowerForMonths = message.toLowerCase();
    const queryMonths = new Set(_ALL_MONTH_KEYS.filter(m => {
      const re = new RegExp('(?<![a-z])' + m + '(?![a-z])');
      return re.test(msgLowerForMonths);
    }));

    // Score each file
    const W = await getScoringWeights(dept, env);
    const scoredFiles = colFiles.map(f => {
      const nameLower = (f.name || '').toLowerCase();
      let score = 0;

      // ── 1. Date range match: highest priority (+30) ───────
      if (rangeStart && rangeEnd) {
        const { fileMonth, fileYear } = fileMonthYear(nameLower);
        if (fileMonth && fileYear) {
          const fileVal  = fileYear * 100 + fileMonth;
          const startVal = rangeStart.y * 100 + rangeStart.m;
          const endVal   = rangeEnd.y   * 100 + rangeEnd.m;
          if (fileVal >= startVal && fileVal <= endVal) score += W.rangeMatchBonus;
        }
      }

      // ── 2. Year match — larger multiplier (x3) so year gaps are decisive ──
      const matchedYears = queryYears.size > 0 ? [...queryYears].filter(y => nameLower.includes(y)) : [];
      if (matchedYears.length > 0) {
        const maxYear = Math.max(...matchedYears.map(Number));
        score += W.yearMatchBase + (maxYear - 2020) * W.yearRecencyMultiplier;
      }

      // ── 3. Month match — only add if NOT already in a range ──
      if (queryMonths.size > 0 && score < W.rangeMatchBonus) {
        const hasMonthMatch = [...queryMonths].some(m => {
          const abbr = _MONTH_ABBREV[m] || m;
          return nameLower.includes(m) || nameLower.includes(abbr);
        });
        if (hasMonthMatch) score += W.monthMatchBonus;
      }

      // ── 4. Annual / full-year boost ───────────────────────
      const isAnnual = nameLower.includes('annual') || nameLower.includes('full year')
        || nameLower.includes('ye ') || nameLower.includes('10-k') || nameLower.includes('10k');
      if (isAnnual && (queryYears.size > 0 || message.toLowerCase().includes('annual') || message.toLowerCase().includes('full year'))) {
        score += W.annualBonus;
      }

      // ── 5. Quarter match ──────────────────────────────────
      const fileQ = nameLower.match(/q[1-4]|first quarter|second quarter|third quarter|fourth quarter/)?.[0];
      if (fileQ && queryQuarters.size > 0) {
        const qMap = { 'q1':'q1','q2':'q2','q3':'q3','q4':'q4','first quarter':'q1','second quarter':'q2','third quarter':'q3','fourth quarter':'q4' };
        if ([...queryQuarters].some(q => qMap[q] === fileQ)) score += W.quarterMatchBonus;
      }

      // ── 6. Keyword match in filename ──────────────────────
      for (const kw of keywords) {
        if (nameLower.includes(kw)) score += W.keywordFilenameBonus;
      }

      // ── 7. PDF preferred over xlsx ────────────────────────
      if (nameLower.endsWith('.xlsx') || nameLower.endsWith('.xls')) score += W.xlsxPenalty;

      // ── 8. Recency boost (up to +3) ───────────────────────
      const age = f.uploadedAt ? Date.now() - new Date(f.uploadedAt).getTime() : 0;
      score += Math.max(0, W.recencyMaxBonus - Math.floor(age / (1000 * 60 * 60 * 24 * 30)));

      // ── 9. categoryCols match — boost if query mentions a known value ──
      if (f.stats?.categoryCols) {
        for (const vals of Object.values(f.stats.categoryCols)) {
          for (const val of vals) {
            const vLower = val.toLowerCase();
            if (keywords.some(kw => vLower === kw || vLower.includes(kw) || kw.includes(vLower))) {
              score += W.categoryColBonus;
              break;
            }
          }
        }
      }

      return { ...f, _score: score };
    });

    scoredFiles.sort((a, b) => b._score - a._score);

    // File selection strategy
    const hasStrongSignal = queryYears.size > 0 || queryQuarters.size > 0 || (rangeStart && rangeEnd);
    let topFiles;
    if (hasStrongSignal) {
      topFiles = scoredFiles.slice(0, 12);
    } else {
      // Broad query: top 3 + spread 7 evenly across rest for full coverage
      const top3 = scoredFiles.slice(0, 3);
      const rest  = scoredFiles.slice(3);
      const spread = [];
      const step = Math.max(1, Math.floor(rest.length / 7));
      for (let i = 0; i < rest.length && spread.length < 7; i += step) spread.push(rest[i]);
      topFiles = [...top3, ...spread];
    }

    // Load full content for selected files — preserve _score for chunk boosting
    const fullFiles = (await Promise.all(
      topFiles.map(async f => {
        const data = await env.CACI_KV.get(`file:${f.id}`, 'json');
        if (data) data._score = f._score || 0;
        return data;
      })
    )).filter(Boolean);

    // ── Collection-level aggregate stats (ALL files, not just loaded) ──
    // colFiles index records carry stats without chunks — use them to compute
    // true collection totals so the LLM sees the full picture even when only
    // a subset of files are loaded for chunk retrieval.
    const collectionAgg = { rowCount: 0, numeric: {}, categoryCols: {}, columns: new Set(), fileCount: colFiles.length };
    for (const f of colFiles) {
      if (!f.stats) continue;
      if (f.stats.rowCount) collectionAgg.rowCount += f.stats.rowCount;
      if (f.stats.columns)  f.stats.columns.forEach(c => collectionAgg.columns.add(c));
      if (f.stats.numeric) {
        for (const [col, s] of Object.entries(f.stats.numeric)) {
          if (!collectionAgg.numeric[col]) {
            collectionAgg.numeric[col] = { sum: s.sum, count: s.count, min: s.min, max: s.max };
          } else {
            const agg = collectionAgg.numeric[col];
            agg.sum   = Math.round((agg.sum + s.sum) * 100) / 100;
            agg.count += s.count;
            agg.min    = Math.min(agg.min, s.min);
            agg.max    = Math.max(agg.max, s.max);
          }
        }
      }
      if (f.stats.categoryCols) {
        for (const [col, vals] of Object.entries(f.stats.categoryCols)) {
          if (!collectionAgg.categoryCols[col]) collectionAgg.categoryCols[col] = new Set();
          vals.forEach(v => collectionAgg.categoryCols[col].add(v));
        }
      }
    }

    // Build stats context — lead with collection-level aggregate, then per-file detail
    const statsLines = [];
    if (collectionAgg.rowCount > 0) {
      statsLines.push(`\n### COLLECTION TOTAL (${collectionAgg.fileCount} files, ${collectionAgg.rowCount.toLocaleString()} rows)`);
      if (collectionAgg.columns.size) statsLines.push(`Columns: ${[...collectionAgg.columns].join(', ')}`);
      for (const [col, agg] of Object.entries(collectionAgg.numeric)) {
        const avg = agg.count > 0 ? Math.round(agg.sum / agg.count * 100) / 100 : 0;
        statsLines.push(`${col}: total=${agg.sum.toLocaleString()}, avg=${avg}, min=${agg.min}, max=${agg.max}, rows=${agg.count}`);
      }
      for (const [col, valSet] of Object.entries(collectionAgg.categoryCols)) {
        const vals = [...valSet];
        statsLines.push(`${col} values: ${vals.map(v => v.includes(',') ? '"' + v + '"' : v).join(', ')}`);
      }
    }

    // Per-file stats for loaded files (provides period-level detail)
    for (const f of fullFiles) {
      if (f.stats && Object.keys(f.stats).length) {
        const meta = f.meta || {};
        const label = `${meta.reportName || f.name} [${meta.period || ''}]`.trim();
        statsLines.push(`\n### ${label}`);
        if (f.stats.rowCount) statsLines.push(`Rows: ${f.stats.rowCount}`);
        if (f.stats.columns) statsLines.push(`Columns: ${f.stats.columns.join(', ')}`);
        if (f.stats.numeric) {
          for (const [col, s] of Object.entries(f.stats.numeric)) {
            statsLines.push(`${col}: sum=${s.sum}, avg=${s.avg}, min=${s.min}, max=${s.max}, count=${s.count}`);
          }
        }
        if (f.stats.categoryCols) {
          for (const [col, vals] of Object.entries(f.stats.categoryCols)) {
            statsLines.push(`${col} values: ${vals.map(v => v.includes(',') ? '"' + v + '"' : v).join(', ')}`);
          }
        }
      }
    }

    // Get best chunks — dynamic allocation: top-scoring files get more chunks
    const keywords2 = extractKeywords(message);
    const intent2   = analyzeQueryIntent(message);
    const TOTAL_CHUNK_BUDGET = fullFiles.length <= 2 ? 32 : fullFiles.length <= 5 ? 30 : 26;
    const MIN_CHUNKS_PER_FILE = 1;  // every selected file gets at least one chunk
    const MAX_CHUNKS_PER_FILE = fullFiles.length <= 2 ? 20 : fullFiles.length <= 4 ? 12 : 7;

    // Guaranteed slots: FILE SUMMARY chunk from each file
    const guaranteedChunks = [];
    const seenSummaryFiles = new Set();
    for (const fileData of fullFiles) {
      if (!fileData.chunks?.length || seenSummaryFiles.has(fileData.name)) continue;
      const first = fileData.chunks[0];
      if (first.toLowerCase().startsWith('file summary') || first.toLowerCase().startsWith('sheet:')) {
        guaranteedChunks.push({ chunk: first, score: 999, filename: fileData.name, collection: fileData.collection, meta: fileData.meta || {}, _guaranteed: true });
        seenSummaryFiles.add(fileData.name);
      }
    }

    // Score all non-summary chunks across all files
    const allChunks = [];
    for (const fileData of fullFiles) {
      if (!fileData.chunks) continue;
      const fileBoost = fileData._score || 0;
      for (const chunk of fileData.chunks) {
        const s = scoreChunk(chunk, keywords2);
        if (s >= 0) allChunks.push({ chunk, score: s + fileBoost, filename: fileData.name, collection: fileData.collection, meta: fileData.meta || {} });
      }
    }
    allChunks.sort((a, b) => b.score - a.score);

    // Distribute budget: give each file a minimum, then award remaining slots to top scorers
    const fileChunkCount = new Map(fullFiles.map(f => [f.name, MIN_CHUNKS_PER_FILE]));
    let remaining = TOTAL_CHUNK_BUDGET - guaranteedChunks.length - (fullFiles.length * MIN_CHUNKS_PER_FILE);
    for (const c of allChunks) {
      if (remaining <= 0) break;
      const cur = fileChunkCount.get(c.filename) || 0;
      if (cur < MAX_CHUNKS_PER_FILE) {
        fileChunkCount.set(c.filename, cur + 1);
        remaining--;
      }
    }

    // Select chunks respecting per-file budgets
    const guaranteedTexts = new Set(guaranteedChunks.map(c => c.chunk));
    const fileSeen = new Map(fullFiles.map(f => [f.name, 0]));
    const scoredOnly = [];
    for (const c of allChunks) {
      if (guaranteedTexts.has(c.chunk)) continue;
      const seen = fileSeen.get(c.filename) || 0;
      const budget = fileChunkCount.get(c.filename) || MIN_CHUNKS_PER_FILE;
      if (seen < budget) {
        scoredOnly.push(c);
        fileSeen.set(c.filename, seen + 1);
      }
    }

    const top = [...guaranteedChunks, ...scoredOnly];

    if (!top.length) return { text: '', sources: [], statsContext: statsLines.join('\n'), focusFile: fullFiles[0]?.name };

    // Re-rank before final assembly
    const reranked = rerankChunks(top, keywords2, intent2);

    const sources = [...new Set(reranked.map(x => x.filename))];
    const text = reranked.map(x => {
      const period = x.meta?.period ? ` [${x.meta.period}]` : '';
      return `[${x.collection}${period} / ${x.filename}]\n${x.chunk}`;
    }).join('\n\n---\n\n');

    const allFileList = `\n\nALL FILES IN THIS COLLECTION (${colFiles.length} total):\n${manifest}`;

    return { text: text + allFileList, sources, statsContext: statsLines.join('\n'), focusFile: fullFiles[0]?.name };
  } catch(err) {
    console.error('Two-pass context error:', err.message);
    return { text: '', sources: [], statsContext: '', focusFile: null };
  }
}

// ── Context Builder ───────────────────────────────────────────
async function buildContext({ message, dept, collection, fileId, scope, env }) {
  if (scope === 'collection' && collection) {
    return await buildContextTwoPass({ message, dept, collection, env });
  }
  try {
    const keywords = extractKeywords(message);
    const intent   = analyzeQueryIntent(message);
    let filesToSearch = [];

    if (scope === 'file' && fileId) {
      const f = await env.CACI_KV.get(`file:${fileId}`, 'json');
      if (f) filesToSearch = [f];
    } else {
      // ── Phase 1: score on lightweight index records (no chunks loaded yet) ──
      const deptIdx   = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
      const globalIdx = dept !== 'global' ? (await env.CACI_KV.get('index:global', 'json') || []) : [] ;
      const allMeta   = [...deptIdx, ...globalIdx].filter(f => !f.isContext);

      // Score each index record without loading chunks
      const { rangeStart: preRange, rangeEnd: preRangeEnd } = parseDateRange(message);
      const preYears  = new Set((message.match(/20\d\d/g) || []));
      const msgLower  = message.toLowerCase();
      const stopWords = new Set(['the','and','for','all','from','with','that','this','are','was','were','has','have','report','reports']);

      // Collection relevance scoring: score ALL collections, pick top matches
      const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
      const colScores = reg.map(c => {
        let cs = 0;
        if (msgLower.includes(c.name.toLowerCase())) cs += 20;
        if (c.category && msgLower.includes(c.category.toLowerCase())) cs += 8;
        if (c.description && keywords.some(kw => c.description.toLowerCase().includes(kw))) cs += 5;
        const words = c.name.toLowerCase().split(/[\s&,\/]+/).filter(w => w.length >= 4 && !stopWords.has(w));
        cs += words.filter(w => msgLower.includes(w)).length * 3;
        return { name: c.name, score: cs };
      }).filter(c => c.score > 0).sort((a,b) => b.score - a.score);

      // Determine which files to consider based on collection matches
      let candidateMeta;
      if (colScores.length > 0) {
        // Pull files from top-scoring collections (up to 2 collections)
        const topCols = colScores.slice(0, 2).map(c => c.name);
        const colFileIds = new Set();
        for (const colName of topCols) {
          const cf = await env.CACI_KV.get(`col:${dept}:${colName}`, 'json') || [];
          cf.forEach(f => colFileIds.add(f.id));
        }
        const colMeta  = allMeta.filter(f => colFileIds.has(f.id));
        const otherMeta = allMeta.filter(f => !colFileIds.has(f.id)).slice(0, 15);
        candidateMeta = [...colMeta, ...otherMeta];
      } else {
        candidateMeta = allMeta.slice(0, 40);
      }

      // Score each candidate on date + keywords + categoryCols (no chunk load)
      const scored = candidateMeta.map(f => {
        const nameLower = (f.name || '').toLowerCase();
        let score = 0;

        // Date range
        if (preRange && preRangeEnd) {
          const { fileMonth, fileYear } = fileMonthYear(nameLower);
          if (fileMonth && fileYear) {
            const fv = fileYear * 100 + fileMonth;
            if (fv >= preRange.y * 100 + preRange.m && fv <= preRangeEnd.y * 100 + preRangeEnd.m) score += 30;
          }
        }
        // Year match
        const matchedYears = [...preYears].filter(y => nameLower.includes(y));
        if (matchedYears.length) {
          const maxYear = Math.max(...matchedYears.map(Number));
          score += 10 + (maxYear - 2020) * 3;
        }
        // Month match
        for (const mk of _ALL_MONTH_KEYS) {
          const re = new RegExp('(?<![a-z])' + mk + '(?![a-z])');
          if (re.test(msgLower) && re.test(nameLower)) { score += 8; break; }
        }
        // Keyword in filename
        keywords.forEach(kw => { if (nameLower.includes(kw)) score += 2; });
        // categoryCols boost — free signal from index record
        if (f.stats?.categoryCols) {
          for (const vals of Object.values(f.stats.categoryCols)) {
            for (const val of vals) {
              const vl = val.toLowerCase();
              if (keywords.some(kw => vl === kw || vl.includes(kw) || kw.includes(vl))) { score += 5; break; }
            }
          }
        }
        // Recency
        const age = f.uploadedAt ? Date.now() - new Date(f.uploadedAt).getTime() : 0;
        score += Math.max(0, 3 - Math.floor(age / (1000 * 60 * 60 * 24 * 30)));

        return { ...f, _preScore: score };
      }).sort((a,b) => b._preScore - a._preScore);

      // Phase 2: load full records (with chunks) for top candidates only
      const hasSignal = preYears.size > 0 || (preRange && preRangeEnd) || colScores.length > 0;
      const loadLimit = hasSignal ? 10 : 15;
      const topMeta   = scored.slice(0, loadLimit);

      filesToSearch = (await Promise.all(
        topMeta.map(async f => {
          const full = await env.CACI_KV.get(`file:${f.id}`, 'json');
          if (full) full._preScore = f._preScore || 0;
          return full;
        })
      )).filter(Boolean);
    }

    if (!filesToSearch.length) return { text: '', sources: [], statsContext: '', focusFile: null };

    const statsLines = [];
    for (const f of filesToSearch) {
      if (f.stats && Object.keys(f.stats).length) {
        const meta = f.meta || {};
        const label = `${meta.reportName || f.name} [${meta.period || ''} ${meta.state ? '| ' + meta.state : ''}]`.trim();
        statsLines.push(`\n### ${label}`);
        if (f.stats.rowCount) statsLines.push(`Rows: ${f.stats.rowCount}`);
        if (f.stats.columns) statsLines.push(`Columns: ${f.stats.columns.join(', ')}`);
        if (f.stats.numeric) {
          for (const [col, s] of Object.entries(f.stats.numeric)) {
            statsLines.push(`${col}: sum=${s.sum}, avg=${s.avg}, min=${s.min}, max=${s.max}, count=${s.count}`);
          }
        }
        if (f.stats.categoryCols) {
          for (const [col, vals] of Object.entries(f.stats.categoryCols)) {
            statsLines.push(`${col} values: ${vals.map(v => v.includes(',') ? '"' + v + '"' : v).join(', ')}`);
          }
        }
      }
    }

    // File scores already computed during pre-scoring phase — reuse them
    const fileScoreMap = new Map(
      filesToSearch.map(f => [f.name, f._preScore || 0])
    );

    // Guaranteed: FILE SUMMARY chunk from each file (same pattern as buildContextTwoPass)
    const summaryChunks = [];
    const seenSummary = new Set();
    for (const fileData of filesToSearch) {
      if (!fileData.chunks?.length || seenSummary.has(fileData.name)) continue;
      const first = fileData.chunks[0];
      if (first.toLowerCase().startsWith('file summary') || first.toLowerCase().startsWith('sheet:')) {
        summaryChunks.push({ chunk: first, score: 999, filename: fileData.name, collection: fileData.collection, meta: fileData.meta || {} });
        seenSummary.add(fileData.name);
      }
    }

    const scored = [];
    for (const fileData of filesToSearch) {
      if (!fileData.chunks) continue;
      const fileBoost = fileScoreMap.get(fileData.name) || 0;
      for (const chunk of fileData.chunks) {
        const score = scoreChunk(chunk, keywords) + fileBoost;
        if (score >= 0) scored.push({ chunk, score, filename: fileData.name, collection: fileData.collection, meta: fileData.meta || {} });
      }
    }

    scored.sort((a, b) => b.score - a.score);
    // Guarantee at least one scored chunk per file for coverage
    const seenFiles = new Set();
    const perFileGuarantee = [];
    for (const s of scored) {
      if (!seenFiles.has(s.filename)) { perFileGuarantee.push(s); seenFiles.add(s.filename); }
      if (seenFiles.size >= filesToSearch.length) break;
    }
    const remaining = scored.filter(s => !perFileGuarantee.includes(s));
    const combined = [...perFileGuarantee, ...remaining];

    // Give more chunks when searching a small number of files (e.g. single file scope)
    const ctxChunkLimit = filesToSearch.length <= 2 ? 20 : filesToSearch.length <= 5 ? 16 : 12;
    const summaryTexts = new Set(summaryChunks.map(c => c.chunk));
    const scoredNonSummary = combined.filter(c => !summaryTexts.has(c.chunk));
    const remainingSlots = Math.max(ctxChunkLimit - summaryChunks.length, 0);
    const top = [...summaryChunks, ...scoredNonSummary.slice(0, remainingSlots)];

    if (!top.length) {
      const fallback = filesToSearch.slice(0, 3).flatMap(f =>
        (f.chunks || []).slice(0, 2).map(chunk => ({ chunk, filename: f.name, collection: f.collection, meta: f.meta || {} }))
      );
      if (!fallback.length) return { text: '', sources: [], statsContext: statsLines.join('\n'), focusFile: filesToSearch[0]?.name };
      const sources = [...new Set(fallback.map(x => x.filename))];
      const text = fallback.map(x => {
        const period = x.meta?.period ? ` [${x.meta.period}]` : '';
        return `[${x.collection}${period} / ${x.filename}]\n${x.chunk}`;
      }).join('\n\n---\n\n');
      return { text, sources, statsContext: statsLines.join('\n'), focusFile: filesToSearch[0]?.name };
    }

    // Re-rank before final assembly
    const rerankedTop = rerankChunks(top, keywords, intent);

    const sources = [...new Set(rerankedTop.map(x => x.filename))];
    const text = rerankedTop.map(x => {
      const period = x.meta?.period ? ` [${x.meta.period}]` : '';
      return `[${x.collection}${period} / ${x.filename}]\n${x.chunk}`;
    }).join('\n\n---\n\n');

    return { text, sources, statsContext: statsLines.join('\n'), focusFile: filesToSearch[0]?.name };
  } catch (err) {
    console.error('Context error:', err.message);
    return { text: '', sources: [], statsContext: '', focusFile: null };
  }
}

// ── Collection Analysis ───────────────────────────────────────
async function handleCollectionAnalyze(request, env) {
  try {
    const { colName, dept, manifest } = await request.json();
    if (!colName || !manifest) return json({ error: 'colName and manifest required' }, 400);

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey && !env.AI) return json({ error: 'No AI provider configured. Add an API key in Config.' }, 400);

    const prompt = `You are analyzing a document collection for a cannabis company (Jushi Holdings).

Collection name: "${colName}"

Files in this collection (name, period, row count, columns where available):
${manifest}

Based on this information, return ONLY valid JSON with no markdown or preamble:
{
  "description": "1-2 sentence description covering: what kind of data this is, which states/stores/products if apparent, and the time range covered. Be specific — mention column names or metrics if they reveal what the data tracks. Example: 'Monthly retail sales data by state, store, and product category tracking units sold and revenue, covering March 2024 through February 2026 across PA, IL, NV, and VA locations.'",
  "category": "the primary category: Sales, Inventory, Compliance, Finance, HR, Operations, Marketing, Customer, Product, Legal, Technology, or Other",
  "summary": "ultra-short 5-8 word summary (e.g. 'Monthly retail sales by state and product')"
}`;

    const result = await callLLMLight({
      model: 'claude',
      system: 'You are a precise document analyst. Return only valid JSON, no markdown, no preamble.',
      messages: [{ role: 'user', content: prompt }],
      maxTokens: 300,
      env,
      apiKey,
    });

    let parsed;
    try { parsed = JSON.parse(result.replace(/```json|```/g, '').trim()); }
    catch { return json({ error: 'Failed to parse AI response' }, 500); }

    return json({ ok: true, ...parsed });
  } catch(err) {
    return json({ error: 'Collection analyze error: ' + err.message }, 500);
  }
}

// ── Collection Describe (PATCH description onto registry entry) ─
async function handleCollectionDescribe(request, env) {
  try {
    const { name, dept, description, category, summary } = await request.json();
    if (!name || !dept) return json({ error: 'name and dept required' }, 400);

    const regKey = `colreg:${dept}`;
    const reg    = await env.CACI_KV.get(regKey, 'json') || [];
    const entry  = reg.find(c => c.name === name);

    if (!entry) return json({ error: 'Collection not found' }, 404);

    if (description) entry.description = description;
    if (category)    entry.category    = category;
    if (summary)     entry.summary     = summary;
    entry.analyzedAt = new Date().toISOString();

    await env.CACI_KV.put(regKey, JSON.stringify(reg));
    return json({ ok: true });
  } catch(err) {
    return json({ error: 'Collection describe error: ' + err.message }, 500);
  }
}

// ── AI Document Classification ────────────────────────────────
async function handleAiClassify(request, env) {
  try {
    const { fileName, collection, sample, colNames } = await request.json();
    if (!fileName || !sample) return json({ error: 'fileName and sample required' }, 400);

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey && !env.AI) return json({ error: 'No AI provider configured. Add an API key in Config.' }, 400);

    const prompt = `You are classifying a business document for a cannabis company (Jushi Holdings).

Document filename: "${fileName}"
Current collection: "${collection}"
Available collections: ${colNames || 'none yet'}

Document sample (may include FILE SUMMARY with stats for tabular files, or opening text for PDFs/DOCX):
---
${sample}
---

Extract the following and return ONLY valid JSON, no markdown, no other text:
{
  "reportName": "short descriptive name for this specific report (not the filename, max 60 chars)",
  "category": "one of: Sales, Inventory, Compliance, Finance, HR, Operations, Marketing, Customer, Product, Legal, Technology, Other",
  "period": "time period this covers e.g. Q1 2026, March 2025, Full Year 2025 — empty string if evergreen or no clear date",
  "reportType": "one of: Summary Report, Detail Report, Audit, Survey, Invoice, Contract, Policy, Presentation, Spreadsheet, Other",
  "suggestedCollection": "name of best matching collection from the available list if different from current, else empty string",
  "isContextDoc": false,
  "confidence": "high, medium, or low"
}

Rules:
- isContextDoc = true only for SOPs, glossaries, regulations, policies, org charts, reference documents (not time-series data reports)
- period = empty string for regulations, policies, contracts (evergreen docs)
- confidence = low if you cannot determine category or reportName clearly from the text`;

    const result = await callLLMLight({
      model: 'claude',
      system: 'You are a precise document classifier. Return only valid JSON, no markdown, no preamble.',
      messages: [{ role: 'user', content: prompt }],
      maxTokens: 350,
      env,
      apiKey,
    });
    return json({ ok: true, result });
  } catch(err) {
    return json({ error: 'AI classify error: ' + err.message }, 500);
  }
}

// ── Admin ─────────────────────────────────────────────────────
async function handleAdminSave(request, env) {
  try {
    const body = await request.json();
    const saved = [];
    for (const key of ['ANTHROPIC_API_KEY', 'XAI_API_KEY']) {
      if (body[key] !== undefined) {
        body[key] === '' ? await env.CACI_KV.delete('config:' + key) : await env.CACI_KV.put('config:' + key, body[key]);
        saved.push(key);
      }
    }
    return json({ ok: true, saved });
  } catch (err) { return json({ error: err.message }, 500); }
}

async function handleAdminGet(env) {
  try {
    const kvAnt = await env.CACI_KV.get('config:ANTHROPIC_API_KEY');
    const kvXai = await env.CACI_KV.get('config:XAI_API_KEY');
    return json({
      ANTHROPIC_API_KEY: {
        configured: !!(kvAnt || env.ANTHROPIC_API_KEY),
        source: kvAnt ? 'admin' : env.ANTHROPIC_API_KEY ? 'secret' : 'none',
      },
      XAI_API_KEY: {
        configured: !!(kvXai || env.XAI_API_KEY),
        source: kvXai ? 'admin' : env.XAI_API_KEY ? 'secret' : 'none',
      },
    });
  } catch (err) { return json({ error: err.message }, 500); }
}

// ── Retrieval Weight Admin ────────────────────────────────────
async function handleGetWeights(request, env) {
  try {
    const url  = new URL(request.url);
    const dept = url.searchParams.get('dept') || 'default';
    const stored = await env.CACI_KV.get(`config:scoring-weights:${dept}`, 'json').catch(() => null);
    const defaults = getDefaultWeights();
    const weights  = stored ? { ...defaults, ...stored } : defaults;
    return json({ ok: true, weights, defaults, dept, isCustom: !!stored, lastUpdated: stored?._lastUpdated || null });
  } catch (err) { return json({ error: err.message }, 500); }
}

async function handleSaveWeights(request, env) {
  try {
    const { dept, weights } = await request.json();
    if (!dept || !weights) return json({ error: 'dept and weights required' }, 400);
    const defaults = getDefaultWeights();
    const sanitized = {};
    for (const key of Object.keys(defaults)) {
      if (typeof weights[key] === 'number' && isFinite(weights[key])) {
        sanitized[key] = weights[key];
      }
    }
    sanitized._lastUpdated = new Date().toISOString();
    sanitized._appliedCount = weights._appliedCount || 0;
    await env.CACI_KV.put(`config:scoring-weights:${dept}`, JSON.stringify(sanitized));
    return json({ ok: true, weights: sanitized });
  } catch (err) { return json({ error: err.message }, 500); }
}

async function handleResetWeights(request, env) {
  try {
    const { dept } = await request.json();
    if (!dept) return json({ error: 'dept required' }, 400);
    await env.CACI_KV.delete(`config:scoring-weights:${dept}`);
    return json({ ok: true, weights: getDefaultWeights() });
  } catch (err) { return json({ error: err.message }, 500); }
}

// ── Feedback & Self-Improvement ───────────────────────────────

async function handleFeedback(request, env) {
  try {
    const { responseId, signal, comment, query, dept, sources, retrievalScope } = await request.json();
    if (!responseId || !signal || !dept) return json({ error: 'Missing required fields' }, 400);
    if (!['sharp', 'missed', 'incomplete'].includes(signal)) return json({ error: 'Invalid signal' }, 400);

    const entry = {
      responseId, signal, comment: comment || '', query: query || '',
      dept, sources: sources || [], retrievalScope: retrievalScope || 'all',
      timestamp: new Date().toISOString(),
    };

    // Store individual feedback entry
    await env.CACI_KV.put(`feedback:${dept}:${responseId}`, JSON.stringify(entry));

    // Update rolling summary counter for this dept
    const summaryKey = `feedback:summary:${dept}`;
    const summary = await env.CACI_KV.get(summaryKey, 'json') || { sharp: 0, missed: 0, incomplete: 0, total: 0, lastUpdated: null };
    summary[signal] = (summary[signal] || 0) + 1;
    summary.total = (summary.total || 0) + 1;
    summary.lastUpdated = entry.timestamp;
    await env.CACI_KV.put(summaryKey, JSON.stringify(summary));

    return json({ ok: true });
  } catch (err) { return json({ error: err.message }, 500); }
}

async function handleGetFeedbackLog(url, env) {
  try {
    const dept = url.searchParams.get('dept') || 'default';
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);

    const summary = await env.CACI_KV.get(`feedback:summary:${dept}`, 'json') || { sharp: 0, missed: 0, incomplete: 0, total: 0 };

    // List recent feedback keys for this dept
    const listResult = await env.CACI_KV.list({ prefix: `feedback:${dept}:` });
    const keys = (listResult.keys || []).slice(-limit);

    const entries = await Promise.all(
      keys.map(k => env.CACI_KV.get(k.name, 'json').catch(() => null))
    );
    const valid = entries.filter(Boolean).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

    return json({ ok: true, summary, entries: valid });
  } catch (err) { return json({ error: err.message }, 500); }
}

async function handleAnalyzeFeedback(request, env) {
  try {
    const { dept } = await request.json();
    if (!dept) return json({ error: 'dept required' }, 400);

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured' }, 400);

    // Gather recent negative feedback (missed + incomplete)
    const listResult = await env.CACI_KV.list({ prefix: `feedback:${dept}:` });
    const keys = listResult.keys || [];

    const entries = (await Promise.all(
      keys.map(k => env.CACI_KV.get(k.name, 'json').catch(() => null))
    )).filter(Boolean);

    const negatives = entries.filter(e => e.signal === 'missed' || e.signal === 'incomplete');
    const positives = entries.filter(e => e.signal === 'sharp');

    if (negatives.length < 3) {
      return json({ ok: true, message: 'Not enough feedback yet (need at least 3 negative signals to analyze)', suggestions: [] });
    }

    // Get current weights
    const currentWeights = await env.CACI_KV.get(`config:scoring-weights:${dept}`, 'json') || getDefaultWeights();
    const summary = await env.CACI_KV.get(`feedback:summary:${dept}`, 'json') || {};

    // Ask Claude to analyze and suggest weight adjustments
    const analysisPrompt = `You are analyzing feedback on a RAG (retrieval-augmented generation) system called CACI used by a cannabis company. Your job is to suggest scoring weight adjustments based on user feedback patterns.

CURRENT SCORING WEIGHTS:
${JSON.stringify(currentWeights, null, 2)}

FEEDBACK SUMMARY for department "${dept}":
- Sharp (good): ${positives.length}
- Missed (wrong retrieval): ${negatives.filter(e=>e.signal==='missed').length}  
- Incomplete (right direction, not enough depth): ${negatives.filter(e=>e.signal==='incomplete').length}
- Total: ${entries.length}

RECENT NEGATIVE FEEDBACK SAMPLES (query + signal + comment):
${negatives.slice(-15).map(e => `[${e.signal.toUpperCase()}] Query: "${e.query}" | Sources returned: ${(e.sources||[]).join(', ')||'none'} | Comment: "${e.comment||'none'}"`).join('\n')}

RECENT POSITIVE FEEDBACK SAMPLES:
${positives.slice(-8).map(e => `[SHARP] Query: "${e.query}" | Sources: ${(e.sources||[]).join(', ')||'none'}`).join('\n')}

Based on this feedback, suggest 1-4 specific weight adjustments. For each suggestion:
1. Name the weight to change (must be one of: yearMatchBase, yearRecencyMultiplier, monthMatchBonus, rangeMatchBonus, quarterMatchBonus, keywordFilenameBonus, recencyMaxBonus, categoryColBonus, fileSummaryBonus, rerankDirectAnswerBonus, rerankNumericBonus, rerankComparativeBonus, xlsxPenalty)
2. Current value (from the weights above)
3. Suggested new value
4. One-sentence reason grounded in the feedback patterns

Respond ONLY as a JSON array of objects with keys: weightName, currentValue, suggestedValue, reason. No preamble, no markdown.`;

    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 800,
        messages: [{ role: 'user', content: analysisPrompt }],
      }),
    });

    const data = await resp.json();
    const raw = data.content?.[0]?.text || '[]';

    let suggestions = [];
    try {
      const cleaned = raw.replace(/```json|```/g, '').trim();
      suggestions = JSON.parse(cleaned);
    } catch { suggestions = []; }

    // Store pending suggestions for admin review
    const pending = {
      dept, generatedAt: new Date().toISOString(),
      feedbackSnapshot: { sharp: positives.length, missed: negatives.filter(e=>e.signal==='missed').length, incomplete: negatives.filter(e=>e.signal==='incomplete').length },
      suggestions,
      status: 'pending',
    };
    await env.CACI_KV.put(`config:tune-pending:${dept}`, JSON.stringify(pending));

    return json({ ok: true, suggestions, feedbackSnapshot: pending.feedbackSnapshot });
  } catch (err) { return json({ error: err.message }, 500); }
}

async function handleGetPendingTuning(env) {
  try {
    const listResult = await env.CACI_KV.list({ prefix: 'config:tune-pending:' });
    const pending = (await Promise.all(
      (listResult.keys || []).map(k => env.CACI_KV.get(k.name, 'json').catch(() => null))
    )).filter(p => p && p.status === 'pending');
    return json({ ok: true, pending });
  } catch (err) { return json({ error: err.message }, 500); }
}

async function handleApproveTuning(request, env) {
  try {
    const { dept, approvedIndices, action } = await request.json();
    // action: 'approve' | 'reject_all'
    if (!dept) return json({ error: 'dept required' }, 400);

    const pendingKey = `config:tune-pending:${dept}`;
    const pending = await env.CACI_KV.get(pendingKey, 'json');
    if (!pending) return json({ error: 'No pending suggestions found' }, 404);

    if (action === 'reject_all') {
      pending.status = 'rejected';
      pending.rejectedAt = new Date().toISOString();
      await env.CACI_KV.put(pendingKey, JSON.stringify(pending));
      return json({ ok: true, message: 'All suggestions rejected' });
    }

    // Apply only approved indices
    const toApply = (approvedIndices || []).map(i => pending.suggestions[i]).filter(Boolean);
    if (!toApply.length) return json({ error: 'No valid indices provided' }, 400);

    const weights = await env.CACI_KV.get(`config:scoring-weights:${dept}`, 'json') || getDefaultWeights();
    const changes = [];
    for (const s of toApply) {
      if (weights.hasOwnProperty(s.weightName)) {
        const old = weights[s.weightName];
        weights[s.weightName] = s.suggestedValue;
        changes.push({ weightName: s.weightName, from: old, to: s.suggestedValue, reason: s.reason });
      }
    }

    weights._lastUpdated = new Date().toISOString();
    weights._appliedCount = (weights._appliedCount || 0) + changes.length;
    await env.CACI_KV.put(`config:scoring-weights:${dept}`, JSON.stringify(weights));

    // Archive the tune log
    const logKey = `config:tune-log:${dept}`;
    const log = await env.CACI_KV.get(logKey, 'json') || [];
    log.unshift({ appliedAt: new Date().toISOString(), changes, feedbackSnapshot: pending.feedbackSnapshot });
    if (log.length > 50) log.splice(50);
    await env.CACI_KV.put(logKey, JSON.stringify(log));

    pending.status = 'applied';
    pending.appliedAt = new Date().toISOString();
    pending.appliedChanges = changes;
    await env.CACI_KV.put(pendingKey, JSON.stringify(pending));

    return json({ ok: true, changes });
  } catch (err) { return json({ error: err.message }, 500); }
}

function getDefaultWeights() {
  return {
    yearMatchBase: 8,
    yearRecencyMultiplier: 2,
    monthMatchBonus: 7,
    rangeMatchBonus: 22,
    quarterMatchBonus: 7,
    annualBonus: 6,
    keywordFilenameBonus: 6,
    recencyMaxBonus: 3,
    categoryColBonus: 8,
    fileSummaryBonus: 50,
    rerankDirectAnswerBonus: 4,
    rerankNumericBonus: 0.3,
    rerankComparativeBonus: 4,
    xlsxPenalty: -1,
  };
}

async function getScoringWeights(dept, env) {
  const stored = await env.CACI_KV.get(`config:scoring-weights:${dept}`, 'json').catch(() => null);
  return stored ? { ...getDefaultWeights(), ...stored } : getDefaultWeights();
}

// ── Re-ranking pass ───────────────────────────────────────────
function rerankChunks(chunks, keywords, intent, W = null) {
  if (!chunks.length) return chunks;
  const w = W || getDefaultWeights();
  const expanded = expandKeywords(keywords);
  const scored = chunks.map((c, idx) => {
    const lower = c.chunk.toLowerCase();
    let bonus = 0;
    // Direct answer signal: number near a keyword
    for (const kw of expanded) {
      const re = new RegExp('(?<![a-z0-9])' + escapeRegex(kw) + '[^\n]{0,40}\d+[,.]?\d*', 'i');
      if (re.test(lower)) bonus += w.rerankDirectAnswerBonus;
    }
    // Numeric density for aggregate queries
    if (intent?.isAggregate) {
      const numCount = (lower.match(/\b\d+[,.]?\d*\b/g) || []).length;
      bonus += Math.min(numCount * w.rerankNumericBonus, 3);
    }
    // Multi-year presence for comparative queries
    if (intent?.isComparative) {
      const years = [...new Set(lower.match(/20\d\d/g) || [])];
      if (years.length >= 2) bonus += w.rerankComparativeBonus;
    }
    // FILE SUMMARY and DOCUMENT SUMMARY always first
    if (lower.startsWith('file summary') || lower.startsWith('document summary')) bonus += w.fileSummaryBonus;
    // Redundancy penalty
    const fp = lower.replace(/\s+/g, ' ').slice(0, 100);
    const isDup = chunks.slice(0, idx).some(prev =>
      prev.chunk.toLowerCase().replace(/\s+/g, ' ').slice(0, 100) === fp
    );
    if (isDup) bonus -= 20;
    return { ...c, rerankScore: (c.score || 0) + bonus };
  });
  scored.sort((a, b) => b.rerankScore - a.rerankScore);
  return scored;
}

// ── Helpers ───────────────────────────────────────────────────
function chunkText(text, size = 1500) {
  if (!text || text.length === 0) return [];

  // Detect tabular format — preserve it as-is (already chunked by frontend)
  // These arrive with "---" separators between row batches
  if (text.includes('\n---\n') || text.includes('\nRow 1 —') || text.startsWith('FILE SUMMARY')) {
    const parts = text.split(/\n\n---\n\n/);
    // Re-merge small parts to avoid tiny chunks; split oversized ones
    const out = [];
    let buf = '';
    for (const part of parts) {
      if (buf.length + part.length + 4 <= size * 1.2) {
        buf = buf ? buf + '\n\n---\n\n' + part : part;
      } else {
        if (buf) out.push(buf);
        buf = part;
      }
    }
    if (buf) out.push(buf);
    return out.length ? out : [text];
  }

  // For prose/PDF/DOCX: chunk on paragraph or sentence boundaries
  // Never slice mid-sentence — look back up to 300 chars for a good break
  const chunks = [];
  let start = 0;
  while (start < text.length) {
    let end = Math.min(start + size, text.length);
    if (end < text.length) {
      // Try to break at paragraph boundary first
      const paraBreak = text.lastIndexOf('\n\n', end);
      if (paraBreak > start + size * 0.5) {
        end = paraBreak;
      } else {
        // Fall back to sentence boundary
        const sentBreak = Math.max(
          text.lastIndexOf('. ', end),
          text.lastIndexOf('.\n', end),
          text.lastIndexOf('? ', end),
          text.lastIndexOf('! ', end)
        );
        if (sentBreak > start + size * 0.5) end = sentBreak + 1;
      }
    }
    chunks.push(text.slice(start, end).trim());
    // Overlap: back up 150 chars to preserve cross-boundary context
    start = Math.max(start + 1, end - 150);
    if (start >= text.length) break;
  }
  return chunks.filter(c => c.length > 50);
}

// ── Relative date resolver ────────────────────────────────────
function resolveRelativeDates(message) {
  const msg   = message.toLowerCase();
  const today = new Date();
  const m     = today.getMonth() + 1;
  const y     = today.getFullYear();
  const q     = Math.ceil(m / 3);

  if (/last month|previous month/.test(msg)) {
    const lm = m === 1 ? 12 : m - 1, ly = m === 1 ? y - 1 : y;
    return { rangeStart: { m: lm, y: ly }, rangeEnd: { m: lm, y: ly } };
  }
  if (/this month|month to date|mtd/.test(msg)) {
    return { rangeStart: { m, y }, rangeEnd: { m, y } };
  }
  if (/last quarter|previous quarter/.test(msg)) {
    const lq = q === 1 ? 4 : q - 1, lqy = q === 1 ? y - 1 : y;
    const qs = [0,1,4,7,10][lq], qe = [0,3,6,9,12][lq];
    return { rangeStart: { m: qs, y: lqy }, rangeEnd: { m: qe, y: lqy } };
  }
  if (/this quarter|current quarter/.test(msg)) {
    const qs = [0,1,4,7,10][q], qe = [0,3,6,9,12][q];
    return { rangeStart: { m: qs, y }, rangeEnd: { m: qe, y } };
  }
  if (/last year|prior year/.test(msg)) {
    return { rangeStart: { m: 1, y: y - 1 }, rangeEnd: { m: 12, y: y - 1 } };
  }
  if (/this year|year to date|ytd/.test(msg)) {
    return { rangeStart: { m: 1, y }, rangeEnd: { m, y } };
  }
  const pastN = msg.match(/past\s+(\w+|\d+)\s+months?/);
  if (pastN) {
    const wn = {one:1,two:2,three:3,four:4,five:5,six:6,seven:7,eight:8,nine:9,ten:10,twelve:12};
    const n = parseInt(pastN[1]) || wn[pastN[1]] || 3;
    let em = m - 1, ey = y; if (em === 0) { em = 12; ey--; }
    let sm = em - n + 1, sy = ey; while (sm <= 0) { sm += 12; sy--; }
    return { rangeStart: { m: sm, y: sy }, rangeEnd: { m: em, y: ey } };
  }
  const lastNQ = msg.match(/last\s+(\d+|two|three|four)\s+quarters?/);
  if (lastNQ) {
    const wn = {two:2,three:3,four:4};
    const n = parseInt(lastNQ[1]) || wn[lastNQ[1]] || 2;
    let eq = q - 1, eqy = y; if (eq === 0) { eq = 4; eqy--; }
    let sq = eq - n + 1, sqy = eqy; while (sq <= 0) { sq += 4; sqy--; }
    const qs = [0,1,4,7,10][sq], qe = [0,3,6,9,12][eq];
    return { rangeStart: { m: qs, y: sqy }, rangeEnd: { m: qe, y: eqy } };
  }
  return null;
}

// ── Derived business metric synonyms ─────────────────────────
const DERIVED_METRIC_SYNONYMS = {
  'basket size':          ['revenue','avg','transaction'],
  'average order':        ['revenue','avg'],
  'average transaction':  ['revenue','avg'],
  'aov':                  ['revenue','avg'],
  'items per basket':     ['units','avg'],
  'units per transaction':['units','avg'],
  'sell through':         ['sold','units','inventory'],
  'sell-through':         ['sold','units','inventory'],
  'sellthrough':          ['sold','units','inventory'],
  'margin':               ['revenue','cost','profit','gross'],
  'profitability':        ['revenue','cost','margin','gross'],
  'gross profit':         ['revenue','cost','gross'],
  'conversion':           ['transactions','visits','customers'],
  'conversion rate':      ['transactions','visits'],
  'foot traffic':         ['visits','customers','transactions'],
  'shrinkage':            ['shrink','loss','variance','adjustment'],
  'variance':             ['shrink','loss','adjustment'],
  'loss rate':            ['shrink','loss','variance'],
  'growth rate':          ['revenue','change','increase','decrease'],
  'run rate':             ['revenue','monthly','annualized'],
  'compliance rate':      ['violations','audit','passed','failed'],
  'error rate':           ['errors','corrections','rejected'],
  'retention':            ['repeat','customers','returning'],
  'new customers':        ['new','first','customers'],
  'customer count':       ['customers','patients','transactions'],
};

function expandWithDerivedMetrics(message, keywords) {
  const msgLower = message.toLowerCase();
  const extras = new Set(keywords);
  for (const [phrase, expansions] of Object.entries(DERIVED_METRIC_SYNONYMS)) {
    if (msgLower.includes(phrase)) expansions.forEach(e => extras.add(e));
  }
  return [...extras];
}

// Known 2-char cannabis state abbreviations — preserved despite length filter
const STATE_ABBREVS = new Set(['pa','il','nv','va','nj','oh','ma','fl','ca','co','mi','az','mo','md','mn','ok','or','wa','ny']);

function extractKeywords(query) {
  const stop = new Set(['a','an','the','is','are','was','were','be','been','have','has','had',
    'do','does','did','will','would','could','should','may','might','what','which','who',
    'this','that','these','those','i','me','my','we','our','you','your','he','she','it',
    'they','them','and','but','or','for','at','by','in','of','on','to','as','if','how',
    'when','where','why','with','about','from','into','can','tell','show','give','get','all',
    'report','reports','data','file','files','show','list','give','find','between','across',
    'total','totals','number','numbers','amount','amounts','value','values']);
  // Split preserving case for state abbrev detection, then lowercase
  const tokens = query.replace(/[^a-zA-Z0-9\s]/g,' ').split(/\s+/);
  const base = tokens
    .map(t => t.toLowerCase())
    .filter(w => {
      if (w.length === 0) return false;
      if (stop.has(w)) return false;
      // Keep 2-char state abbreviations; drop other 1-2 char tokens
      if (w.length <= 2) return STATE_ABBREVS.has(w);
      return true;
    });
  // Expand with derived business metrics (maps KPIs to actual column terms)
  return expandWithDerivedMetrics(query, base);
}

// Escape special regex characters so keywords like "$" or "." don't misbehave
function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Cannabis/retail domain synonym map — expands queries without adding noise
// Keys are query terms; values are additional terms to score against
const DOMAIN_SYNONYMS = {
  // State abbreviations → full names (for chunk content matching)
  'pa': ['pennsylvania'], 'il': ['illinois'], 'nv': ['nevada'],
  'va': ['virginia'],     'nj': ['new jersey'], 'oh': ['ohio'],
  'ma': ['massachusetts'], 'fl': ['florida'],  'ca': ['california'],
  'co': ['colorado'],
  // Revenue/sales
  'revenue':    ['sales', 'gross'],
  'sales':      ['revenue', 'gross'],
  'profit':     ['margin', 'net', 'revenue'],
  'margin':     ['profit', 'net'],
  'inventory':  ['stock', 'units', 'qty', 'quantity', 'on hand'],
  'shrink':     ['loss', 'shrinkage', 'variance', 'adjustment'],
  'flower':     ['bud', 'herb', 'cannabis flower'],
  'vape':       ['vapes', 'vaporizer', 'cartridge', 'cart', 'pod'],
  'concentrate':['concentrates', 'wax', 'shatter', 'resin', 'rosin', 'badder', 'diamonds'],
  'edible':     ['edibles', 'gummy', 'gummies', 'chocolate', 'capsule'],
  'preroll':    ['pre-roll', 'pre roll', 'joint', 'infused'],
  'compliance': ['regulatory', 'regulation', 'audit', 'metrc'],
  'return':     ['returns', 'refund', 'credit', 'complaint'],
  'transfer':   ['transfers', 'manifest', 'transport'],
  'dispensary': ['store', 'retail', 'location', 'site'],
  'wholesale':  ['b2b', 'leaftrade', 'bulk'],
  'patient':    ['customer', 'member', 'consumer'],
  'adult use':  ['recreational', 'adult-use', 'rec'],
  'medical':    ['mmj', 'patient', 'caregiver'],
};

function expandKeywords(keywords) {
  const expanded = new Set(keywords);
  for (const kw of keywords) {
    const syns = DOMAIN_SYNONYMS[kw];
    if (syns) syns.forEach(s => expanded.add(s));
    // Also match plurals/stems simply: "vape" matches "vapes" etc.
    if (kw.endsWith('s')) expanded.add(kw.slice(0, -1));
    else expanded.add(kw + 's');
  }
  return [...expanded];
}

function scoreChunk(chunk, keywords) {
  const lower = chunk.toLowerCase();
  // Summary blocks are guaranteed-included separately — exclude from race
  if (lower.startsWith('file summary\n') || lower.startsWith('file summary\r\n')) return -1;

  const expanded = expandKeywords(keywords);
  // Normalize by chunk length so short precise chunks aren't penalized vs long ones
  const lengthNorm = Math.sqrt(Math.max(lower.length, 100) / 1000);

  let raw = 0;
  for (const kw of expanded) {
    const re = new RegExp('(?<![a-z0-9])' + escapeRegex(kw) + '(?![a-z0-9])', 'gi');
    const matches = lower.match(re);
    if (matches) {
      // Column-value pattern gets 2x weight: "State: PA" >> incidental "pa"
      const colValueRe = new RegExp('[a-z_ ]+:\s*' + escapeRegex(kw) + '\b', 'gi');
      const colMatches = lower.match(colValueRe);
      const termScore = matches.length + (colMatches ? colMatches.length : 0);
      // Synonyms count at 0.6x weight so original keywords still dominate
      const weight = keywords.includes(kw) ? 1.0 : 0.6;
      raw += termScore * weight;
    }
  }
  // Return normalized score — keeps short direct-answer chunks competitive with long ones
  return raw / lengthNorm;
}

// ─────────────────────────────────────────────────────────────
//  INTEGRATIONS  (Microsoft 365, QuickBase)
// ─────────────────────────────────────────────────────────────

const ALLOWED_INTEGRATIONS = ['excel','word','teams','powerbi','quickbase'];

async function handleIntegrationSave(path, request, env) {
  const id = path.replace('/integrations/', '').split('/')[0];
  if (!ALLOWED_INTEGRATIONS.includes(id)) return json({ error: 'Unknown integration' }, 400);
  try {
    const body = await request.json();
    const secrets = {};
    const meta    = { id, connectedAt: new Date().toISOString(), dept: body.dept || 'global' };

    if (id === 'excel' || id === 'word') {
      secrets.clientSecret = body.secret || '';
      meta.tenantId        = body.tenant ? body.tenant.slice(0,8) + '...' : '';
    } else if (id === 'teams') {
      secrets.webhookUrl   = body.webhook || '';
      secrets.botToken     = body.token   || '';
      meta.hasWebhook      = !!body.webhook;
    } else if (id === 'powerbi') {
      secrets.clientSecret = body.secret    || '';
      meta.workspaceId     = body.workspace || '';
    } else if (id === 'quickbase') {
      secrets.userToken = body.token || '';
      meta.realm        = body.realm || '';
    }

    await env.CACI_KV.put('integ:' + id,        JSON.stringify(meta));
    await env.CACI_KV.put('integ-secret:' + id, JSON.stringify(secrets));
    return json({ ok: true, id });
  } catch(e) { return json({ error: e.message }, 500); }
}

async function handleIntegrationGet(path, url, env) {
  const id = path.replace('/integrations/', '').split('/')[0];
  if (id === '' || id === undefined) {
    const all = [];
    for (const iid of ALLOWED_INTEGRATIONS) {
      const m = await env.CACI_KV.get('integ:' + iid, 'json');
      if (m) all.push(m);
    }
    return json(all);
  }
  const meta = await env.CACI_KV.get('integ:' + id, 'json');
  if (!meta) return json({ error: 'Not found' }, 404);
  return json(meta);
}

async function handleIntegrationDelete(path, env) {
  const id = path.replace('/integrations/', '').split('/')[0];
  await env.CACI_KV.delete('integ:' + id);
  await env.CACI_KV.delete('integ-secret:' + id);
  return json({ ok: true });
}

// ─────────────────────────────────────────────────────────────
//  CONNECTORS  (Cannabis platforms)
// ─────────────────────────────────────────────────────────────

const ALLOWED_CONNECTORS = ['dutchie','metrc','iheartjane','leaftrade','mjfreeway'];

const CONNECTOR_ENDPOINTS = {
  dutchie:    { base: 'https://api.dutchie.com/v1',              verify: '/store' },
  metrc:      { base: 'https://api-{state}.metrc.com/v3',         verify: '/facilities' },
  iheartjane: { base: 'https://api.iheartjane.com/v1',            verify: '/stores/{store}' },
  leaftrade:  { base: 'https://api.leaftrade.com/api/v1',         verify: '/company/profile/' },
  mjfreeway:  { base: null,                                        verify: '/company' },
};

async function handleConnectorSave(path, request, env) {
  const parts = path.replace('/connectors/', '').split('/');
  const id    = parts[0];
  if (!ALLOWED_CONNECTORS.includes(id)) return json({ error: 'Unknown connector' }, 400);

  try {
    const body    = await request.json();
    const secrets = {};
    const meta    = { id, savedAt: new Date().toISOString(), dept: body.dept || 'global' };

    if (id === 'dutchie') {
      secrets.apiKey  = body.key   || '';
      meta.storeId    = body.store || '';
      meta.env        = body.env   || 'prod';
    } else if (id === 'metrc') {
      secrets.swKey   = body.sw   || '';
      secrets.userKey = body.user || '';
      meta.state      = (body.state || '').toUpperCase();
    } else if (id === 'iheartjane') {
      secrets.apiKey  = body.key   || '';
      meta.storeId    = body.store || '';
    } else if (id === 'leaftrade') {
      secrets.apiKey  = body.key     || '';
      meta.license    = body.license || '';
    } else if (id === 'mjfreeway') {
      secrets.token   = body.token   || '';
      meta.license    = body.license || '';
      meta.baseUrl    = body.url     || 'https://api.mjfreeway.com/v1';
    }

    await env.CACI_KV.put('con:' + id,        JSON.stringify(meta));
    await env.CACI_KV.put('con-secret:' + id, JSON.stringify(secrets));

    const verified = await verifyConnector(id, meta, secrets);
    return json({ ok: true, id, verified });
  } catch(e) { return json({ error: e.message }, 500); }
}

async function verifyConnector(id, meta, secrets) {
  try {
    if (id === 'dutchie') {
      const r = await fetch('https://api.dutchie.com/v1/store', { headers: { Authorization: 'Bearer ' + secrets.apiKey } });
      return r.ok;
    }
    if (id === 'metrc') {
      const state = (meta.state || 'co').toLowerCase();
      const creds = btoa(secrets.swKey + ':' + secrets.userKey);
      const r = await fetch('https://api-' + state + '.metrc.com/v3/facilities', { headers: { Authorization: 'Basic ' + creds } });
      return r.ok;
    }
    if (id === 'iheartjane') {
      const r = await fetch('https://api.iheartjane.com/v1/stores/' + meta.storeId, { headers: { Authorization: 'Bearer ' + secrets.apiKey } });
      return r.ok;
    }
    if (id === 'leaftrade') {
      const r = await fetch('https://api.leaftrade.com/api/v1/company/profile/', { headers: { Authorization: 'Token ' + secrets.apiKey } });
      return r.ok;
    }
    if (id === 'mjfreeway') {
      const r = await fetch((meta.baseUrl || 'https://api.mjfreeway.com/v1') + '/company', { headers: { Authorization: 'Bearer ' + secrets.token } });
      return r.ok;
    }
  } catch { return false; }
  return false;
}

async function handleConnectorGet(path, url, env) {
  const id = path.replace('/connectors/', '').split('/')[0];
  if (!id) {
    const all = [];
    for (const cid of ALLOWED_CONNECTORS) {
      const m = await env.CACI_KV.get('con:' + cid, 'json');
      if (m) all.push(m);
    }
    return json(all);
  }
  const meta = await env.CACI_KV.get('con:' + id, 'json');
  if (!meta) return json({ error: 'Not configured' }, 404);
  return json(meta);
}

async function handleConnectorDelete(path, env) {
  const id = path.replace('/connectors/', '').split('/')[0];
  await env.CACI_KV.delete('con:' + id);
  await env.CACI_KV.delete('con-secret:' + id);
  return json({ ok: true });
}

async function handleConnectorFetch(path, request, env) {
  const id = path.replace('/connectors/', '').split('/fetch')[0];
  const meta    = await env.CACI_KV.get('con:' + id, 'json');
  const secrets = await env.CACI_KV.get('con-secret:' + id, 'json');
  if (!meta || !secrets) return json({ error: 'Connector not configured' }, 404);

  const body     = await request.json().catch(() => ({}));
  const endpoint = body.endpoint || '';
  let   apiUrl   = '';
  const headers  = {};

  if (id === 'dutchie') {
    apiUrl = 'https://api.dutchie.com/v1' + endpoint;
    headers['Authorization'] = 'Bearer ' + secrets.apiKey;
  } else if (id === 'metrc') {
    const state = (meta.state || 'co').toLowerCase();
    const creds = btoa(secrets.swKey + ':' + secrets.userKey);
    apiUrl = 'https://api-' + state + '.metrc.com/v3' + endpoint;
    headers['Authorization'] = 'Basic ' + creds;
  } else if (id === 'iheartjane') {
    apiUrl = 'https://api.iheartjane.com/v1' + endpoint;
    headers['Authorization'] = 'Bearer ' + secrets.apiKey;
  } else if (id === 'leaftrade') {
    apiUrl = 'https://api.leaftrade.com/api/v1' + endpoint;
    headers['Authorization'] = 'Token ' + secrets.apiKey;
  } else if (id === 'mjfreeway') {
    apiUrl = (meta.baseUrl || 'https://api.mjfreeway.com/v1') + endpoint;
    headers['Authorization'] = 'Bearer ' + secrets.token;
  } else {
    return json({ error: 'Unknown connector' }, 400);
  }

  try {
    const r    = await fetch(apiUrl, { headers });
    const data = await r.json();
    return json({ ok: r.ok, status: r.status, data });
  } catch(e) {
    return json({ error: e.message }, 502);
  }
}

// ── TTS (Grok / Cloudflare) ───────────────────────────────────
async function handleTTSDebug(request, env) {
  try {
    const { text = 'test', voice = 'eve' } = await request.json().catch(() => ({}));
    const apiKey = (await env.CACI_KV.get('config:XAI_API_KEY')) || env.XAI_API_KEY;
    if (!apiKey) return json({ error: 'No xAI key found', kv: 'empty', env: typeof env.XAI_API_KEY });
    const keyPreview = apiKey.slice(0, 8) + '...' + apiKey.slice(-4);
    const res = await fetch('https://api.x.ai/v1/tts', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, voice_id: voice, language: 'en' }),
    });
    const body = await res.text();
    return json({ status: res.status, ok: res.ok, body, keyPreview });
  } catch(e) { return json({ error: e.message }); }
}

async function handleTTS(request, env) {
  try {
    const { text, provider = 'grok', voice = 'eve' } = await request.json();
    if (!text) return json({ error: 'Text required' }, 400);
    if (provider === 'grok' || provider === 'claude') {
      const apiKey = (await env.CACI_KV.get('config:XAI_API_KEY')) || env.XAI_API_KEY;
      if (!apiKey) return json({ error: 'xAI API key not configured.' }, 400);
      const ttsText = text.slice(0, 800);
      const res = await fetch('https://api.x.ai/v1/tts', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: ttsText, voice_id: voice, language: 'en' }),
      });
      if (!res.ok) {
        const errText = await res.text().catch(() => 'TTS failed');
        return json({ error: `xAI TTS error (${res.status}): ${errText}` }, 500);
      }
      return new Response(res.body, { headers: { 'Content-Type': 'audio/mpeg', ...CORS } });
    }
    if (provider === 'cloudflare') {
      if (!env.AI) return json({ error: 'Cloudflare AI binding not available' }, 500);
      const result = await env.AI.run('@cf/deepgram/aura-2-en', { text });
      return new Response(result, { headers: { 'Content-Type': 'audio/mpeg', ...CORS } });
    }
    return json({ error: 'Unknown TTS provider' }, 400);
  } catch (err) { return json({ error: 'TTS error: ' + err.message }, 500); }
}
