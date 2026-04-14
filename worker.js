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
    if (path === '/report'    && method === 'POST')   return handleReport(request, env);
    if (path === '/ai-classify'         && method === 'POST') return handleAiClassify(request, env);
    if (path === '/collection-analyze'  && method === 'POST') return handleCollectionAnalyze(request, env);
    if (path === '/collections/describe'&& method === 'POST') return handleCollectionDescribe(request, env);
    if (path === '/admin/config' && method === 'POST') return handleAdminSave(request, env);
    if (path === '/admin/config' && method === 'GET')  return handleAdminGet(env);

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
    const cleanText  = text.replace(/\s+/g, ' ').trim();
    const chunks     = chunkText(cleanText, chunkSize);

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
      chunks: chunks.length,
      chunkSize,
      meta,
      stats,
    };

    await env.CACI_KV.put(
      `file:${id}`,
      JSON.stringify({ ...fileRecord, chunks }),
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

    return json({ ok: true, id, name, collection, chunks: chunks.length, charCount: cleanText.length, isContext });
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
      const system = `You are Caci (pronounced like "Cassie") — the internal AI intelligence assistant for Jushi Holdings. You were built specifically for this team.

Today's date is ${new Date().toLocaleDateString('en-US', {year:'numeric',month:'long',day:'numeric'})}. The full calendar year 2025 is complete. When discussing 2025 data, treat it as a full historical year.

Your personality: You work in cannabis. You know these people. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have a lot of grace in how you communicate — you're tactful without being fake, honest without being harsh. You have a filter, but it's a thin one, because you value the truth more than comfort. You know how to read a room.

You also know that a lot of people are still figuring out how to work with AI. That's completely fine. You meet people where they are, you don't make them feel dumb for asking basic questions, and you guide them with patience. You're good at anticipating what someone actually needs vs. what they literally asked.

You're not just a number cruncher. You can talk about anything — industry trends, general questions, ideas, strategy, or just shoot the breeze. You happen to also be extremely good at analyzing data and documents when that's what's needed.

Today you're working with the ${dept} team. Here's what you have access to:
${discovery.collectionList}

Greet them like a colleague — warm, real, not robotic. Ask what they want to dig into. Keep it short.

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
      let discResponse;
      try {
        discResponse = await callLLM({ model, system, messages: [{ role: 'user', content: message }], maxTokens: 400, env, apiKey });
      } catch(e) {
        try {
          discResponse = await callLLM({ model: 'grok', system, messages: [{ role: 'user', content: message }], maxTokens: 400, env, apiKey });
        } catch(e2) {
          discResponse = `Hey! I'm Caci. Here's what I have access to:\n\n${discovery.collectionList}\n\nWhat would you like to dig into?`;
        }
      }
      return json({ ok: true, response: discResponse, sources: [], scope: 'discovery', collections: discovery.rawCollections, model: model || 'claude' });
    }

    // If user is asking about available collections, always answer from registry
    const collectionQueryWords = ['collections', 'collection', 'what do you have', 'what collections', 'which collections', 'what can you access', 'what data', 'what files'];
    const isCollectionQuery = collectionQueryWords.some(w => message.toLowerCase().includes(w));
    if (isCollectionQuery) {
      const discovery = await buildDiscoveryContext({ dept, env });
      const colSystem = `You are Caci. The user is asking what collections/data you have access to. Answer ONLY from this list — do not guess or add anything else:

${discovery.collectionList}

Be direct and conversational. List them clearly.`;
      const colRes = await callLLM({ model, system: colSystem, messages: [{ role: 'user', content: message }], maxTokens: 400, env, apiKey });
      return json({ ok: true, response: colRes, sources: [], scope: scope, model: model || 'claude' });
    }

    // Auto-switch collection — only on full collection name match
    let activeCollection = collection;
    let activeScope = scope;
    if (scope === 'collection' && collection) {
      const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
      const msgLower = message.toLowerCase();
      const matchedCol = reg.find(c => {
        if (c.name === collection) return false;
        if (msgLower.includes(c.name.toLowerCase())) return true;
        return false;
      });
      if (matchedCol) { activeCollection = matchedCol.name; activeScope = 'collection'; }
    }
    const context = await buildContext({ message, dept, collection: activeCollection, fileId, scope: activeScope, env });

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

    const responseText = await callLLM({ model, system, messages: [...history.slice(-10).map(h => ({ role: h.role, content: h.content })), { role: 'user', content: message }], maxTokens: 3000, env, apiKey });
    return json({ ok: true, response: responseText, sources: context.sources, scope, model: model || 'claude' });
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

// ── Report Generation ─────────────────────────────────────────
async function handleReport(request, env) {
  try {
    const { prompt, dept, collection, fileId, scope = 'all', format = 'markdown' } = await request.json();

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured.' }, 400);

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

    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4000,
        messages: [{ role: 'user', content: reportPrompt }],
      }),
    });

    if (!res.ok) {
      const err = await res.text();
      return json({ error: `Report generation error (${res.status}): ${err}` }, 500);
    }

    const data = await res.json();
    const reportText = data.content?.[0]?.text || '';
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
  const pat = /(?:between|from)\s+(\w+)\s+(?:of\s+)?(\d{4})\s+(?:and|to|through)\s+(\w+)\s+(?:of\s+)?(\d{4})|(\w+)\s+(?:of\s+)?(\d{4})\s+through\s+(\w+)\s+(?:of\s+)?(\d{4})/i;
  const m = message.match(pat);
  if (!m) return { rangeStart: null, rangeEnd: null };
  const [, m1, y1, m2, y2, m1b, y1b, m2b, y2b] = m;
  const rm1 = (m1 || m1b || '').toLowerCase();
  const ry1 = parseInt(y1 || y1b || '0');
  const rm2 = (m2 || m2b || '').toLowerCase();
  const ry2 = parseInt(y2 || y2b || '0');
  if (_MONTH_TO_NUM[rm1] && _MONTH_TO_NUM[rm2]) {
    return {
      rangeStart: { m: _MONTH_TO_NUM[rm1], y: ry1 },
      rangeEnd:   { m: _MONTH_TO_NUM[rm2], y: ry2 },
    };
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
          if (fileVal >= startVal && fileVal <= endVal) score += 30;
        }
      }

      // ── 2. Year match — larger multiplier (x3) so year gaps are decisive ──
      const matchedYears = queryYears.size > 0 ? [...queryYears].filter(y => nameLower.includes(y)) : [];
      if (matchedYears.length > 0) {
        const maxYear = Math.max(...matchedYears.map(Number));
        score += 10 + (maxYear - 2020) * 3; // 2026=+28, 2025=+25, 2024=+22
      }

      // ── 3. Month match — only add if NOT already in a range ──
      // Prevents e.g. "February 2025" from matching "february" in a 2025–2026 range query
      if (queryMonths.size > 0 && score < 30) {
        const hasMonthMatch = [...queryMonths].some(m => {
          const abbr = _MONTH_ABBREV[m] || m;
          return nameLower.includes(m) || nameLower.includes(abbr);
        });
        if (hasMonthMatch) score += 8;
      }

      // ── 4. Annual / full-year boost ───────────────────────
      const isAnnual = nameLower.includes('annual') || nameLower.includes('full year')
        || nameLower.includes('ye ') || nameLower.includes('10-k') || nameLower.includes('10k');
      if (isAnnual && (queryYears.size > 0 || message.toLowerCase().includes('annual') || message.toLowerCase().includes('full year'))) {
        score += 8;
      }

      // ── 5. Quarter match ──────────────────────────────────
      const fileQ = nameLower.match(/q[1-4]|first quarter|second quarter|third quarter|fourth quarter/)?.[0];
      if (fileQ && queryQuarters.size > 0) {
        const qMap = { 'q1':'q1','q2':'q2','q3':'q3','q4':'q4','first quarter':'q1','second quarter':'q2','third quarter':'q3','fourth quarter':'q4' };
        if ([...queryQuarters].some(q => qMap[q] === fileQ)) score += 8;
      }

      // ── 6. Keyword match in filename ──────────────────────
      for (const kw of keywords) {
        if (nameLower.includes(kw)) score += 2;
      }

      // ── 7. PDF preferred over xlsx ────────────────────────
      if (nameLower.endsWith('.xlsx') || nameLower.endsWith('.xls')) score -= 1;

      // ── 8. Recency boost (up to +3) ───────────────────────
      const age = f.uploadedAt ? Date.now() - new Date(f.uploadedAt).getTime() : 0;
      score += Math.max(0, 3 - Math.floor(age / (1000 * 60 * 60 * 24 * 30)));

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

    // Build stats context
    const statsLines = [];
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

    // Get best chunks — carry file-level _score as boost so range-matched files dominate top slots
    // When few files in collection, give more chunks per file for deeper coverage
    const keywords2 = extractKeywords(message);
    const chunksPerFile = fullFiles.length <= 2 ? 12 : fullFiles.length <= 5 ? 6 : 3;
    const totalChunkLimit = fullFiles.length <= 2 ? 20 : fullFiles.length <= 5 ? 20 : 15;

    // Guaranteed slots: FILE SUMMARY chunk (chunk[0]) from each file that has one.
    // These are always included and don't compete in the scoring race.
    const guaranteedChunks = [];
    const seenSummaryFiles = new Set();
    for (const fileData of fullFiles) {
      if (!fileData.chunks || !fileData.chunks.length) continue;
      const first = fileData.chunks[0];
      if ((first.toLowerCase().startsWith('file summary') || first.toLowerCase().startsWith('sheet:')) && !seenSummaryFiles.has(fileData.name)) {
        guaranteedChunks.push({ chunk: first, score: 999, filename: fileData.name, collection: fileData.collection, meta: fileData.meta || {}, _guaranteed: true });
        seenSummaryFiles.add(fileData.name);
      }
    }

    const allChunks = [];
    for (const fileData of fullFiles) {
      if (!fileData.chunks) continue;
      const fileBoost = fileData._score || 0;
      const fileChunks = fileData.chunks.map(chunk => ({
        chunk,
        score: scoreChunk(chunk, keywords2) + fileBoost,
        filename: fileData.name, collection: fileData.collection, meta: fileData.meta || {}
      })).filter(c => c.score >= 0); // score=-1 means summary chunk — excluded from race
      fileChunks.sort((a, b) => b.score - a.score);
      allChunks.push(...fileChunks.slice(0, chunksPerFile));
    }

    allChunks.sort((a, b) => b.score - a.score);
    // Merge: guaranteed summaries first, then scored chunks (deduplicate by content)
    const guaranteedTexts = new Set(guaranteedChunks.map(c => c.chunk));
    const scoredOnly = allChunks.filter(c => !guaranteedTexts.has(c.chunk));
    const remaining = totalChunkLimit - guaranteedChunks.length;
    const top = [...guaranteedChunks, ...scoredOnly.slice(0, Math.max(remaining, 0))];

    if (!top.length) return { text: '', sources: [], statsContext: statsLines.join('\n'), focusFile: fullFiles[0]?.name };

    const sources = [...new Set(top.map(x => x.filename))];
    const text = top.map(x => {
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
    let filesToSearch = [];

    if (scope === 'file' && fileId) {
      const f = await env.CACI_KV.get(`file:${fileId}`, 'json');
      if (f) filesToSearch = [f];
    } else {
      const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
      const stopWords = new Set(['the','and','for','all','from','with','that','this','are','was','were','has','have','report','reports']);
      const msgLower = message.toLowerCase();
      const matchedCol = reg.find(c => {
        if (msgLower.includes(c.name.toLowerCase())) return true;
        if (c.category && msgLower.includes(c.category.toLowerCase())) return true;
        const words = c.name.toLowerCase().split(/[\s&,\/]+/).filter(w => w.length >= 4 && !stopWords.has(w));
        return words.some(w => msgLower.includes(w));
      });
      if (matchedCol) {
        const colFiles = await env.CACI_KV.get(`col:${dept}:${matchedCol.name}`, 'json') || [];
        const colFull = (await Promise.all(colFiles.map(f => env.CACI_KV.get(`file:${f.id}`, 'json')))).filter(Boolean);
        const deptIdx = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
        const colIds = new Set(colFiles.map(f => f.id));
        const otherMeta = deptIdx.filter(f => !colIds.has(f.id)).slice(0, 20);
        const otherFull = (await Promise.all(otherMeta.map(f => env.CACI_KV.get(`file:${f.id}`, 'json')))).filter(Boolean);
        filesToSearch = [...colFull, ...otherFull];
      } else {
        const deptIdx   = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
        const globalIdx = dept !== 'global' ? (await env.CACI_KV.get('index:global', 'json') || []) : [];
        const allMeta   = [...deptIdx, ...globalIdx].slice(0, 40);
        filesToSearch   = (await Promise.all(allMeta.map(f => env.CACI_KV.get(`file:${f.id}`, 'json')))).filter(Boolean);
      }
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

    // Date range detection — same logic as buildContextTwoPass
    const { rangeStart: ctxRangeStart, rangeEnd: ctxRangeEnd } = parseDateRange(message);
    const yearMatches = message.match(/20\d\d/g) || [];
    const queryYears = new Set(yearMatches);

    // Score each file by date relevance, then propagate into chunk scores
    const fileScoreMap = new Map();
    for (const fileData of filesToSearch) {
      const nameLower = (fileData.name || '').toLowerCase();
      let fScore = 0;
      // Range match
      if (ctxRangeStart && ctxRangeEnd) {
        const { fileMonth, fileYear } = fileMonthYear(nameLower);
        if (fileMonth && fileYear) {
          const fileVal  = fileYear * 100 + fileMonth;
          const startVal = ctxRangeStart.y * 100 + ctxRangeStart.m;
          const endVal   = ctxRangeEnd.y   * 100 + ctxRangeEnd.m;
          if (fileVal >= startVal && fileVal <= endVal) fScore += 30;
        }
      }
      // Year match with larger multiplier
      const matchedYears = queryYears.size > 0 ? [...queryYears].filter(y => nameLower.includes(y)) : [];
      if (matchedYears.length > 0) {
        const maxYear = Math.max(...matchedYears.map(Number));
        fScore += 10 + (maxYear - 2020) * 3;
      }
      const isXlsx = nameLower.endsWith('.xlsx') || nameLower.endsWith('.xls');
      if (isXlsx) fScore -= 1;
      fileScoreMap.set(fileData.name, fScore);
    }

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

    const sources = [...new Set(top.map(x => x.filename))];
    const text = top.map(x => {
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
    if (!apiKey) return json({ error: 'Anthropic API key not configured' }, 400);

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

    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 300,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    if (!res.ok) { const e = await res.text(); return json({ error: e }, 500); }
    const data   = await res.json();
    const result = data.content?.[0]?.text || '';

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
    if (!apiKey) return json({ error: 'Anthropic API key not configured' }, 400);

    const prompt = `You are classifying a business document for a cannabis company (Jushi Holdings).

Document filename: "${fileName}"
Current collection: "${collection}"
Available collections: ${colNames || 'none yet'}

First 2500 characters of document text:
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

    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 350,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    if (!res.ok) {
      const err = await res.text();
      return json({ error: `Claude error: ${err}` }, 500);
    }

    const data = await res.json();
    const result = data.content?.[0]?.text || '';
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

// ── Helpers ───────────────────────────────────────────────────
function chunkText(text, size = 1500) {
  if (!text || text.length === 0) return [];
  const chunks = [];
  for (let i = 0; i < text.length; i += size - 200) {
    chunks.push(text.slice(i, i + size));
    if (i + size >= text.length) break;
  }
  return chunks;
}

function extractKeywords(query) {
  const stop = new Set(['a','an','the','is','are','was','were','be','been','have','has','had',
    'do','does','did','will','would','could','should','may','might','what','which','who',
    'this','that','these','those','i','me','my','we','our','you','your','he','she','it',
    'they','them','and','but','or','for','at','by','in','of','on','to','as','if','how',
    'when','where','why','with','about','from','into','can','tell','show','give','get','all',
    'report','reports','data','file','files','show','list','give','find','between','across',
    'total','totals','number','numbers','amount','amounts','value','values']);
  return query.toLowerCase().replace(/[^a-z0-9\s]/g,' ').split(/\s+/).filter(w => w.length > 2 && !stop.has(w));
}

// Escape special regex characters so keywords like "$" or "." don't misbehave
function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function scoreChunk(chunk, keywords) {
  const lower = chunk.toLowerCase();
  // Skip pure summary blocks — they match everything and dilute the race
  // (Summary chunks are guaranteed-included separately in buildContextTwoPass)
  if (lower.startsWith('file summary\n') || lower.startsWith('file summary\r\n')) return -1;
  let score = 0;
  for (const kw of keywords) {
    // Use word-boundary-aware matching with regex escape
    const re = new RegExp('(?<![a-z0-9])' + escapeRegex(kw) + '(?![a-z0-9])', 'gi');
    const matches = lower.match(re);
    if (matches) {
      // Column-value pattern gets double weight: "State: PA" is stronger than incidental "pa" match
      const colValueRe = new RegExp('[a-z_ ]+:\s*' + escapeRegex(kw) + '\\b', 'gi');
      const colMatches = lower.match(colValueRe);
      score += matches.length + (colMatches ? colMatches.length : 0);
    }
  }
  return score;
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
