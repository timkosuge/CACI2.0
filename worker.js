// ─────────────────────────────────────────────────────────────
//  CACI Worker v4 — Metadata, Collections, Computed Stats,
//  Duplicate Detection, Report Generation
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
    if (path === '/health') return json({ ok: true, version: '4.0.0' });

    if (!verifyToken(request, env)) return json({ error: 'Unauthorized' }, 401);

    if (path === '/upload'    && method === 'POST')   return handleUpload(request, env);
    if (path === '/duplicate-check' && method === 'POST') return handleDuplicateCheck(request, env);
    if (path === '/files'     && method === 'GET')    return handleListFiles(url, env);
    if (path.startsWith('/files/') && path.endsWith('/meta') && method === 'PATCH') return handlePatchFileMeta(path, request, env);
    if (path.startsWith('/files/') && method === 'DELETE') return handleDeleteFile(path.replace('/files/', ''), env);
    if (path === '/collections' && method === 'GET')  return handleListCollections(url, env);
    if (path === '/collections/create' && method === 'POST') return handleCreateCollection(request, env);
    if (path.startsWith('/collections/') && method === 'DELETE') return handleDeleteCollection(path.replace('/collections/', ''), url, env);
    if (path === '/chat'      && method === 'POST')   return handleChat(request, env);
    if (path === '/report'    && method === 'POST')   return handleReport(request, env);
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

    const name      = meta.fileName || (file ? file.name : 'unknown');
    const category  = meta.category || formData.get('category') || 'General';
    const period    = meta.period   || formData.get('period')   || '';
    // Use explicitly sent collection name if provided, otherwise build from fields
    const sentCollection = formData.get('collection') || '';
    const collection = sentCollection || (period ? `${category} — ${period}` : category);

    if (!text && !file) return json({ error: 'No content provided' }, 400);

    const id         = crypto.randomUUID();
    const uploadedAt = new Date().toISOString();
    const cleanText  = text.replace(/\s+/g, ' ').trim();
    const chunks     = chunkText(cleanText, 1500);

    // Store raw file in R2
    if (file && env.CACI_R2) {
      const buffer = await file.arrayBuffer();
      await env.CACI_R2.put(`${dept}/${collection}/${id}/${name}`, buffer, {
        httpMetadata: { contentType: file.type || 'application/octet-stream' },
        customMetadata: { dept, collection, name, uploadedAt },
      });
    }

    // File record — includes metadata + computed stats
    const fileRecord = {
      id, name, dept, collection, uploadedAt,
      charCount: cleanText.length,
      chunks: chunks.length,
      meta,   // reportName, category, period, state, store, reportType
      stats,  // rowCount, columns, numericSummaries
    };

    // Store text + stats in KV
    await env.CACI_KV.put(
      `file:${id}`,
      JSON.stringify({ ...fileRecord, chunks }),
      { expirationTtl: 60 * 60 * 24 * 365 }
    );

    // Update dept index (no chunks — keep index lean)
    const deptKey = `index:${dept}`;
    const deptIdx = await env.CACI_KV.get(deptKey, 'json') || [];
    deptIdx.unshift(fileRecord);
    if (deptIdx.length > 500) deptIdx.splice(500);
    await env.CACI_KV.put(deptKey, JSON.stringify(deptIdx));

    // Update collection index
    const colKey = `col:${dept}:${collection}`;
    const colIdx = await env.CACI_KV.get(colKey, 'json') || [];
    colIdx.unshift(fileRecord);
    await env.CACI_KV.put(colKey, JSON.stringify(colIdx));

    // Update collection registry
    const regKey = `colreg:${dept}`;
    const reg    = await env.CACI_KV.get(regKey, 'json') || [];
    const fileDeptMeta = formData.get('fileDept') || dept;
    if (!reg.find(c => c.name === collection)) {
      reg.unshift({ name: collection, dept: fileDeptMeta, category, period, created: uploadedAt });
      await env.CACI_KV.put(regKey, JSON.stringify(reg));
    }

    const isContext = formData.get('isContext') === 'true';
    if (isContext) {
      // Store in context index for this collection
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
      // All files for the dept that have no collection or are in an auto-named collection
      const allFiles = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
      const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
      const namedCols = new Set(reg.map(c => c.name));
      files = allFiles.filter(f => !f.isContext && (!f.collection || !namedCols.has(f.collection)));
    } else {
      files = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
      files = files.filter(f => !f.isContext);
    }

    // Apply filters
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

    // Merge changes
    if (name)       stored.name = name;
    if (collection !== undefined) stored.collection = collection;
    if (!stored.meta) stored.meta = {};
    if (reportName  !== undefined) stored.meta.reportName  = reportName;
    if (category    !== undefined) stored.meta.category    = category;
    if (period      !== undefined) stored.meta.period      = period;
    if (reportType  !== undefined) stored.meta.reportType  = reportType;

    await env.CACI_KV.put(`file:${id}`, JSON.stringify(stored));

    // Update dept index
    const deptKey = `index:${oldDept}`;
    const deptIdx = await env.CACI_KV.get(deptKey, 'json') || [];
    const deptEntry = deptIdx.find(f => f.id === id);
    if (deptEntry) {
      Object.assign(deptEntry, { name: stored.name, collection: stored.collection, meta: stored.meta });
      await env.CACI_KV.put(deptKey, JSON.stringify(deptIdx));
    }

    // Update old collection index — remove or update entry
    if (oldCollection) {
      const oldColKey = `col:${oldDept}:${oldCollection}`;
      const oldColIdx = await env.CACI_KV.get(oldColKey, 'json') || [];
      if (collection && collection !== oldCollection) {
        // Move to new collection
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
async function handleDeleteFile(id, env) {
  try {
    const fileMeta = await env.CACI_KV.get(`file:${id}`, 'json');

    // Even if the file record is missing, sweep all indexes to remove any trace
    if (!fileMeta) {
      // Scan dept indexes to find and remove the orphaned entry
      const depts = ['global','retail','compliance','commercial','human_resources','finance','operations','technology'];
      for (const d of depts) {
        const deptIdx = await env.CACI_KV.get(`index:${d}`, 'json') || [];
        const filtered = deptIdx.filter(f => f.id !== id);
        if (filtered.length !== deptIdx.length) {
          await env.CACI_KV.put(`index:${d}`, JSON.stringify(filtered));
          // Also sweep collection and ctx indexes for this dept
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

    // Remove from dept index
    const deptIdx = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
    await env.CACI_KV.put(`index:${dept}`, JSON.stringify(deptIdx.filter(f => f.id !== id)));

    // Remove from collection index
    const colKey = `col:${dept}:${collection}`;
    const colIdx = await env.CACI_KV.get(colKey, 'json') || [];
    const newColIdx = colIdx.filter(f => f.id !== id);
    await env.CACI_KV.put(colKey, JSON.stringify(newColIdx));

    // Remove from context index
    const ctxKey = `ctx:${dept}:${collection}`;
    const ctxIdx = await env.CACI_KV.get(ctxKey, 'json') || [];
    const newCtxIdx = ctxIdx.filter(f => f.id !== id);
    if (newCtxIdx.length !== ctxIdx.length) {
      await env.CACI_KV.put(ctxKey, JSON.stringify(newCtxIdx));
    }

    // If collection fully empty, remove from registry
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

// ── Chat ──────────────────────────────────────────────────────
async function handleChat(request, env) {
  try {
    const { message, dept, collection, fileId, scope = 'all', model, history = [] } = await request.json();
    if (!message) return json({ error: 'Message required' }, 400);

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured. Go to Admin to add it.' }, 400);

    const context = await buildContext({ message, dept, collection, fileId, scope, env });

    // Load collection context docs (SOPs, rules, calendars)
    let contextDocs = '';
    if (collection) {
      const ctxFiles = await env.CACI_KV.get(`ctx:${dept}:${collection}`, 'json') || [];
      if (ctxFiles.length) {
        const ctxTexts = [];
        for (const f of ctxFiles.slice(0, 5)) {
          const full = await env.CACI_KV.get(`file:${f.id}`, 'json');
          if (full?.chunks) ctxTexts.push(`[Context: ${f.name}]\n${full.chunks.join(' ')}`);
        }
        if (ctxTexts.length) contextDocs = ctxTexts.join('\n\n');
      }
    }

    const scopeLabel = scope === 'file' ? `the document "${context.focusFile}"`
      : scope === 'collection' ? `the "${collection}" collection`
      : `all ${dept} documents`;

    let system = `You are CACI, an internal AI intelligence assistant for Jushi Holdings, a vertically integrated multi-state cannabis operator. You are analyzing ${scopeLabel} in the ${dept} department.

Your capabilities:
- Answer questions accurately from company documents and data
- Analyze trends, variances, and patterns across multiple files
- Compare data across time periods, states, and store locations
- Generate professional executive-level insights and reports
- Always cite the specific document name and period when referencing data
- Never fabricate numbers — if data is not present, say so clearly

When analyzing data, note the metadata context (period, location, category) to give accurate, temporally-aware answers.`;

    if (context.statsContext) {
      system += `\n\nCOMPUTED DATA SUMMARIES (pre-calculated, use these for precise numbers):\n${context.statsContext}`;
    }

    if (contextDocs) {
      system += `\n\nCOLLECTION CONTEXT (SOPs, rules, calendars — read these first and use them to interpret all data):\n${contextDocs}`;
    }

    if (context.text) {
      system += `\n\nDOCUMENT CONTENT:\n${context.text}\n\nBase your answers on the above. Cite document names and periods when referencing specific data.`;
    } else {
      system += `\n\nNo documents found for this scope. Ask the user to upload relevant files first.`;
    }

    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 3000,
        system,
        messages: [
          ...history.slice(-10).map(h => ({ role: h.role, content: h.content })),
          { role: 'user', content: message },
        ],
      }),
    });

    if (!res.ok) {
      const err = await res.text();
      return json({ error: `Claude API error (${res.status}): ${err}` }, 500);
    }

    const data = await res.json();
    return json({ ok: true, response: data.content?.[0]?.text || 'No response.', sources: context.sources, scope });
  } catch (err) { return json({ error: 'Chat error: ' + err.message }, 500); }
}

// ── Report Generation ─────────────────────────────────────────
async function handleReport(request, env) {
  try {
    const { prompt, dept, collection, fileId, scope = 'all', format = 'markdown' } = await request.json();

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured.' }, 400);

    const context = await buildContext({ message: prompt, dept, collection, fileId, scope, env });

    const reportPrompt = `You are generating a professional internal business report for Jushi Holdings executive team.

Report request: ${prompt}

${context.statsContext ? `COMPUTED DATA SUMMARIES:\n${context.statsContext}\n` : ''}
${context.text ? `SOURCE DOCUMENTS:\n${context.text}\n` : ''}

Generate a comprehensive, well-structured professional report. Include:
1. Executive Summary (2-3 sentences)
2. Key Findings (bullet points with specific numbers)
3. Detailed Analysis (organized by relevant categories)
4. Period-over-Period Comparison (if multiple periods present)
5. State/Store Breakdown (if location data present)
6. Recommendations
7. Data Sources

Format the report in clean Markdown. Use ## for sections, **bold** for key metrics, tables where appropriate. Be precise with numbers. Cite source files.`;

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

// ── Context Builder ───────────────────────────────────────────
async function buildContext({ message, dept, collection, fileId, scope, env }) {
  try {
    const keywords = extractKeywords(message);
    let filesToSearch = [];

    if (scope === 'file' && fileId) {
      const f = await env.CACI_KV.get(`file:${fileId}`, 'json');
      if (f) filesToSearch = [f];
    } else if (scope === 'collection' && collection) {
      const colFiles = await env.CACI_KV.get(`col:${dept}:${collection}`, 'json') || [];
      filesToSearch = (await Promise.all(colFiles.map(f => env.CACI_KV.get(`file:${f.id}`, 'json')))).filter(Boolean);
    } else {
      const deptIdx   = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
      const globalIdx = dept !== 'global' ? (await env.CACI_KV.get('index:global', 'json') || []) : [];
      const allMeta   = [...deptIdx, ...globalIdx].slice(0, 40);
      filesToSearch   = (await Promise.all(allMeta.map(f => env.CACI_KV.get(`file:${f.id}`, 'json')))).filter(Boolean);
    }

    if (!filesToSearch.length) return { text: '', sources: [], statsContext: '', focusFile: null };

    // Build stats context from computed summaries
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
      }
    }

    // Score chunks
    const scored = [];
    for (const fileData of filesToSearch) {
      if (!fileData.chunks) continue;
      const meta = fileData.meta || {};
      for (const chunk of fileData.chunks) {
        const score = scoreChunk(chunk, keywords);
        if (score > 0) scored.push({ chunk, score, filename: fileData.name, collection: fileData.collection, meta });
      }
    }

    scored.sort((a, b) => b.score - a.score);
    const top = scored.slice(0, 8);

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

// ── Admin ─────────────────────────────────────────────────────
async function handleAdminSave(request, env) {
  try {
    const body = await request.json();
    const saved = [];
    for (const key of ['ANTHROPIC_API_KEY']) {
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
    const kvKey = await env.CACI_KV.get('config:ANTHROPIC_API_KEY');
    return json({
      ANTHROPIC_API_KEY: {
        configured: !!(kvKey || env.ANTHROPIC_API_KEY),
        source: kvKey ? 'admin' : env.ANTHROPIC_API_KEY ? 'secret' : 'none',
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
    'when','where','why','with','about','from','into','can','tell','show','give','get','all']);
  return query.toLowerCase().replace(/[^a-z0-9\s]/g,' ').split(/\s+/).filter(w => w.length > 2 && !stop.has(w));
}

function scoreChunk(chunk, keywords) {
  const lower = chunk.toLowerCase();
  let score = 0;
  for (const kw of keywords) {
    const count = (lower.match(new RegExp(kw, 'g')) || []).length;
    score += count;
  }
  return score;
}

// ─────────────────────────────────────────────────────────────
//  INTEGRATIONS  (Microsoft 365, QuickBase)
//  Keys stored in KV under  integ:{id}  as JSON
//  Secrets stored in KV under  integ-secret:{id}
// ─────────────────────────────────────────────────────────────

const ALLOWED_INTEGRATIONS = ['excel','word','teams','powerbi','quickbase'];

async function handleIntegrationSave(path, request, env) {
  const id = path.replace('/integrations/', '').split('/')[0];
  if (!ALLOWED_INTEGRATIONS.includes(id)) return json({ error: 'Unknown integration' }, 400);
  try {
    const body = await request.json();
    // Separate secrets from metadata
    const secrets = {};
    const meta    = { id, connectedAt: new Date().toISOString(), dept: body.dept || 'global' };

    if (id === 'excel' || id === 'word') {
      secrets.clientSecret = body.secret || '';
      meta.tenantId        = body.tenant ? body.tenant.slice(0,8) + '…' : '';
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
    // list all
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
//  Keys stored in KV under  con:{id}  /  con-secret:{id}
//  Live data fetch proxied to avoid CORS / exposing keys
// ─────────────────────────────────────────────────────────────

const ALLOWED_CONNECTORS = ['dutchie','metrc','iheartjane','leaftrade','mjfreeway'];

const CONNECTOR_ENDPOINTS = {
  dutchie:    { base: 'https://api.dutchie.com/v1',              verify: '/store' },
  metrc:      { base: 'https://api-{state}.metrc.com/v3',         verify: '/facilities' },
  iheartjane: { base: 'https://api.iheartjane.com/v1',            verify: '/stores/{store}' },
  leaftrade:  { base: 'https://api.leaftrade.com/api/v1',         verify: '/company/profile/' },
  mjfreeway:  { base: null /* dynamic */,                          verify: '/company' },
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

    // Optionally attempt a verify ping
    const verified = await verifyConnector(id, meta, secrets);
    return json({ ok: true, id, verified });
  } catch(e) { return json({ error: e.message }, 500); }
}

async function verifyConnector(id, meta, secrets) {
  try {
    if (id === 'dutchie') {
      const r = await fetch('https://api.dutchie.com/v1/store', {
        headers: { Authorization: 'Bearer ' + secrets.apiKey }
      });
      return r.ok;
    }
    if (id === 'metrc') {
      const state = (meta.state || 'co').toLowerCase();
      const creds = btoa(secrets.swKey + ':' + secrets.userKey);
      const r = await fetch('https://api-' + state + '.metrc.com/v3/facilities', {
        headers: { Authorization: 'Basic ' + creds }
      });
      return r.ok;
    }
    if (id === 'iheartjane') {
      const r = await fetch('https://api.iheartjane.com/v1/stores/' + meta.storeId, {
        headers: { Authorization: 'Bearer ' + secrets.apiKey }
      });
      return r.ok;
    }
    if (id === 'leaftrade') {
      const r = await fetch('https://api.leaftrade.com/api/v1/company/profile/', {
        headers: { Authorization: 'Token ' + secrets.apiKey }
      });
      return r.ok;
    }
    if (id === 'mjfreeway') {
      const r = await fetch((meta.baseUrl || 'https://api.mjfreeway.com/v1') + '/company', {
        headers: { Authorization: 'Bearer ' + secrets.token }
      });
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
  return json(meta);  // never returns secrets
}

async function handleConnectorDelete(path, env) {
  const id = path.replace('/connectors/', '').split('/')[0];
  await env.CACI_KV.delete('con:' + id);
  await env.CACI_KV.delete('con-secret:' + id);
  return json({ ok: true });
}

// Proxy live data from connector to avoid exposing API keys to browser
async function handleConnectorFetch(path, request, env) {
  const id = path.replace('/connectors/', '').split('/fetch')[0];
  const meta    = await env.CACI_KV.get('con:' + id, 'json');
  const secrets = await env.CACI_KV.get('con-secret:' + id, 'json');
  if (!meta || !secrets) return json({ error: 'Connector not configured' }, 404);

  const body     = await request.json().catch(() => ({}));
  const endpoint = body.endpoint || '';  // e.g. "/inventory", "/sales"
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
