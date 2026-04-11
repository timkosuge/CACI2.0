// ─────────────────────────────────────────────────────────────
//  CACI Worker v3 — Collections support
//  Structure: dept → collection → files
// ─────────────────────────────────────────────────────────────

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
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
    if (path === '/health') return json({ ok: true, version: '3.0.0' });

    if (!verifyToken(request, env)) return json({ error: 'Unauthorized' }, 401);

    // Files
    if (path === '/upload' && method === 'POST')           return handleUpload(request, env);
    if (path === '/files' && method === 'GET')             return handleListFiles(url, env);
    if (path.startsWith('/files/') && method === 'DELETE') return handleDeleteFile(path.replace('/files/', ''), env);

    // Collections
    if (path === '/collections' && method === 'GET')                       return handleListCollections(url, env);
    if (path.startsWith('/collections/') && method === 'DELETE')           return handleDeleteCollection(path.replace('/collections/', ''), url, env);

    // Chat
    if (path === '/chat' && method === 'POST') return handleChat(request, env);

    // Admin
    if (path === '/admin/config' && method === 'POST') return handleAdminSave(request, env);
    if (path === '/admin/config' && method === 'GET')  return handleAdminGet(env);

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

// ── Upload ────────────────────────────────────────────────────
async function handleUpload(request, env) {
  try {
    const formData = await request.formData();
    const file       = formData.get('file');
    const text       = formData.get('text') || '';
    const name       = formData.get('name') || (file ? file.name : 'unknown');
    const dept       = formData.get('dept') || 'global';
    const collection = (formData.get('collection') || 'General').trim();
    const fileType   = formData.get('type') || 'unknown';

    if (!text && !file) return json({ error: 'No content provided' }, 400);

    const id          = crypto.randomUUID();
    const uploadedAt  = new Date().toISOString();
    const cleanText   = text.replace(/\s+/g, ' ').trim();
    const chunks      = chunkText(cleanText, 1500);

    // Store raw file in R2
    if (file && env.CACI_R2) {
      const buffer = await file.arrayBuffer();
      await env.CACI_R2.put(`${dept}/${collection}/${id}/${name}`, buffer, {
        httpMetadata: { contentType: file.type || 'application/octet-stream' },
        customMetadata: { dept, collection, name, uploadedAt },
      });
    }

    // Store text chunks in KV
    await env.CACI_KV.put(
      `file:${id}`,
      JSON.stringify({ id, name, dept, collection, fileType, uploadedAt, chunks, charCount: cleanText.length }),
      { expirationTtl: 60 * 60 * 24 * 365 }
    );

    // Update dept file index
    const deptKey = `index:${dept}`;
    const deptIdx = await env.CACI_KV.get(deptKey, 'json') || [];
    deptIdx.unshift({ id, name, dept, collection, fileType, uploadedAt, charCount: cleanText.length, chunks: chunks.length });
    if (deptIdx.length > 500) deptIdx.splice(500);
    await env.CACI_KV.put(deptKey, JSON.stringify(deptIdx));

    // Update collection index
    const colKey = `col:${dept}:${collection}`;
    const colIdx = await env.CACI_KV.get(colKey, 'json') || [];
    colIdx.unshift({ id, name, dept, collection, fileType, uploadedAt, charCount: cleanText.length, chunks: chunks.length });
    await env.CACI_KV.put(colKey, JSON.stringify(colIdx));

    // Update collection registry for this dept
    const regKey = `colreg:${dept}`;
    const reg    = await env.CACI_KV.get(regKey, 'json') || [];
    if (!reg.find(c => c.name === collection)) {
      reg.unshift({ name: collection, created: uploadedAt });
      await env.CACI_KV.put(regKey, JSON.stringify(reg));
    }

    return json({ ok: true, id, name, collection, chunks: chunks.length, charCount: cleanText.length });
  } catch (err) {
    return json({ error: 'Upload failed: ' + err.message }, 500);
  }
}

// ── List Files ────────────────────────────────────────────────
// ?dept=retail               → all files in dept
// ?dept=retail&col=Q1 Shrink → files in that collection
// ?dept=retail&fileId=xxx    → single file meta
async function handleListFiles(url, env) {
  try {
    const dept   = url.searchParams.get('dept') || 'global';
    const col    = url.searchParams.get('col');
    const fileId = url.searchParams.get('fileId');

    if (fileId) {
      const f = await env.CACI_KV.get(`file:${fileId}`, 'json');
      return f ? json(f) : json({ error: 'Not found' }, 404);
    }

    if (col) {
      const files = await env.CACI_KV.get(`col:${dept}:${col}`, 'json') || [];
      return json(files);
    }

    const files = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
    return json(files);
  } catch { return json([]); }
}

// ── List Collections ──────────────────────────────────────────
// ?dept=retail → [{name, created, fileCount}]
async function handleListCollections(url, env) {
  try {
    const dept = url.searchParams.get('dept') || 'global';
    const reg  = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];

    // Enrich with file counts
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
    if (!fileMeta) return json({ error: 'File not found' }, 404);

    const { dept, collection, name } = fileMeta;

    // Remove from KV
    await env.CACI_KV.delete(`file:${id}`);

    // Remove from dept index
    const deptIdx = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
    await env.CACI_KV.put(`index:${dept}`, JSON.stringify(deptIdx.filter(f => f.id !== id)));

    // Remove from collection index
    const colKey = `col:${dept}:${collection}`;
    const colIdx = await env.CACI_KV.get(colKey, 'json') || [];
    const newColIdx = colIdx.filter(f => f.id !== id);
    await env.CACI_KV.put(colKey, JSON.stringify(newColIdx));

    // If collection now empty, remove from registry
    if (newColIdx.length === 0) {
      const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
      await env.CACI_KV.put(`colreg:${dept}`, JSON.stringify(reg.filter(c => c.name !== collection)));
      await env.CACI_KV.delete(colKey);
    }

    // Remove from R2
    if (env.CACI_R2) {
      await env.CACI_R2.delete(`${dept}/${collection}/${id}/${name}`).catch(() => {});
    }

    return json({ ok: true });
  } catch (err) {
    return json({ error: 'Delete failed: ' + err.message }, 500);
  }
}

// ── Delete Collection ─────────────────────────────────────────
async function handleDeleteCollection(encodedName, url, env) {
  try {
    const dept = url.searchParams.get('dept') || 'global';
    const name = decodeURIComponent(encodedName);

    const colKey = `col:${dept}:${name}`;
    const files  = await env.CACI_KV.get(colKey, 'json') || [];

    // Delete all files in collection
    for (const f of files) {
      await env.CACI_KV.delete(`file:${f.id}`);
      // Remove from dept index
      const deptIdx = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
      await env.CACI_KV.put(`index:${dept}`, JSON.stringify(deptIdx.filter(x => x.id !== f.id)));
      // R2
      if (env.CACI_R2) await env.CACI_R2.delete(`${dept}/${name}/${f.id}/${f.name}`).catch(() => {});
    }

    // Remove collection
    await env.CACI_KV.delete(colKey);
    const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
    await env.CACI_KV.put(`colreg:${dept}`, JSON.stringify(reg.filter(c => c.name !== name)));

    return json({ ok: true, deleted: files.length });
  } catch (err) {
    return json({ error: 'Delete collection failed: ' + err.message }, 500);
  }
}

// ── Chat ──────────────────────────────────────────────────────
// scope: 'all' | 'collection' | 'file'
async function handleChat(request, env) {
  try {
    const { message, dept, collection, fileId, scope = 'all', model, history = [] } = await request.json();
    if (!message) return json({ error: 'Message required' }, 400);

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured. Go to Admin to add it.' }, 400);

    // Build context based on scope
    const context = await buildContext({ message, dept, collection, fileId, scope, env });

    // System prompt
    const scopeLabel = scope === 'file' ? `the file "${context.focusFile || fileId}"`
      : scope === 'collection' ? `the "${collection}" collection`
      : `all ${dept} documents`;

    let system = `You are CACI, an internal AI intelligence assistant for Jushi Holdings. You are currently analyzing ${scopeLabel} in the ${dept} department.

Your role:
- Answer questions accurately from the provided company documents
- Summarize, compare, and analyze data across multiple files when relevant
- Generate professional internal reports when asked
- Always cite the specific document or file name when referencing data
- If data is not in the provided documents, say so clearly — never fabricate numbers or data`;

    if (context.text) {
      system += `\n\n--- DOCUMENTS ---\n${context.text}\n--- END ---\n\nAnswer from these documents. Cite filenames when referencing specific data.`;
    } else {
      system += `\n\nNo documents found for this scope. Ask the user to upload relevant files first.`;
    }

    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 2048,
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
  } catch (err) {
    return json({ error: 'Chat error: ' + err.message }, 500);
  }
}

// ── Context Builder ───────────────────────────────────────────
async function buildContext({ message, dept, collection, fileId, scope, env }) {
  try {
    const keywords = extractKeywords(message);
    let filesToSearch = [];

    if (scope === 'file' && fileId) {
      // Single file
      const f = await env.CACI_KV.get(`file:${fileId}`, 'json');
      if (f) filesToSearch = [f];
    } else if (scope === 'collection' && collection) {
      // All files in collection
      const colFiles = await env.CACI_KV.get(`col:${dept}:${collection}`, 'json') || [];
      filesToSearch = await Promise.all(colFiles.map(f => env.CACI_KV.get(`file:${f.id}`, 'json')));
      filesToSearch = filesToSearch.filter(Boolean);
    } else {
      // All files in dept + global
      const deptIdx  = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
      const globalIdx = dept !== 'global' ? (await env.CACI_KV.get('index:global', 'json') || []) : [];
      const allMeta  = [...deptIdx, ...globalIdx].slice(0, 40);
      filesToSearch  = (await Promise.all(allMeta.map(f => env.CACI_KV.get(`file:${f.id}`, 'json')))).filter(Boolean);
    }

    if (!filesToSearch.length) return { text: '', sources: [], focusFile: null };

    // Score chunks across all files
    const scored = [];
    for (const fileData of filesToSearch) {
      if (!fileData.chunks) continue;
      for (const chunk of fileData.chunks) {
        const score = scoreChunk(chunk, keywords);
        if (score > 0) scored.push({ chunk, score, filename: fileData.name, collection: fileData.collection });
      }
    }

    scored.sort((a, b) => b.score - a.score);
    const top = scored.slice(0, 8);

    // If no keyword matches, fall back to first chunks of each file (up to 3 files)
    if (!top.length) {
      const fallback = filesToSearch.slice(0, 3).flatMap(f =>
        (f.chunks || []).slice(0, 2).map(chunk => ({ chunk, filename: f.name, collection: f.collection }))
      );
      if (!fallback.length) return { text: '', sources: [], focusFile: null };
      const sources = [...new Set(fallback.map(x => x.filename))];
      const text = fallback.map(x => `[${x.collection} / ${x.filename}]\n${x.chunk}`).join('\n\n---\n\n');
      return { text, sources, focusFile: filesToSearch[0]?.name };
    }

    const sources = [...new Set(top.map(x => x.filename))];
    const text = top.map(x => `[${x.collection} / ${x.filename}]\n${x.chunk}`).join('\n\n---\n\n');
    return { text, sources, focusFile: filesToSearch[0]?.name };
  } catch (err) {
    console.error('Context error:', err.message);
    return { text: '', sources: [], focusFile: null };
  }
}

// ── Admin Config ──────────────────────────────────────────────
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
    'when','where','why','with','about','from','into','can','tell','show','give','get']);
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
