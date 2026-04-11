// ─────────────────────────────────────────────────────────────
//  CACI — Single-File Cloudflare Worker
//  No external dependencies. No module splitting.
// ─────────────────────────────────────────────────────────────

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

function verifyToken(request, env) {
  const auth = request.headers.get('Authorization') || '';
  const token = auth.replace('Bearer ', '').trim();
  return token && (token === env.SESSION_SECRET || token === 'dev-token');
}

// ── Main Router ───────────────────────────────────────────────
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    if (method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    // Login — no auth required
    if (path === '/auth/login' && method === 'POST') {
      return handleLogin(request, env);
    }

    // Health — no auth required
    if (path === '/health') {
      return json({ ok: true, version: '2.0.0' });
    }

    // All other routes require valid token
    if (!verifyToken(request, env)) {
      return json({ error: 'Unauthorized' }, 401);
    }

    // Upload file (text already extracted client-side)
    if (path === '/upload' && method === 'POST') {
      return handleUpload(request, env);
    }

    // List files for a department
    if (path === '/files' && method === 'GET') {
      const dept = url.searchParams.get('dept') || 'global';
      return handleListFiles(dept, env);
    }

    // Delete a file
    if (path.startsWith('/files/') && method === 'DELETE') {
      const id = path.replace('/files/', '');
      return handleDeleteFile(id, env);
    }

    // Chat
    if (path === '/chat' && method === 'POST') {
      return handleChat(request, env);
    }

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
  } catch {
    return json({ error: 'Bad request' }, 400);
  }
}

// ── Upload ────────────────────────────────────────────────────
// The frontend extracts text from files before sending.
// We receive: file (raw binary for R2), text (extracted), name, dept, type
async function handleUpload(request, env) {
  try {
    const formData = await request.formData();
    const file = formData.get('file');         // raw file blob
    const text = formData.get('text') || '';   // extracted text from client
    const name = formData.get('name') || (file ? file.name : 'unknown');
    const dept = formData.get('dept') || 'global';
    const type = formData.get('type') || 'unknown';

    if (!file && !text) return json({ error: 'No file or text provided' }, 400);

    const id = crypto.randomUUID();
    const uploadedAt = new Date().toISOString();

    // 1. Store raw file in R2
    if (file && env.CACI_R2) {
      const buffer = await file.arrayBuffer();
      await env.CACI_R2.put(`${dept}/${id}/${name}`, buffer, {
        httpMetadata: { contentType: file.type || 'application/octet-stream' },
        customMetadata: { dept, name, uploadedAt },
      });
    }

    // 2. Store extracted text chunks in KV
    // Chunk into ~1500 char pieces for retrieval
    const cleanText = text.replace(/\s+/g, ' ').trim();
    const chunks = chunkText(cleanText, 1500);

    await env.CACI_KV.put(
      `file:${id}`,
      JSON.stringify({ id, name, dept, type, uploadedAt, chunks, charCount: cleanText.length }),
      { expirationTtl: 60 * 60 * 24 * 365 } // 1 year
    );

    // 3. Update department file index
    const indexKey = `index:${dept}`;
    const existing = await env.CACI_KV.get(indexKey, 'json') || [];
    existing.unshift({ id, name, dept, type, uploadedAt, charCount: cleanText.length, chunks: chunks.length });
    // Keep index to 200 files max
    if (existing.length > 200) existing.splice(200);
    await env.CACI_KV.put(indexKey, JSON.stringify(existing));

    return json({ ok: true, id, name, chunks: chunks.length, charCount: cleanText.length });
  } catch (err) {
    return json({ error: 'Upload failed: ' + err.message }, 500);
  }
}

// ── List Files ────────────────────────────────────────────────
async function handleListFiles(dept, env) {
  try {
    const files = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
    return json(files);
  } catch {
    return json([]);
  }
}

// ── Delete File ───────────────────────────────────────────────
async function handleDeleteFile(id, env) {
  try {
    const fileMeta = await env.CACI_KV.get(`file:${id}`, 'json');
    if (!fileMeta) return json({ error: 'File not found' }, 404);

    // Remove from KV
    await env.CACI_KV.delete(`file:${id}`);

    // Remove from index
    const indexKey = `index:${fileMeta.dept}`;
    const existing = await env.CACI_KV.get(indexKey, 'json') || [];
    await env.CACI_KV.put(indexKey, JSON.stringify(existing.filter(f => f.id !== id)));

    // Remove from R2
    if (env.CACI_R2) {
      await env.CACI_R2.delete(`${fileMeta.dept}/${id}/${fileMeta.name}`).catch(() => {});
    }

    return json({ ok: true });
  } catch (err) {
    return json({ error: 'Delete failed: ' + err.message }, 500);
  }
}

// ── Chat ──────────────────────────────────────────────────────
async function handleChat(request, env) {
  try {
    const { message, dept, model, history = [] } = await request.json();
    if (!message) return json({ error: 'Message required' }, 400);

    // Get Claude API key
    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) {
      return json({ error: 'Anthropic API key not configured. Add it in Admin settings.' }, 400);
    }

    // Retrieve relevant file context from KV
    const context = await buildContext(message, dept, env);

    // Build system prompt
    const deptLabel = dept ? dept.charAt(0).toUpperCase() + dept.slice(1) : 'General';
    let systemPrompt = `You are CACI, an internal AI intelligence assistant for Jushi Holdings, a cannabis company. You are operating in the ${deptLabel} department.

Your responsibilities:
- Answer questions accurately from company documents
- Summarize reports, data, and documents clearly
- Generate professional internal reports when asked
- Always cite the document name when referencing specific data
- If data is not in the provided documents, say so clearly — never fabricate numbers`;

    if (context.text) {
      systemPrompt += `\n\n--- RELEVANT COMPANY DOCUMENTS ---\n${context.text}\n--- END DOCUMENTS ---\n\nAnswer based on the documents above. Cite filenames when referencing data.`;
    } else {
      systemPrompt += `\n\nNo documents were found for this query in the ${deptLabel} department. Let the user know they should upload relevant documents first.`;
    }

    // Call Claude
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 2048,
        system: systemPrompt,
        messages: [
          ...history.slice(-10).map(h => ({ role: h.role, content: h.content })),
          { role: 'user', content: message },
        ],
      }),
    });

    if (!res.ok) {
      const errText = await res.text();
      return json({ error: `Claude API error (${res.status}): ${errText}` }, 500);
    }

    const data = await res.json();
    const reply = data.content?.[0]?.text || 'No response.';

    return json({
      ok: true,
      response: reply,
      sources: context.sources,
      dept,
    });

  } catch (err) {
    return json({ error: 'Chat error: ' + err.message }, 500);
  }
}

// ── Context Builder ───────────────────────────────────────────
// Simple keyword search across all files in the department.
// No embeddings, no Vectorize — just reliable KV lookups.
async function buildContext(query, dept, env) {
  try {
    const keywords = extractKeywords(query);

    // Get file index for this department (+ global)
    const deptFiles = await env.CACI_KV.get(`index:${dept}`, 'json') || [];
    const globalFiles = dept !== 'global'
      ? (await env.CACI_KV.get('index:global', 'json') || [])
      : [];
    const allFiles = [...deptFiles, ...globalFiles].slice(0, 30); // cap at 30 files to check

    if (!allFiles.length) return { text: '', sources: [] };

    // Score each file by keyword relevance
    const scored = [];
    for (const fileMeta of allFiles) {
      const fileData = await env.CACI_KV.get(`file:${fileMeta.id}`, 'json');
      if (!fileData || !fileData.chunks) continue;

      // Score each chunk
      const chunkScores = fileData.chunks.map(chunk => ({
        chunk,
        score: scoreChunk(chunk, keywords),
        filename: fileMeta.name,
      }));

      const topChunks = chunkScores
        .filter(c => c.score > 0)
        .sort((a, b) => b.score - a.score)
        .slice(0, 3);

      scored.push(...topChunks);
    }

    // Sort all chunks by score, take top 6
    scored.sort((a, b) => b.score - a.score);
    const top = scored.slice(0, 6);

    if (!top.length) {
      // No keyword matches — fall back to most recent file's first chunk
      if (allFiles.length) {
        const first = await env.CACI_KV.get(`file:${allFiles[0].id}`, 'json');
        if (first && first.chunks && first.chunks.length) {
          return {
            text: `[From: ${allFiles[0].name}]\n${first.chunks[0]}`,
            sources: [allFiles[0].name],
          };
        }
      }
      return { text: '', sources: [] };
    }

    const sources = [...new Set(top.map(t => t.filename))];
    const text = top.map(t => `[From: ${t.filename}]\n${t.chunk}`).join('\n\n---\n\n');

    return { text, sources };
  } catch (err) {
    console.error('Context error:', err.message);
    return { text: '', sources: [] };
  }
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
  const stopWords = new Set(['a','an','the','is','are','was','were','be','been','being',
    'have','has','had','do','does','did','will','would','could','should','may','might',
    'shall','can','need','dare','ought','used','what','which','who','whom','this','that',
    'these','those','i','me','my','we','our','you','your','he','she','it','they','them',
    'his','her','its','their','and','but','or','nor','for','yet','so','at','by','in',
    'of','on','to','up','as','if','how','when','where','why','with','about','from','into']);
  return query.toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length > 2 && !stopWords.has(w));
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
