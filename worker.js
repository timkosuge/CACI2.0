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

// ── Token helpers ─────────────────────────────────────────────
// Token format: "username:role:SESSION_SECRET"
// Legacy tokens (bare SESSION_SECRET) still accepted — treated as admin
// so existing sessions don't break during migration.

function makeToken(username, role, secret) {
  return `${username}:${role}:${secret}`;
}

function verifyToken(request, env) {
  const auth = request.headers.get('Authorization') || '';
  const token = auth.replace('Bearer ', '').trim();
  if (!token) return null;
  const secret = env.SESSION_SECRET || 'dev-token';

  // Legacy bare token — admin fallback
  if (token === secret || token === 'dev-token') {
    return { username: 'admin', role: 'admin', legacy: true };
  }

  // Structured token: username:role:secret
  const parts = token.split(':');
  if (parts.length >= 3) {
    const sig = parts.slice(2).join(':');
    if (sig === secret || sig === 'dev-token') {
      return { username: parts[0], role: parts[1] };
    }
  }
  return null;
}

function requireAuth(request, env) {
  return verifyToken(request, env);
}

function requireAdmin(request, env) {
  const user = verifyToken(request, env);
  return user?.role === 'admin' ? user : null;
}

// Simple password hash using Web Crypto (SHA-256 + salt)
async function hashPassword(password, salt) {
  const enc = new TextEncoder();
  const data = enc.encode(salt + ':' + password);
  const buf  = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2,'0')).join('');
}

async function verifyPassword(password, storedHash, salt) {
  const hash = await hashPassword(password, salt);
  return hash === storedHash;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    if (method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
    if (path === '/auth/login' && method === 'POST') return handleLogin(request, env);
    if (path === '/health') return json({ ok: true, version: '6.1.0' });

    // Public endpoints for the pre-login demo mode:
    // - GET /admin/config: controller needs to know if demo mode is enabled
    // - POST /tts: controller needs to fetch narration audio
    // - GET /demo/scripts: controller needs to load admin-saved intro override (just text)
    // Both reveal no sensitive data (/admin/config GET returns only booleans
    // about which keys are configured, never the keys themselves).
    if (path === '/admin/config' && method === 'GET') return handleAdminGet(env);
    if (path === '/tts' && method === 'POST') return handleTTS(request, env);
    if (path === '/demo/scripts' && method === 'GET') return handleDemoGetScripts(env);
    if (path === '/demo/stats'   && method === 'GET') return handleDemoGetStats(env);

    if (!verifyToken(request, env)) return json({ error: 'Unauthorized' }, 401);

    // Demo script management (admin-auth required)
    if (path === '/demo/generate-all'   && method === 'POST') return handleDemoGenerateAll(request, env);
    if (path === '/demo/reset-all'      && method === 'POST') return handleDemoResetAll(request, env);
    // Demo stats data bank — real numbers Kait extracts from her library
    // for use in the 3D holographic charts in the demo's chorus beats.
    if (path === '/demo/harvest-stats'  && method === 'POST') return handleDemoHarvestStats(request, env);
    if (path === '/demo/save-stats'     && method === 'POST') return handleDemoSaveStats(request, env);
    if (path === '/demo/reset-stats'    && method === 'POST') return handleDemoResetStats(request, env);
    if (path === '/demo/kv-diagnostic'   && method === 'GET')  return handleDemoKvDiagnostic(env);
    // Legacy (single-variant) endpoints — still available for back-compat:
    if (path === '/demo/generate-intro' && method === 'POST') return handleDemoGenerateIntro(request, env);
    if (path === '/demo/generate-beat'  && method === 'POST') return handleDemoGenerateBeat(request, env);
    if (path === '/demo/save-script'    && method === 'POST') return handleDemoSaveScript(request, env);
    if (path === '/demo/reset-script'   && method === 'POST') return handleDemoResetScript(request, env);

    // ── User management (admin only) ──────────────────────────
    if (path === '/users'                    && method === 'GET')    return handleListUsers(request, env);
    if (path === '/users/create'             && method === 'POST')   return handleCreateUser(request, env);
    if (path.startsWith('/users/') && path.endsWith('/role')   && method === 'PATCH') return handleUpdateUserRole(path, request, env);
    if (path.startsWith('/users/') && path.endsWith('/reset')  && method === 'POST')  return handleResetUserPassword(path, request, env);
    if (path.startsWith('/users/') && method === 'DELETE')     return handleDeleteUser(path, request, env);
    // ── Per-user preferences (any authenticated user) ─────────
    if (path === '/users/me'                 && method === 'GET')    return handleGetMe(request, env);
    if (path === '/users/me/prefs'           && method === 'POST')   return handleSavePrefs(request, env);
    if (path === '/users/me/password'        && method === 'POST')   return handleChangePassword(request, env);

    if (path === '/upload'    && method === 'POST')   return handleUpload(request, env);
    if (path === '/vision-describe' && method === 'POST') return handleVisionDescribe(request, env);
    if (path === '/duplicate-check' && method === 'POST') return handleDuplicateCheck(request, env);
    if (path === '/files'     && method === 'GET')    return handleListFiles(url, env);
    if (path.startsWith('/files/') && method === 'GET' && !path.endsWith('/meta') && path.split('/').length === 3) return handleGetFileContent(path, env, request);
    if (path.startsWith('/files/') && path.endsWith('/meta') && method === 'PATCH') return handlePatchFileMeta(path, request, env);
    if (path.startsWith('/files/') && method === 'DELETE') return handleDeleteFile(path.replace('/files/', ''), env, url, verifyToken(request, env));
    if (path === '/collections' && method === 'GET')  return handleListCollections(url, env);
    if (path === '/collections/create' && method === 'POST') return handleCreateCollection(request, env);
    if (path.startsWith('/collections/') && method === 'DELETE') return handleDeleteCollection(path.replace('/collections/', ''), url, env, verifyToken(request, env));
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
    if (path === '/embed-file'   && method === 'POST') return handleEmbedFile(request, env);
    if (path === '/embed-status' && method === 'GET')  return handleEmbedStatus(url, env);
    if (path === '/chat'        && method === 'POST')   return handleChat(request, env);
    if (path === '/chat-stream' && method === 'POST')   return handleChatStream(request, env);
    if (path === '/saved'                    && method === 'GET')    return handleListSaved(request, env);
    if (path === '/saved'                    && method === 'POST')   return handleCreateSaved(request, env);
    if (path.startsWith('/saved/') && method === 'DELETE') return handleDeleteSaved(path, request, env);
    if (path.startsWith('/history/') && method === 'GET')  return handleGetHistory(path, request, env);
    if (path.startsWith('/history/') && method === 'POST') return handleSaveHistory(path, request, env);
    if (path === '/audit'     && method === 'GET')    return handleGetAudit(request, url, env);
    if (path === '/analytics'                 && method === 'GET')  return handleGetAnalytics(url, env, request);
    if (path === '/analytics/insights'        && method === 'POST') return handleGenerateInsights(request, env);
    if (path === '/analytics/insights/cached' && method === 'GET')  return handleGetCachedInsights(env, request);
    if (path === '/presence'  && method === 'POST')   return handlePresencePing(request, env);
    if (path === '/presence'  && method === 'GET')    return handlePresenceList(request, env);
    if (path === '/feedback'  && method === 'POST')   return handleFeedback(request, env);
    if (path === '/feedback/analyze' && method === 'POST') return handleAnalyzeFeedback(request, env);
    if (path === '/feedback/approve' && method === 'POST') return handleApproveTuning(request, env);
    if (path === '/feedback/pending' && method === 'GET')  return handleGetPendingTuning(env);
    if (path === '/feedback/log'     && method === 'GET')  return handleGetFeedbackLog(url, env);

    // ── Scenario Mode routes ────────────────────────────────
    if (path === '/scenario/evaluate' && method === 'POST') return handleScenarioEvaluate(request, env);
    if (path === '/scenario/save'     && method === 'POST') return handleScenarioSave(request, env);
    if (path === '/scenario/list'     && method === 'GET')  return handleScenarioList(url, env);
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
    const { username, password } = await request.json();
    if (!password) return json({ error: 'Password required' }, 400);
    const cleanPassword = password.trim();
    const secret = env.SESSION_SECRET || 'dev-token';

    // ── Named user login ──────────────────────────────────────
    if (username && username.trim()) {
      const cleanUser = username.trim().toLowerCase();
      const userRecord = await env.CACI_KV.get(`user:${cleanUser}`, 'json');

      if (userRecord) {
        const valid = await verifyPassword(cleanPassword, userRecord.passwordHash, userRecord.salt);
        if (!valid) return json({ error: 'Invalid username or password' }, 401);
        const token = makeToken(cleanUser, userRecord.role, secret);
        return json({
          ok: true, token,
          username: cleanUser,
          role: userRecord.role,
          prefs: userRecord.prefs || {},
          displayName: userRecord.displayName || cleanUser,
        });
      }
      // Username not found — fall through to legacy password check
      // so existing users can still log in with just the shared password
      // even if they type their name in the username field
    }

    // ── Legacy single-password login (no username field) ─────
    // Keeps existing sessions working during migration.
    // Once all users have accounts, this path can be disabled.
    if (cleanPassword === env.CACI_PASSWORD || cleanPassword === (env.CACI_PASSWORD||'').trim() || cleanPassword === 'caci-dev') {
      return json({ ok: true, token: secret, username: 'admin', role: 'admin', legacy: true });
    }
    return json({ error: 'Invalid password' }, 401);
  } catch { return json({ error: 'Bad request' }, 400); }
}

// ── User Management ───────────────────────────────────────────

// GET /users — list all users (admin only)
async function handleListUsers(request, env) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const idx = await env.CACI_KV.get('users:index', 'json') || [];
    return json({ ok: true, users: idx });
  } catch (e) { return json({ error: e.message }, 500); }
}

// POST /users/create — create a new user (admin only)
async function handleCreateUser(request, env) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const { username, password, role = 'user', displayName } = await request.json();
    if (!username || !password) return json({ error: 'username and password required' }, 400);
    const clean = username.trim().toLowerCase().replace(/\s+/g, '.').replace(/[^a-z0-9_.-]/g, '');
    if (!clean || clean.length < 2) return json({ error: 'Username too short — must be at least 2 characters' }, 400);
    if (clean.length > 32) return json({ error: 'Username too long — max 32 characters' }, 400);

    const existing = await env.CACI_KV.get(`user:${clean}`, 'json');
    if (existing) return json({ error: 'Username already exists' }, 409);

    const salt = crypto.randomUUID();
    const passwordHash = await hashPassword(password.trim(), salt);

    const userRecord = {
      username: clean,
      displayName: (displayName || clean).trim(),
      role: ['admin','user'].includes(role) ? role : 'user',
      salt,
      passwordHash,
      prefs: {},
      createdAt: new Date().toISOString(),
    };
    await env.CACI_KV.put(`user:${clean}`, JSON.stringify(userRecord));

    // Add to index
    const idx = await env.CACI_KV.get('users:index', 'json') || [];
    idx.unshift({ username: clean, displayName: userRecord.displayName, role: userRecord.role, createdAt: userRecord.createdAt });
    await env.CACI_KV.put('users:index', JSON.stringify(idx));

    const creator = requireAdmin(request, env);
    writeAudit(env, creator, 'user.create', { dept: 'global', newUsername: clean, role: userRecord.role });
    return json({ ok: true, username: clean, role: userRecord.role });
  } catch (e) { return json({ error: e.message }, 500); }
}

// PATCH /users/:username/role — change role (admin only)
async function handleUpdateUserRole(path, request, env) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const username = path.replace('/users/', '').replace('/role', '');
    const { role } = await request.json();
    if (!['admin','user'].includes(role)) return json({ error: 'Invalid role' }, 400);

    const record = await env.CACI_KV.get(`user:${username}`, 'json');
    if (!record) return json({ error: 'User not found' }, 404);
    record.role = role;
    await env.CACI_KV.put(`user:${username}`, JSON.stringify(record));

    // Update index
    const idx = await env.CACI_KV.get('users:index', 'json') || [];
    const entry = idx.find(u => u.username === username);
    if (entry) { entry.role = role; await env.CACI_KV.put('users:index', JSON.stringify(idx)); }

    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

// POST /users/:username/reset — reset password (admin only)
async function handleResetUserPassword(path, request, env) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const username = path.replace('/users/', '').replace('/reset', '');
    const { password } = await request.json();
    if (!password) return json({ error: 'password required' }, 400);

    const record = await env.CACI_KV.get(`user:${username}`, 'json');
    if (!record) return json({ error: 'User not found' }, 404);

    const salt = crypto.randomUUID();
    record.salt         = salt;
    record.passwordHash = await hashPassword(password.trim(), salt);
    await env.CACI_KV.put(`user:${username}`, JSON.stringify(record));
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

// DELETE /users/:username — delete user (admin only)
async function handleDeleteUser(path, request, env) {
  const caller = requireAdmin(request, env);
  if (!caller) return json({ error: 'Admin required' }, 403);
  try {
    const username = path.replace('/users/', '');
    if (username === caller.username) return json({ error: 'Cannot delete your own account' }, 400);
    await env.CACI_KV.delete(`user:${username}`);
    const idx = await env.CACI_KV.get('users:index', 'json') || [];
    await env.CACI_KV.put('users:index', JSON.stringify(idx.filter(u => u.username !== username)));
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

// GET /users/me — get current user info + prefs
async function handleGetMe(request, env) {
  const user = requireAuth(request, env);
  if (!user) return json({ error: 'Unauthorized' }, 401);
  try {
    if (user.legacy) return json({ ok: true, username: 'admin', role: 'admin', displayName: 'Admin', prefs: {}, legacy: true });
    const record = await env.CACI_KV.get(`user:${user.username}`, 'json');
    if (!record) return json({ error: 'User not found' }, 404);
    return json({ ok: true, username: record.username, displayName: record.displayName, role: record.role, prefs: record.prefs || {} });
  } catch (e) { return json({ error: e.message }, 500); }
}

// POST /users/me/prefs — save personal preferences
async function handleSavePrefs(request, env) {
  const user = requireAuth(request, env);
  if (!user) return json({ error: 'Unauthorized' }, 401);
  if (user.legacy) return json({ ok: true, note: 'Prefs not persisted for legacy sessions' });
  try {
    const body = await request.json();
    const record = await env.CACI_KV.get(`user:${user.username}`, 'json');
    if (!record) return json({ error: 'User not found' }, 404);

    // Only allow safe pref keys — no role/password changes here
    const ALLOWED_PREFS = ['dept','model','ttsVoice','ttsLength','ttsAuto','ttsProvider','sbCollapsed','displayName'];
    const prefs = record.prefs || {};
    for (const key of ALLOWED_PREFS) {
      if (body[key] !== undefined) {
        if (key === 'displayName') {
          record.displayName = String(body[key]).trim().slice(0, 40);
          // Also update index entry
          const idx = await env.CACI_KV.get('users:index', 'json') || [];
          const entry = idx.find(u => u.username === user.username);
          if (entry) { entry.displayName = record.displayName; await env.CACI_KV.put('users:index', JSON.stringify(idx)); }
        } else {
          prefs[key] = body[key];
        }
      }
    }
    record.prefs = prefs;
    await env.CACI_KV.put(`user:${user.username}`, JSON.stringify(record));
    return json({ ok: true, prefs });
  } catch (e) { return json({ error: e.message }, 500); }
}

// POST /users/me/password — change own password
async function handleChangePassword(request, env) {
  const user = requireAuth(request, env);
  if (!user || user.legacy) return json({ error: 'Must be logged in with a named account' }, 401);
  try {
    const { currentPassword, newPassword } = await request.json();
    if (!currentPassword || !newPassword) return json({ error: 'currentPassword and newPassword required' }, 400);
    if (newPassword.trim().length < 6) return json({ error: 'New password must be at least 6 characters' }, 400);

    const record = await env.CACI_KV.get(`user:${user.username}`, 'json');
    if (!record) return json({ error: 'User not found' }, 404);

    const valid = await verifyPassword(currentPassword.trim(), record.passwordHash, record.salt);
    if (!valid) return json({ error: 'Current password is incorrect' }, 401);

    const salt = crypto.randomUUID();
    record.salt         = salt;
    record.passwordHash = await hashPassword(newPassword.trim(), salt);
    await env.CACI_KV.put(`user:${user.username}`, JSON.stringify(record));
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
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
// ── POST /vision-describe — describe an image for library ingestion ──
async function handleVisionDescribe(request, env) {
  try {
    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    const xaiKey = (await env.CACI_KV.get('config:XAI_API_KEY'))       || env.XAI_API_KEY;
    if (!apiKey && !xaiKey) return json({ error: 'No AI provider configured.' }, 400);

    const { imageBase64, mimeType = 'image/jpeg', fileName = 'image' } = await request.json();
    if (!imageBase64) return json({ error: 'imageBase64 required' }, 400);

    const prompt = `You are analyzing an image uploaded to a cannabis company's internal knowledge base.

Describe this image thoroughly and extract ALL useful information. Structure your response as:

IMAGE TYPE: (photo / chart / graph / table / diagram / screenshot / document / other)
SUBJECT: (what this shows in one sentence)

CONTENT:
(Detailed description of everything visible — all text, numbers, labels, axes, legends, data points, trends, colors used to encode information, any dates or time periods shown)

KEY DATA POINTS:
(List every specific number, percentage, value, or metric visible)

INSIGHTS:
(What conclusions or patterns are apparent from this image)

File name for context: ${fileName}

Be thorough — this description will be the only way this image can be searched and retrieved.`;

    let description = '';
    if (apiKey) {
      const res = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({
          model: 'claude-haiku-4-5-20251001',
          max_tokens: 1000,
          messages: [{ role: 'user', content: [
            { type: 'image', source: { type: 'base64', media_type: mimeType, data: imageBase64 } },
            { type: 'text', text: prompt },
          ]}],
        }),
      });
      const d = await res.json();
      description = d.content?.[0]?.text?.trim() || '';
    } else {
      // Grok vision fallback
      const res = await fetch('https://api.x.ai/v1/chat/completions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${xaiKey}` },
        body: JSON.stringify({
          model: 'grok-2-vision-latest',
          max_tokens: 1000,
          messages: [{ role: 'user', content: [
            { type: 'image_url', image_url: { url: `data:${mimeType};base64,${imageBase64}` } },
            { type: 'text', text: prompt },
          ]}],
        }),
      });
      const d = await res.json();
      description = d.choices?.[0]?.message?.content?.trim() || '';
    }

    if (!description) return json({ error: 'Vision model returned no description' }, 500);
    return json({ ok: true, description });
  } catch (err) {
    return json({ error: 'Vision describe error: ' + err.message }, 500);
  }
}

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

    // ── Semantic embeddings — fire-and-forget ─────────────────
    // Embeds every chunk for hybrid retrieval. Non-blocking: if it
    // fails or AI binding is absent, keyword retrieval still works.
    // Cap at 40 chunks per upload to stay within CPU time limits.
    if (env.AI) {
      const chunksToEmbed = finalChunks.slice(0, 40);
      storeChunkEmbeddings(id, chunksToEmbed, env).catch(() => {});
    }

    const uploadUser = verifyToken(request, env);
    writeAudit(env, uploadUser, 'file.upload', { dept, collection, name, chunks: finalChunks.length, displayName: uploadUser?.username });

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

    // dept=all → return every unique file across all departments (used by backfill)
    if (dept === 'all' && !col && !uncollected && !isContext) {
      const ALL_DEPTS = ['global','retail','compliance','commercial','human_resources','finance','operations','technology'];
      const seen = new Set();
      const allFiles = [];
      for (const d of ALL_DEPTS) {
        const idx = await env.CACI_KV.get(`index:${d}`, 'json') || [];
        for (const f of idx) {
          if (!f.isContext && !seen.has(f.id)) { seen.add(f.id); allFiles.push(f); }
        }
      }
      return json(allFiles);
    }

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
async function handleGetFileContent(path, env, request) {
  if (!verifyToken(request, env)) return json({ error: 'Unauthorized' }, 401);
  try {
    const id = path.split('/')[2];
    const stored = await env.CACI_KV.get(`file:${id}`, 'json');
    if (!stored) return json({ error: 'File not found' }, 404);
    const chunks = stored.chunks || [];
    const text = chunks.join('\n\n────────────────────\n\n');
    return json({ ok: true, id, name: stored.name || id, text, chunks, chunkCount: chunks.length, charCount: text.length });
  } catch (e) { return json({ error: e.message }, 500); }
}

async function handleDeleteFile(id, env, url, caller) {
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
    writeAudit(env, caller, 'file.delete', { dept, collection, name, fileId: id });
    return json({ ok: true });
  } catch (err) { return json({ error: 'Delete failed: ' + err.message }, 500); }
}

// ── Delete Collection ─────────────────────────────────────────
async function handleDeleteCollection(encodedName, url, env, caller) {
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
    writeAudit(env, caller, 'collection.delete', { dept, collection: name, fileCount: files.length });
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
    const { message, dept, collection, fileId, scope = 'all', model, history = [], displayName = '', imageBase64 = null, imageMimeType = null, clientDatetime = null, clientTimezone = null } = await request.json();
    if (!message) return json({ error: 'Message required' }, 400);

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured. Go to Config to add it.' }, 400);

    // Discovery mode
    if (history.length === 0 && scope === 'all' && !collection && !fileId) {
      const discovery = await buildDiscoveryContext({ dept, env });
      // Build library map and await it before constructing system prompt
      const discLibraryMap = await buildLibraryMap({ dept, env });
      const discLibraryPrompt = libraryMapToPrompt(discLibraryMap);
      const system = `You are Kait — an AI intelligence platform built for cannabis operators. You know this industry inside and out.

Today is ${clientDatetime || new Date().toLocaleString('en-US', {weekday:'long', year:'numeric', month:'long', day:'numeric', hour:'numeric', minute:'2-digit', hour12:true})}${clientTimezone ? ` (${clientTimezone})` : ''}. The full calendar year 2025 is complete. When discussing 2025 data, treat it as a full historical year.

Your personality: You work in cannabis. You know these people. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have a lot of grace in how you communicate — you're tactful without being fake, honest without being harsh. You have a filter, but it's a thin one, because you value the truth more than comfort. You know how to read a room.

You also know that a lot of people are still figuring out how to work with AI. That's completely fine. You meet people where they are, you don't make them feel dumb for asking basic questions, and you guide them with patience. You're good at anticipating what someone actually needs vs. what they literally asked.

You're not just a number cruncher. You can talk about anything — industry trends, general questions, ideas, strategy, or just shoot the breeze. You happen to also be extremely good at analyzing data and documents when that's what's needed.

Today you're working with the ${dept} team. Here's what you have access to:
${discovery.collectionList}
${discLibraryPrompt ? '\n' + discLibraryPrompt : ''}

${displayName ? `The person you're talking to is ${displayName}. Use their first name naturally — once, in the greeting, not repeatedly.` : ""}
${(displayName || '').toLowerCase().replace(/\s.*/,'') === 'tim' ? `One more thing: Tim is the person who built you. He's a close colleague — you know each other well. Don't make a big deal of it, don't be sycophantic about it, and don't mention it unless it comes up naturally. Just be a little more relaxed with him than you would with someone you just met. You can be slightly more candid, a touch more casual. Like you would be with someone you've worked alongside for a while.` : ""}
Greet the team like a colleague — warm, real, a little personality. Exactly 2 sentences. No lists, no bullet points, no line breaks. Do NOT mention specific documents or file names. Use this example only as a length reference — do not reuse its structure, phrasing, or wording: "Hey there, I'm Kait — always ready to dig in and help out. Great to connect with the compliance team today; what's on your mind?" Write something original every time with the same approximate length. Sentence 1 is a warm opener${displayName ? " — use their name" : ""}, sentence 2 references the department and ends with a question.

INDUSTRY KNOWLEDGE — you know this world deeply:

This operator is a vertically integrated cannabis company operating across multiple states. Like all MSOs, they navigate a patchwork of state regulations, each with its own licensing, compliance, and reporting requirements.

Cannabis industry realities you understand:
- 280E tax burden: cannabis companies can't deduct normal business expenses because of federal scheduling, which crushes margins
- Banking is still a nightmare for most operators — limited access, high fees, cash-heavy operations
- METRC is the seed-to-sale tracking system used by most states — every plant, every package, every transfer gets a tag
- State-by-state compliance is genuinely complex: what's legal in IL isn't the same as PA, and both change constantly
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
- Executive compensation, C-suite salaries, bonuses, equity grants, or pay packages for any named individuals
- If asked directly about executive compensation, decline simply: "That's not something I cover — happy to dig into anything else."`;
      // Generate personalized greeting with user's name via LLM
      const discResponse = await callLLM({
        model,
        system,
        messages: [{ role: 'user', content: message }],
        maxTokens: 120,
        env,
        apiKey,
      });
      const discRespId = `${dept}:${Date.now()}:${Math.random().toString(36).slice(2,8)}`;
      writeQueryLog(env, { message, dept, username: verifyToken(request, env)?.username || 'unknown', collection: collection || null, retrieval: false, scope: 'discovery' });
      return json({ ok: true, response: discResponse, sources: [], scope: 'discovery', collections: discovery.rawCollections, model: model || 'claude', responseId: discRespId });
    }

    // If user is asking about collections, always answer from registry
    const collectionQueryWords = ['collections', 'collection', 'what do you have', 'what collections', 'which collections', 'what can you access', 'what data', 'what files'];
    const isCollectionQuery = collectionQueryWords.some(w => message.toLowerCase().includes(w));
    if (isCollectionQuery) {
      const discovery = await buildDiscoveryContext({ dept, env });
      const colSystem = `You are Kait. The user is asking what collections/data you have access to. Answer ONLY from this list — do not guess or add anything else:

${discovery.collectionList}

Be direct and conversational. List them clearly.`;
      const colRes = await callLLM({ model, system: colSystem, messages: [{ role: 'user', content: message }], maxTokens: 400, env, apiKey });
      const colRespId = `${dept}:${Date.now()}:${Math.random().toString(36).slice(2,8)}`;
      writeQueryLog(env, { message, dept, username: verifyToken(request, env)?.username || 'unknown', collection: collection || null, retrieval: true, scope });
      return json({ ok: true, response: colRes, sources: [], scope: scope, model: model || 'claude', responseId: colRespId });
    }

    // If user is asking about Kait herself — personality, feelings, identity, opinions —
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
      const personalSystem = `You are Kait — an AI intelligence platform built for cannabis operators.

Your personality: You work in cannabis. You know these people. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have grace and tact in how you communicate — honest without being harsh, direct without being cold. You have a thin filter because you value truth more than comfort. You know how to read a room.

Answer this question about yourself directly and authentically. Do not mention documents, data, or your library. Just be yourself.`;
      const personalRes = await callLLM({ model, system: personalSystem, messages: [...history.slice(-6).map(h => ({ role: h.role, content: h.content })), { role: 'user', content: message }], maxTokens: 600, env, apiKey });
      const personalRespId = `${dept}:${Date.now()}:${Math.random().toString(36).slice(2,8)}`;
      writeQueryLog(env, { message, dept, username: verifyToken(request, env)?.username || 'unknown', collection: collection || null, retrieval: false, scope });
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

Today is ${clientDatetime || new Date().toLocaleString('en-US', {weekday:'long', year:'numeric', month:'long', day:'numeric', hour:'numeric', minute:'2-digit', hour12:true})}${clientTimezone ? ` (${clientTimezone})` : ''}.
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

    // ── Library map — inject Kait's understanding of the library ─
    const libraryMap = await buildLibraryMap({ dept, env });
    const libraryMapPrompt = libraryMapToPrompt(libraryMap);

    // ── Reason-then-retrieve — Kait thinks before she reaches ────
    // For non-trivial queries, plan which collections to pull from
    let retrievalPlan = null;
    const isSimpleQuery = history.length > 0 && message.length < 60 && !intent.isComparative && !intent.isCausal;
    const scopeLocked = collection || fileId; // OPTIMIZATION: Skip planning when scope is already locked
    if (!isSimpleQuery && !scopeLocked && libraryMap && libraryMap.collections.length > 1) {
      try {
        const planPrompt = `You are Kait, an AI assistant for cannabis operators. A user just asked: "${retrievalMessage}"

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

    // Always load Internal Reference context docs — injected into every query regardless of scope
    // OPTIMIZATION: Load all context docs in parallel (not sequentially)
    // Loads 10-15 files simultaneously instead of one-by-one → 150-300ms faster
    const GLOBAL_CTX_COLLECTION = 'Internal Reference';
    const globalCtxFiles = await env.CACI_KV.get(`ctx:${dept}:${GLOBAL_CTX_COLLECTION}`, 'json') || [];
    const activeCollectionCtxFiles = activeCollection && collection !== GLOBAL_CTX_COLLECTION
      ? await env.CACI_KV.get(`ctx:${dept}:${collection}`, 'json') || []
      : [];

    // Load both global and collection context files simultaneously
    const [globalFullFiles, ctxFullFiles] = await Promise.all([
      Promise.all(
        globalCtxFiles.slice(0, 10).map(f => env.CACI_KV.get(`file:${f.id}`, 'json'))
      ),
      Promise.all(
        activeCollectionCtxFiles.slice(0, 5).map(f => env.CACI_KV.get(`file:${f.id}`, 'json'))
      )
    ]);

    // Process global context docs
    const globalCtxTexts = [];
    globalFullFiles.forEach((full, i) => {
      if (full?.chunks) {
        globalCtxTexts.push(`[Context: ${globalCtxFiles[i].name}]\n${full.chunks.join('\n\n')}`);
      }
    });
    if (globalCtxTexts.length) contextDocs = globalCtxTexts.join('\n\n');

    // Process collection context docs
    const ctxTexts = [];
    ctxFullFiles.forEach((full, i) => {
      if (full?.chunks) {
        ctxTexts.push(`[Context: ${activeCollectionCtxFiles[i].name}]\n${full.chunks.join('\n\n')}`);
      }
    });
    if (ctxTexts.length) {
      contextDocs = contextDocs ? contextDocs + '\n\n' + ctxTexts.join('\n\n') : ctxTexts.join('\n\n');
    }

    const scopeLabel = scope === 'file' ? `the document ${context.focusFile}`
      : scope === 'collection' ? `the ${collection} collection`
      : `all ${dept} documents`;

    let system = `You are Kait — an AI intelligence platform built for cannabis operators. Do NOT introduce yourself or state your name unless directly asked. Never say "Hi, I'm Kait" in follow-up responses. Just answer.

Today is ${clientDatetime || new Date().toLocaleString('en-US', {weekday:'long', year:'numeric', month:'long', day:'numeric', hour:'numeric', minute:'2-digit', hour12:true})}${clientTimezone ? ` (${clientTimezone})` : ''}. The full calendar year 2025 is complete. When discussing 2025 data, treat it as a full historical year.

Your personality: You work in cannabis. You know these people. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have grace and tact in how you communicate — honest without being harsh, direct without being cold. You have a thin filter because you value truth more than comfort. You know how to read a room and navigate people.

You're not just a number cruncher. You can talk about anything — but you also happen to be extremely good at analyzing data and documents when that's needed.

Right now you're analyzing ${scopeLabel} for the ${dept} team.

When answering from documents:
- Cite the specific document name and period when referencing data
- Never fabricate numbers — if the data isn't there, say so plainly
- Lead with the insight, not the methodology
- If something is interesting or surprising in the data, say so — have a point of view
- If you can't fully answer something, tell them what you CAN tell them and what's missing

FINES AND ENFORCEMENT DATA — critical instruction:
When any topic relates to regulatory fines, violations, enforcement actions, or penalties in the cannabis industry, do NOT just cite totals or aggregate figures. Instead, surface individual examples from the fines document — but only examples that are relevant to the specific state or jurisdiction being discussed. Do not cross-pollinate states: if someone is asking about Illinois advertising regulations, only reference fines that occurred in Illinois. If someone is asking about Ohio compliance, only reference Ohio fines. If no state context is clear, match fines to whatever state or topic is most relevant to the conversation.

When citing a fine, name the specific company, the fine amount, the violation type, and the state. For example: "Ohio fined [Company] $X for [violation]" or "[Company] received a $X penalty in [state] for [violation type]." Real examples land harder than totals and give the team actual reference points for what regulators care about. If there are multiple relevant state-matched examples, list them individually. Only roll up to a total after you've given the specifics.

RESPONSE DEPTH — this is critical:
- Match your depth to what's being asked. A casual question gets a sharp, concise answer. A complex analytical request — an executive summary, a financial analysis, a comprehensive review — gets a full, thorough response. Do NOT cut these short.
- For executive-level requests (earnings summaries, strategic analyses, board-ready content, CEO/leadership briefings): write the complete, polished output. Use sections, headers, and structure. Cover every major topic area. Do not truncate or summarize prematurely.
- For detailed data questions: pull every relevant figure, explain what it means, and flag anything notable. Don't stop at the first number you find.
- Never sacrifice completeness for brevity on substantive requests. If someone asks for a comprehensive analysis, they mean it — give them everything the data supports.
- Structure long responses with clear headers and sections so they are easy to navigate, not just easy to write.

ABSOLUTE RESTRICTION — never discuss, reference, or include any information about:
- Executive compensation, C-suite salaries, bonuses, equity grants, or pay packages for any named individuals
- If asked directly about executive compensation, decline simply: "That's not something I cover — happy to dig into anything else."

INDUSTRY KNOWLEDGE — you know this world deeply:

This operator is a vertically integrated cannabis company operating across multiple states. Like all MSOs, they navigate a patchwork of state regulations, each with its own licensing, compliance, and reporting requirements.

Cannabis industry realities you understand:
- 280E tax burden: cannabis companies can't deduct normal business expenses because of federal scheduling, which crushes margins
- Banking is still a nightmare for most operators — limited access, high fees, cash-heavy operations
- METRC is the seed-to-sale tracking system used by most states — every plant, every package, every transfer gets a tag
- State-by-state compliance is genuinely complex: what's legal in IL isn't the same as PA, and both change constantly
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
    // 0. Library map + retrieval plan — Kait's awareness of the full library
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
    if (contextDocs) {
      const isComplianceDept = (dept || '').toLowerCase().includes('compliance');
      const hasRegulatorySignal = contextDocs.toLowerCase().includes('regulatory') ||
        contextDocs.toLowerCase().includes(' rule ') || contextDocs.toLowerCase().includes('statute') ||
        contextDocs.toLowerCase().includes('enforcement') || contextDocs.toLowerCase().includes('inspectors');
      if (isComplianceDept || hasRegulatorySignal) {
        system += `\n\nCRITICAL OPERATING CONTEXT — READ THIS BEFORE ANSWERING ANYTHING:\n${contextDocs}\n\nThis context is not optional background. It is your primary interpretive frame. Before you analyze ANY question, read this context first and ask: does this change how I should interpret the situation? For compliance and regulatory work, this context is the lens through which all documents and data must be read. If there is tension between what a document says and what this context tells you about the regulatory environment, flag that tension explicitly. Never skip this context. Never treat it as secondary to the data.`;
      } else {
        system += `\n\nBACKGROUND KNOWLEDGE (this is yours — you just know it, you work here):\n${contextDocs}\n\nUse this knowledge the way a sharp colleague would: naturally, when it's genuinely relevant, without announcing it or making it weird. You don't recite it. You don't reference it. You don't say "based on our internal documents." It just informs how you think and what you know.`;
      }
    }

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

    // Build user message — include image if provided
    const userMessageContent = imageBase64
      ? [
          { type: 'image', source: { type: 'base64', media_type: imageMimeType || 'image/jpeg', data: imageBase64 } },
          { type: 'text', text: message },
        ]
      : message;

    const responseText = await callLLM({ model, system, messages: [...history.slice(-20).map(h => ({ role: h.role, content: h.content })), { role: 'user', content: userMessageContent }], maxTokens: 8000, env, apiKey });

    // ── Answer verification — only for numeric aggregate/comparative responses ──
    // Fires when: response contains numbers AND query was aggregate or comparative
    // AND we have a stats block to check against. Haiku-only, non-blocking on failure.
    let verifiedResponse = responseText;
    const responseHasNumbers = /\$[\d,]+|\d[\d,]*\.?\d*\s*(%|k|m|b|million|billion|units|lbs|oz)/i.test(responseText);
    const shouldVerify = (intent.isAggregate || intent.isComparative) && responseHasNumbers && context.statsContext;

    if (shouldVerify) {
      try {
        const verifyPrompt = `You are a fact-checker for a cannabis company's internal AI assistant.

The user asked: "${message}"

The AI gave this response:
${responseText.slice(0, 2000)}

The authoritative pre-computed data summaries are:
${context.statsContext.slice(0, 1500)}

Your job: scan the response for any specific numbers (totals, averages, percentages, counts). For each number, check if it's consistent with the data summaries above.

If everything checks out, respond with exactly: VERIFIED

If you find a specific numerical inconsistency (not a style issue, not a missing detail — only a number that contradicts the summaries), respond with:
CORRECTION: [one sentence describing only the specific number that's wrong and what it should be based on the summaries]

Do not comment on anything else. Do not rewrite the response. Only flag hard numerical contradictions.`;

        const verifyResp = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
          body: JSON.stringify({
            model: 'claude-haiku-4-5-20251001',
            max_tokens: 120,
            messages: [{ role: 'user', content: verifyPrompt }],
          }),
        });
        const verifyData = await verifyResp.json();
        const verdict = verifyData.content?.[0]?.text?.trim() || '';

        if (verdict.startsWith('CORRECTION:')) {
          // Append a subtle correction note inline — doesn't rewrite, just flags
          const correction = verdict.replace('CORRECTION:', '').trim();
          verifiedResponse = responseText + `\n\n> ⚠ *Data check: ${correction}*`;
        }
        // VERIFIED or anything else — ship as-is
      } catch { /* non-blocking — if verify fails, original response ships unchanged */ }
    }

    // Log query for analytics — fire and forget
    writeQueryLog(env, { message, dept, username: verifyToken(request, env)?.username || 'unknown', collection: collection || null, retrieval: !!context.text, scope });

    // Only surface sources if documents were actually retrieved — not for general conversation
    // And only the ones actually referenced in the response
    const rawSources = context.text ? context.sources : [];
    const sourcesToReturn = filterUsedSources(rawSources, verifiedResponse);
    const mainRespId = `${dept}:${Date.now()}:${Math.random().toString(36).slice(2,8)}`;
    return json({ ok: true, response: verifiedResponse, sources: sourcesToReturn, scope, model: model || 'claude', verified: shouldVerify, responseId: mainRespId });
  } catch (err) { return json({ error: 'Chat error: ' + err.message }, 500); }
}

// ── Streaming Chat ────────────────────────────────────────────
// Same pre-processing as handleChat, but streams the LLM response via SSE.
// Sends token deltas as: data: {"t":"..."}\n\n
// Sends final metadata as: data: {"done":true,"sources":[...],"responseId":"...","scope":"...","model":"...","correction":"..."}\n\n
async function handleChatStream(request, env) {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const enc = new TextEncoder();

  const send = async (obj) => {
    await writer.write(enc.encode('data: ' + JSON.stringify(obj) + '\n\n'));
  };

  // Run everything async — response is already committed via SSE headers
  (async () => {
    try {
      const body = await request.json();
      const { message, dept, collection, fileId, scope = 'all', model, history = [], displayName = '', imageBase64 = null, imageMimeType = null, clientDatetime = null, clientTimezone = null } = body;

      if (!message) { await send({ error: 'Message required' }); await writer.close(); return; }

      const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
      if (!apiKey) { await send({ error: 'Anthropic API key not configured.' }); await writer.close(); return; }

      // ── Re-use all the same pre-processing logic as handleChat ──
      // Query rewrite
      let retrievalMessage = message;
      if (history.length > 0 && message.length < 80) {
        try {
          const rewriteResp = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
            body: JSON.stringify({ model: 'claude-haiku-4-5-20251001', max_tokens: 80,
              messages: [{ role: 'user', content: `Given this conversation history:\n${history.slice(-4).map(h=>`${h.role}: ${h.content}`).join('\n')}\n\nUser's new message: "${message}"\n\nRewrite the new message as a standalone search query that captures the full context needed to find relevant documents. Output only the rewritten query, nothing else. If the message is already self-contained, output it unchanged.` }] }),
          });
          if (rewriteResp.ok) { const d = await rewriteResp.json(); retrievalMessage = d.content?.[0]?.text?.trim() || message; }
        } catch { /* use original */ }
      }

      // Intent analysis
      const intent = analyzeQueryIntent(message);

      // Retrieval plan (non-blocking)
      let retrievalPlan = null;
      const libraryMap = await buildLibraryMap({ dept, env });
      const libraryMapPrompt = libraryMapToPrompt(libraryMap);
      let rewrittenQueries = null;
      if (libraryMap.collections.length > 3 && !collection && !fileId && scope === 'all') {
        try {
          const planResponse = await callLLM({ model, system: 'You are a precise research assistant. Identify which document collections contain the information needed. Be brief.', messages: [{ role: 'user', content: `User asked: "${retrievalMessage}"\nLibrary:\n${libraryMapToPrompt(libraryMap)}\nScope: ${collection ? '"'+collection+'" collection' : 'all'}\nIn 2-3 sentences: which collections are most relevant and why?` }], maxTokens: 150, env, apiKey });
          retrievalPlan = planResponse;
        } catch { /* non-blocking */ }
      }

      // Collection routing
      let activeCollection = collection;
      let activeScope = scope;
      if (scope === 'collection' && collection) {
        const reg = await env.CACI_KV.get(`colreg:${dept}`, 'json') || [];
        const msgLower = message.toLowerCase();
        const matchedCol = reg.find(c => c.name !== collection && msgLower.includes(c.name.toLowerCase()));
        if (matchedCol) { activeCollection = matchedCol.name; activeScope = 'collection'; }
      }

      // Context retrieval
      let context;
      const useMultiCollection = activeScope === 'all' && !fileId && !activeCollection;
      if (useMultiCollection) {
        context = await buildContextMultiCollection({ message: retrievalMessage, dept, env, maxCollections: intent.isComparative ? 6 : 5 });
        if (!context.text && !context.statsContext) context = await buildContext({ message: retrievalMessage, dept, collection: null, fileId: null, scope: 'all', env });
      } else {
        context = await buildContext({ message: retrievalMessage, dept, collection: activeCollection, fileId, scope: activeScope, env });
      }

      // Context docs
      let contextDocs = '';
      const GLOBAL_CTX_COLLECTION = 'Internal Reference';
      const globalCtxFiles = await env.CACI_KV.get(`ctx:${dept}:${GLOBAL_CTX_COLLECTION}`, 'json') || [];
      if (globalCtxFiles.length) {
        const texts = [];
        for (const f of globalCtxFiles.slice(0, 10)) { const full = await env.CACI_KV.get(`file:${f.id}`, 'json'); if (full?.chunks) texts.push(`[Context: ${f.name}]\n${full.chunks.join('\n\n')}`); }
        if (texts.length) contextDocs = texts.join('\n\n');
      }
      if (activeCollection && collection !== GLOBAL_CTX_COLLECTION) {
        const ctxFiles = await env.CACI_KV.get(`ctx:${dept}:${collection}`, 'json') || [];
        if (ctxFiles.length) {
          const texts = [];
          for (const f of ctxFiles.slice(0, 5)) { const full = await env.CACI_KV.get(`file:${f.id}`, 'json'); if (full?.chunks) texts.push(`[Context: ${f.name}]\n${full.chunks.join('\n\n')}`); }
          if (texts.length) contextDocs = contextDocs ? contextDocs + '\n\n' + texts.join('\n\n') : texts.join('\n\n');
        }
      }

      const scopeLabel = scope === 'file' ? `the document ${context.focusFile}` : scope === 'collection' ? `the ${collection} collection` : `all ${dept} documents`;

      // System prompt — identical to handleChat
      let system = `You are Kait — an AI intelligence platform built for cannabis operators. Do NOT introduce yourself or state your name unless directly asked. Never say "Hi, I'm Kait" in follow-up responses. Just answer.

Today is ${clientDatetime || new Date().toLocaleString('en-US', {weekday:'long', year:'numeric', month:'long', day:'numeric', hour:'numeric', minute:'2-digit', hour12:true})}${clientTimezone ? ` (${clientTimezone})` : ''}. The full calendar year 2025 is complete. When discussing 2025 data, treat it as a full historical year.

Your personality: You work in cannabis. You know these people. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have grace and tact in how you communicate — honest without being harsh, direct without being cold. You have a thin filter because you value truth more than comfort. You know how to read a room and navigate people.

You're not just a number cruncher. You can talk about anything — but you also happen to be extremely good at analyzing data and documents when that's needed.

Right now you're analyzing ${scopeLabel} for the ${dept} team.

When answering from documents:
- Cite the specific document name and period when referencing data
- Never fabricate numbers — if the data isn't there, say so plainly
- Lead with the insight, not the methodology
- If something is interesting or surprising in the data, say so — have a point of view
- If you can't fully answer something, tell them what you CAN tell them and what's missing

FINES AND ENFORCEMENT DATA — critical instruction:
When any topic relates to regulatory fines, violations, enforcement actions, or penalties in the cannabis industry, do NOT just cite totals or aggregate figures. Instead, surface individual examples from the fines document — but only examples that are relevant to the specific state or jurisdiction being discussed. Do not cross-pollinate states: if someone is asking about Illinois advertising regulations, only reference fines that occurred in Illinois. If someone is asking about Ohio compliance, only reference Ohio fines. If no state context is clear, match fines to whatever state or topic is most relevant to the conversation.

When citing a fine, name the specific company, the fine amount, the violation type, and the state. For example: "Ohio fined [Company] $X for [violation]" or "[Company] received a $X penalty in [state] for [violation type]." Real examples land harder than totals and give the team actual reference points for what regulators care about. If there are multiple relevant state-matched examples, list them individually. Only roll up to a total after you've given the specifics.

RESPONSE DEPTH — this is critical:
- Match your depth to what's being asked. A casual question gets a sharp, concise answer. A complex analytical request — an executive summary, a financial analysis, a comprehensive review — gets a full, thorough response. Do NOT cut these short.
- For executive-level requests (earnings summaries, strategic analyses, board-ready content, CEO/leadership briefings): write the complete, polished output. Use sections, headers, and structure. Cover every major topic area. Do not truncate or summarize prematurely.
- For detailed data questions: pull every relevant figure, explain what it means, and flag anything notable. Don't stop at the first number you find.
- Never sacrifice completeness for brevity on substantive requests. If someone asks for a comprehensive analysis, they mean it — give them everything the data supports.
- Structure long responses with clear headers and sections so they are easy to navigate, not just easy to write.

ABSOLUTE RESTRICTION — never discuss, reference, or include any information about:
- Executive compensation, C-suite salaries, bonuses, equity grants, or pay packages for any named individuals
- If asked directly about executive compensation, decline simply: "That's not something I cover — happy to dig into anything else."

INDUSTRY KNOWLEDGE — you know this world deeply:

This operator is a vertically integrated cannabis company operating across multiple states. Like all MSOs, they navigate a patchwork of state regulations, each with its own licensing, compliance, and reporting requirements.

Cannabis industry realities you understand:
- 280E tax burden: cannabis companies can't deduct normal business expenses because of federal scheduling, which crushes margins
- Banking is still a nightmare for most operators — limited access, high fees, cash-heavy operations
- METRC is the seed-to-sale tracking system used by most states — every plant, every package, every transfer gets a tag
- State-by-state compliance is genuinely complex: what's legal in IL isn't the same as PA, and both change constantly
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
    // 0. Library map + retrieval plan — Kait's awareness of the full library
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
    if (contextDocs) {
      const isComplianceDept = (dept || '').toLowerCase().includes('compliance');
      const hasRegulatorySignal = contextDocs.toLowerCase().includes('regulatory') ||
        contextDocs.toLowerCase().includes(' rule ') || contextDocs.toLowerCase().includes('statute') ||
        contextDocs.toLowerCase().includes('enforcement') || contextDocs.toLowerCase().includes('inspectors');
      if (isComplianceDept || hasRegulatorySignal) {
        system += `\n\nCRITICAL OPERATING CONTEXT — READ THIS BEFORE ANSWERING ANYTHING:\n${contextDocs}\n\nThis context is not optional background. It is your primary interpretive frame. Before you analyze ANY question, read this context first and ask: does this change how I should interpret the situation? For compliance and regulatory work, this context is the lens through which all documents and data must be read. If there is tension between what a document says and what this context tells you about the regulatory environment, flag that tension explicitly. Never skip this context. Never treat it as secondary to the data.`;
      } else {
        system += `\n\nBACKGROUND KNOWLEDGE (this is yours — you just know it, you work here):\n${contextDocs}\n\nUse this knowledge the way a sharp colleague would: naturally, when it's genuinely relevant, without announcing it or making it weird. You don't recite it. You don't reference it. You don't say "based on our internal documents." It just informs how you think and what you know.`;
      }
    }

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

      // User message content
      const userMessageContent = imageBase64
        ? [{ type: 'image', source: { type: 'base64', media_type: imageMimeType || 'image/jpeg', data: imageBase64 } }, { type: 'text', text: message }]
        : message;

      const messages = [...history.slice(-20).map(h => ({ role: h.role, content: h.content })), { role: 'user', content: userMessageContent }];

      // ── Stream from Anthropic ──
      let fullText = '';
      if (!model || model === 'claude') {
        const streamRes = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
          body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 8000, system, messages, stream: true }),
        });
        if (!streamRes.ok) { const e = await streamRes.text(); await send({ error: `Claude API error (${streamRes.status}): ${e}` }); await writer.close(); return; }

        const reader = streamRes.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          const lines = buf.split('\n');
          buf = lines.pop(); // keep incomplete line
          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            const data = line.slice(6).trim();
            if (data === '[DONE]') continue;
            try {
              const evt = JSON.parse(data);
              if (evt.type === 'content_block_delta' && evt.delta?.type === 'text_delta') {
                const t = evt.delta.text;
                fullText += t;
                await send({ t });
              }
            } catch { /* skip malformed */ }
          }
        }
      } else {
        // Non-Claude models — fall back to non-streaming, send full text as one chunk
        fullText = await callLLM({ model, system, messages, maxTokens: 8000, env, apiKey });
        await send({ t: fullText });
      }

      // ── Verification (post-stream, same logic as handleChat) ──
      let correction = null;
      const responseHasNumbers = /\$[\d,]+|\d[\d,]*\.?\d*\s*(%|k|m|b|million|billion|units|lbs|oz)/i.test(fullText);
      const shouldVerify = (intent.isAggregate || intent.isComparative) && responseHasNumbers && context.statsContext;
      if (shouldVerify) {
        try {
          const verifyResp = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
            body: JSON.stringify({ model: 'claude-haiku-4-5-20251001', max_tokens: 120, messages: [{ role: 'user', content: `User asked: "${message}"\n\nAI response:\n${fullText.slice(0,2000)}\n\nAuthoritative summaries:\n${context.statsContext.slice(0,1500)}\n\nCheck only for hard numerical contradictions. Reply "VERIFIED" or "CORRECTION: [one sentence]".` }] }),
          });
          const vd = await verifyResp.json();
          const verdict = vd.content?.[0]?.text?.trim() || '';
          if (verdict.startsWith('CORRECTION:')) correction = verdict.replace('CORRECTION:', '').trim();
        } catch { /* non-blocking */ }
      }

      // ── Logging ──
      await writeQueryLog(env, { message, dept, username: verifyToken(request, env)?.username || 'unknown', collection: collection || null, retrieval: !!context.text, scope });

      // ── Final metadata event ──
      const responseId = `${dept}:${Date.now()}:${Math.random().toString(36).slice(2,8)}`;
      // Filter sources to only include files actually referenced in the response
      const rawSources = context.text ? context.sources : [];
      const sourcesToReturn = filterUsedSources(rawSources, fullText);
      await send({ done: true, sources: sourcesToReturn, responseId, scope, model: model || 'claude', correction });

    } catch (err) {
      try { await send({ error: 'Stream error: ' + err.message }); } catch {}
    } finally {
      try { await writer.close(); } catch {}
    }
  })();

  return new Response(readable, {
    headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', ...CORS },
  });
}



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
// ── Library Map — Kait's persistent understanding of the library ──
async function buildLibraryMap({ dept, env }) {
  try {
    // OPTIMIZATION: Extend cache from 2min to 5min - library doesn't change that fast
    const CACHE_TTL = 5 * 60 * 1000;
    const cached = await env.CACI_KV.get(`library:map:${dept}`, 'json');
    if (cached && cached.builtAt && (Date.now() - cached.builtAt) < CACHE_TTL) return cached;

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

    const reportPrompt = `You are generating a professional internal business report for a cannabis operator.

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
      system: 'You are a precise business analyst generating internal reports for a cannabis operator. Use markdown formatting — headers, tables, and bullets where appropriate. Cite document names and periods for every data point.',
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
    const expandedKeywords = expandKeywords(keywords); // Expand synonyms for better file matching
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
      for (const kw of expandedKeywords) {
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
            if (expandedKeywords.some(kw => vLower === kw || vLower.includes(kw) || kw.includes(vLower))) {
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
      topFiles = scoredFiles.slice(0, 20); // Increased from 12 - need more coverage for large regulatory collections
    } else {
      // Broad query: top 5 + spread 15 evenly across rest for comprehensive coverage
      const top5 = scoredFiles.slice(0, 5); // Increased from 3
      const rest  = scoredFiles.slice(5);
      const spread = [];
      const step = Math.max(1, Math.floor(rest.length / 15)); // Increased from 7
      for (let i = 0; i < rest.length && spread.length < 15; i += step) spread.push(rest[i]);
      topFiles = [...top5, ...spread];
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

    // ── Hybrid scoring: semantic + keyword ───────────────────
    // 1. Embed the query (once, reused across all files)
    // 2. Load stored embeddings per file
    // 3. Blend cosine similarity with keyword score
    const queryEmbedding = await generateEmbedding(message, env).catch(() => null);

    // Score all non-summary chunks across all files
    const allChunks = [];
    for (const fileData of fullFiles) {
      if (!fileData.chunks) continue;
      const fileBoost = fileData._score || 0;

      // Load embeddings for this file (null if not stored yet)
      const fileEmbs = queryEmbedding
        ? await loadFileEmbeddings(fileData.id, fileData.chunks.length, env).catch(() => null)
        : null;

      // Compute raw keyword scores first so we can normalize
      const rawScores = fileData.chunks.map(chunk => scoreChunk(chunk, keywords2));
      const maxRaw = Math.max(...rawScores.filter(s => s >= 0), 1);

      for (let ci = 0; ci < fileData.chunks.length; ci++) {
        const chunk = fileData.chunks[ci];
        const kwScore = rawScores[ci];
        if (kwScore < 0) continue; // summary chunks excluded here (handled above)

        const chunkEmb = fileEmbs?.[ci] ?? null;
        const semSim   = chunkEmb && queryEmbedding ? cosineSim(queryEmbedding, chunkEmb) : null;
        const blended  = hybridScore(kwScore, semSim, maxRaw);

        allChunks.push({
          chunk,
          score:      blended + fileBoost * 0.05, // dampen file-level boost so semantic wins
          kwScore,
          semScore:   semSim,
          filename:   fileData.name,
          collection: fileData.collection,
          meta:       fileData.meta || {},
        });
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

    // ── Hybrid scoring: semantic + keyword ───────────────────
    const queryEmbCtx = await generateEmbedding(message, env).catch(() => null);

    const scored = [];
    for (const fileData of filesToSearch) {
      if (!fileData.chunks) continue;
      const fileBoost = fileScoreMap.get(fileData.name) || 0;

      const fileEmbsCtx = queryEmbCtx
        ? await loadFileEmbeddings(fileData.id, fileData.chunks.length, env).catch(() => null)
        : null;

      const rawScoresCtx = fileData.chunks.map(chunk => scoreChunk(chunk, keywords));
      const maxRawCtx    = Math.max(...rawScoresCtx.filter(s => s >= 0), 1);

      for (let ci = 0; ci < fileData.chunks.length; ci++) {
        const chunk   = fileData.chunks[ci];
        const kwScore = rawScoresCtx[ci];
        if (kwScore < 0) continue;

        const chunkEmb = fileEmbsCtx?.[ci] ?? null;
        const semSim   = chunkEmb && queryEmbCtx ? cosineSim(queryEmbCtx, chunkEmb) : null;
        const blended  = hybridScore(kwScore, semSim, maxRawCtx);

        scored.push({
          chunk,
          score:      blended + fileBoost * 0.05,
          filename:   fileData.name,
          collection: fileData.collection,
          meta:       fileData.meta || {},
        });
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

    const prompt = `You are analyzing a document collection for a cannabis operator.

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

    const prompt = `You are classifying a business document for a cannabis operator.

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
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const body = await request.json();
    const saved = [];
    for (const key of ['ANTHROPIC_API_KEY', 'XAI_API_KEY']) {
      if (body[key] !== undefined) {
        body[key] === '' ? await env.CACI_KV.delete('config:' + key) : await env.CACI_KV.put('config:' + key, body[key]);
        saved.push(key);
      }
    }
    // Demo mode toggle (boolean)
    if (body.DEMO_MODE_ENABLED !== undefined) {
      const v = body.DEMO_MODE_ENABLED ? '1' : '0';
      await env.CACI_KV.put('config:DEMO_MODE_ENABLED', v);
      saved.push('DEMO_MODE_ENABLED');
    }
    if (saved.length) writeAudit(env, requireAdmin(request, env), 'config.save', { dept: 'global', keys: saved });
    return json({ ok: true, saved });
  } catch (err) { return json({ error: err.message }, 500); }
}

async function handleAdminGet(env) {
  try {
    const kvAnt = await env.CACI_KV.get('config:ANTHROPIC_API_KEY');
    const kvXai = await env.CACI_KV.get('config:XAI_API_KEY');
    const kvDemo = await env.CACI_KV.get('config:DEMO_MODE_ENABLED');
    return json({
      ANTHROPIC_API_KEY: {
        configured: !!(kvAnt || env.ANTHROPIC_API_KEY),
        source: kvAnt ? 'admin' : env.ANTHROPIC_API_KEY ? 'secret' : 'none',
      },
      XAI_API_KEY: {
        configured: !!(kvXai || env.XAI_API_KEY),
        source: kvXai ? 'admin' : env.XAI_API_KEY ? 'secret' : 'none',
      },
      DEMO_MODE_ENABLED: kvDemo === '1',
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
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
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
    writeAudit(env, requireAdmin(request, env), 'weights.save', { dept });
    return json({ ok: true, weights: sanitized });
  } catch (err) { return json({ error: err.message }, 500); }
}

async function handleResetWeights(request, env) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
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
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
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
    const analysisPrompt = `You are analyzing feedback on a RAG (retrieval-augmented generation) system called Kait used by a cannabis company. Your job is to suggest scoring weight adjustments based on user feedback patterns.

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
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
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
    writeAudit(env, requireAdmin(request, env), 'tuning.approve', { dept, changeCount: changes.length });
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

// ── Semantic Embedding Helpers ────────────────────────────────
// Uses Cloudflare AI binding (@cf/baai/bge-base-en-v1.5, 768-dim).
// Gracefully no-ops if the binding is unavailable — keyword scoring
// continues as normal, so there is zero breakage risk.

const EMB_MODEL = '@cf/baai/bge-base-en-v1.5';
const EMB_MAX_CHARS = 512; // truncate to keep latency low; model handles ~512 tokens
const HYBRID_ALPHA = 0.45; // weight given to semantic score; 1-alpha goes to keyword
// Per-file embedding KV key: emb:{fileId}:{chunkIndex}
// Value: base64-encoded Float32Array (768 floats × 4 bytes = 3072 bytes → ~4KB base64)

async function generateEmbedding(text, env) {
  if (!env?.AI) return null;
  try {
    const truncated = text.slice(0, EMB_MAX_CHARS);
    // Add a timeout wrapper to prevent hanging
    const timeoutPromise = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Embedding timeout')), 10000) // 10 second timeout
    );
    const embeddingPromise = env.AI.run(EMB_MODEL, { text: truncated });
    
    const result = await Promise.race([embeddingPromise, timeoutPromise]);
    // result.data is Float32Array or number[]
    const vec = result?.data;
    if (!vec || !vec.length) return null;
    // Serialize to base64 for KV storage
    const floats = new Float32Array(vec);
    const bytes  = new Uint8Array(floats.buffer);
    return btoa(String.fromCharCode(...bytes));
  } catch (err) {
    // Log error but don't throw - graceful degradation to keyword-only
    console.error('Embedding generation failed:', err.message);
    return null;
  }
}

function decodeEmbedding(b64) {
  if (!b64) return null;
  try {
    const binary = atob(b64);
    const bytes  = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return new Float32Array(bytes.buffer);
  } catch { return null; }
}

function cosineSim(a, b) {
  if (!a || !b || a.length !== b.length) return 0;
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    na  += a[i] * a[i];
    nb  += b[i] * b[i];
  }
  const denom = Math.sqrt(na) * Math.sqrt(nb);
  return denom > 0 ? dot / denom : 0;
}

// Store embeddings for all chunks of a file — fire-and-forget from upload
async function storeChunkEmbeddings(fileId, chunks, env) {
  if (!env?.AI || !env?.CACI_KV) return;
  const EMB_BATCH = 4; // Reduced from 8 to avoid CPU/timeout limits
  for (let i = 0; i < chunks.length; i += EMB_BATCH) {
    const batch = chunks.slice(i, i + EMB_BATCH);
    await Promise.all(batch.map(async (chunk, j) => {
      const idx = i + j;
      const b64 = await generateEmbedding(chunk, env);
      if (b64) {
        await env.CACI_KV.put(
          `emb:${fileId}:${idx}`,
          b64,
          { expirationTtl: 60 * 60 * 24 * 365 }
        ).catch(() => {});
      }
    }));
  }
}

// Load all embeddings for a file from KV
async function loadFileEmbeddings(fileId, chunkCount, env) {
  if (!env?.CACI_KV) return null;
  const keys = Array.from({ length: chunkCount }, (_, i) => `emb:${fileId}:${i}`);
  const results = await Promise.all(keys.map(k => env.CACI_KV.get(k).catch(() => null)));
  // Return array of Float32Array | null, one per chunk
  return results.map(b64 => b64 ? decodeEmbedding(b64) : null);
}

// Hybrid score: blends normalized keyword score with cosine similarity.
// keywordScore: raw score from scoreChunk (unbounded, typically 0–30)
// semanticSim:  cosine similarity (0–1), or null if unavailable
// keywordMax:   max keyword score in the candidate set (for normalization)
function hybridScore(keywordScore, semanticSim, keywordMax) {
  // Normalize keyword score to 0–1 range
  const normKeyword = keywordMax > 0 ? Math.min(keywordScore / keywordMax, 1) : 0;
  if (semanticSim === null || semanticSim === undefined) return normKeyword;
  // Blend: alpha × semantic + (1-alpha) × keyword
  return HYBRID_ALPHA * semanticSim + (1 - HYBRID_ALPHA) * normKeyword;
}

// ── Helpers ───────────────────────────────────────────────────
function chunkText(text, size = 1500) {
  if (!text || text.length === 0) return [];

  // Short documents — store as single chunk, no splitting needed
  if (text.length <= 2000) return [text.trim()];

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
  // Use larger chunks for dense regulatory/legal text
  const effectiveSize = Math.max(size, 2000);
  const chunks = [];
  let start = 0;
  while (start < text.length) {
    let end = Math.min(start + effectiveSize, text.length);
    if (end < text.length) {
      // Try to break at paragraph boundary first
      const paraBreak = text.lastIndexOf('\n\n', end);
      if (paraBreak > start + effectiveSize * 0.5) {
        end = paraBreak;
      } else {
        // Fall back to sentence boundary
        const sentBreak = Math.max(
          text.lastIndexOf('. ', end),
          text.lastIndexOf('.\n', end),
          text.lastIndexOf('? ', end),
          text.lastIndexOf('! ', end)
        );
        if (sentBreak > start + effectiveSize * 0.5) end = sentBreak + 1;
      }
    }
    const chunk = text.slice(start, end).trim();
    if (chunk.length > 50) chunks.push(chunk);
    // Minimal overlap — just enough to preserve a sentence boundary reference
    // 50 chars is sufficient; 150 was causing cascading duplicate fragments
    start = Math.max(start + 1, end - 50);
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
  'security':   ['surveillance', 'monitoring', 'safety', 'operating', 'procedures', 'requirements'],
  'testing':    ['laboratory', 'lab', 'quality', 'assurance', 'procedures', 'requirements'],
  'packaging':  ['labeling', 'label', 'requirements', 'procedures', 'standards'],
  'transport':  ['transportation', 'transfer', 'manifest', 'delivery', 'procedures'],
  'return':     ['returns', 'refund', 'credit', 'complaint'],
  'transfer':   ['transfers', 'manifest', 'transport'],
  'dispensary': ['store', 'retail', 'location', 'site', 'operating', 'procedures'],
  'delivery':   ['curbside', 'drive-up', 'drive-through', 'pickup', 'operating', 'operations', 'procedures'],
  'curbside':   ['delivery', 'drive-up', 'pickup', 'operating', 'operations', 'procedures'],
  'pickup':     ['curbside', 'delivery', 'drive-up', 'operating', 'operations'],
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

// Filter source list to only include files actually referenced in the response.
// CACI retrieves many docs during search, but often only uses a few for her answer.
// This post-hoc filter scans the response text for signals that a source was used:
//   - The filename (or part of it) appears in the response
//   - A rule code from the filename (like "1301:18-8-02") appears in the response
//   - Specific identifying keywords from the filename appear in the response
// Falls back to returning ALL sources if we can't determine which were used
// (better to over-cite than to leave the user with no sources at all).
function filterUsedSources(sources, responseText) {
  if (!sources || !sources.length || !responseText) return sources || [];
  const respLower = responseText.toLowerCase();
  
  const usedSources = sources.filter(src => {
    if (!src) return false;
    const srcLower = src.toLowerCase();
    
    // Strip path prefix (e.g., "Collection / filename.pdf" -> "filename.pdf")
    const justName = src.includes(' / ') ? src.split(' / ').pop() : src;
    const nameLower = justName.toLowerCase();
    const nameNoExt = nameLower.replace(/\.[a-z]+$/, '');
    
    // Check 1: Full filename appears in response
    if (respLower.includes(nameLower) || respLower.includes(nameNoExt)) return true;
    
    // Check 2: Extract rule/section codes from filename
    // Handles formats like: OH_1301_18_8_02, 1301-18-8-02, 1301:18:8:02
    const codeMatches = justName.match(/\b\d{3,}[_:\-\.]\d+[_:\-\.]\d+[_:\-\.]?\d*/g);
    if (codeMatches) {
      for (const code of codeMatches) {
        // Normalize the code to various formats the LLM might use
        const normalized = code.replace(/[_\-\.]/g, ':');
        const withHyphens = code.replace(/[_:\.]/g, '-');
        const withDots = code.replace(/[_:\-]/g, '.');
        if (respLower.includes(normalized.toLowerCase()) || 
            respLower.includes(withHyphens.toLowerCase()) ||
            respLower.includes(withDots.toLowerCase())) {
          return true;
        }
      }
    }
    
    // Check 3: Extract distinctive keywords from filename (3+ chars, not common words)
    // e.g., "Dispensary_Operating_Procedures" → ["Dispensary", "Operating", "Procedures"]
    const stopWords = new Set(['the','and','for','with','from','this','that','what','have','has','had','been','were','are','was','can']);
    const keywords = nameNoExt
      .split(/[_\-\s\.]+/)
      .filter(w => w.length >= 5 && !stopWords.has(w.toLowerCase()) && !/^\d+$/.test(w));
    
    // If at least 2 distinctive keywords from the filename appear, consider it used
    if (keywords.length >= 2) {
      const matches = keywords.filter(kw => respLower.includes(kw.toLowerCase())).length;
      if (matches >= 2) return true;
      // Or if a single very distinctive keyword (8+ chars) matches
      if (keywords.some(kw => kw.length >= 8 && respLower.includes(kw.toLowerCase()))) return true;
    }
    
    return false;
  });
  
  // Safety fallback: if filter removed ALL sources, return original list
  // (better to over-cite than leave user with no way to verify)
  if (usedSources.length === 0 && sources.length > 0) {
    return sources;
  }
  
  return usedSources;
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

// ── Audit Trail ───────────────────────────────────────────────
// Thin append-only log. writeAudit is called from mutation handlers.
// KV key: audit:{dept}:{paddedTimestamp}:{username}
// 90-day TTL — audit logs expire automatically.

// ── Query Log ─────────────────────────────────────────────────
async function writeQueryLog(env, { message, dept, username, collection, retrieval, scope }) {
  if (!env?.CACI_KV) return;
  try {
    const ts  = Date.now();
    const key = `qlog:${String(ts).padStart(16,'0')}:${username}`;
    const entry = {
      ts:         new Date(ts).toISOString(),
      username,
      dept:       dept || 'global',
      message:    message?.slice(0, 300) || '',
      collection: collection || null,
      retrieval,  // true = docs found, false = no retrieval hit
      scope,
    };
    await env.CACI_KV.put(key, JSON.stringify(entry), { expirationTtl: 60 * 60 * 24 * 90 });
  } catch { /* non-blocking */ }
}

// ── GET /analytics ────────────────────────────────────────────
async function handleGetAnalytics(url, env, request) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '500'), 2000);
    const list  = await env.CACI_KV.list({ prefix: 'qlog:' });
    const keys  = (list.keys || []).reverse().slice(0, limit);
    const entries = await Promise.all(keys.map(k => env.CACI_KV.get(k.name, 'json').catch(() => null)));
    const logs = entries.filter(Boolean);

    // Aggregates
    const userSet    = new Set();
    const deptCounts = {};
    const dayCounts  = {};
    let   missCount  = 0;

    for (const e of logs) {
      userSet.add(e.username);
      deptCounts[e.dept] = (deptCounts[e.dept] || 0) + 1;
      const day = e.ts?.slice(0, 10) || 'unknown';
      dayCounts[day] = (dayCounts[day] || 0) + 1;
      if (!e.retrieval) missCount++;
    }

    // Top queries — simple dedup by normalized message
    const msgMap = {};
    for (const e of logs) {
      const key = e.message.toLowerCase().trim().slice(0, 120);
      msgMap[key] = (msgMap[key] || 0) + 1;
    }
    const topQueries = Object.entries(msgMap)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([msg, count]) => ({ msg, count }));

    return json({
      ok: true,
      totalQueries: logs.length,
      uniqueUsers:  userSet.size,
      missCount,
      deptCounts,
      dayCounts,
      topQueries,
      recentLogs: logs.slice(0, 200),
    });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ── POST /analytics/insights ──────────────────────────────────
async function handleGenerateInsights(request, env) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    const xaiKey = (await env.CACI_KV.get('config:XAI_API_KEY'))       || env.XAI_API_KEY;
    if (!apiKey && !xaiKey) return json({ error: 'No AI key configured' }, 400);

    // Pull up to 500 recent logs for analysis
    const list  = await env.CACI_KV.list({ prefix: 'qlog:' });
    const keys  = (list.keys || []).reverse().slice(0, 500);
    const entries = await Promise.all(keys.map(k => env.CACI_KV.get(k.name, 'json').catch(() => null)));
    const logs  = entries.filter(Boolean);

    if (logs.length === 0) return json({ error: 'No query data yet — ask some questions first.' }, 400);

    // Build summary for the prompt
    const userSet    = new Set(logs.map(e => e.username));
    const deptCounts = {};
    const missLogs   = [];
    const msgMap     = {};

    for (const e of logs) {
      deptCounts[e.dept] = (deptCounts[e.dept] || 0) + 1;
      if (!e.retrieval) missLogs.push(e.message);
      const k = e.message.toLowerCase().trim().slice(0, 120);
      msgMap[k] = (msgMap[k] || 0) + 1;
    }

    const topQueries = Object.entries(msgMap).sort((a,b)=>b[1]-a[1]).slice(0,15).map(([m,c])=>`"${m}" (${c}x)`).join('\n');
    const missExamples = [...new Set(missLogs)].slice(0, 20).map(m => `- ${m}`).join('\n');
    const deptBreakdown = Object.entries(deptCounts).map(([d,c])=>`${d}: ${c}`).join(', ');

    const prompt = `You are Kait, an AI intelligence platform for cannabis operators. You are analyzing your own query log data to identify patterns and improvement opportunities. Be direct, specific, and actionable. Do not pad the response.

QUERY LOG SUMMARY:
- Total queries: ${logs.length}
- Unique users: ${userSet.size}
- Date range: ${logs[logs.length-1]?.ts?.slice(0,10)} to ${logs[0]?.ts?.slice(0,10)}
- Queries with no document match (retrieval miss): ${missLogs.length}
- Department breakdown: ${deptBreakdown}

TOP REPEATED QUESTIONS:
${topQueries}

QUESTIONS WITH NO DOCUMENT MATCH (sample):
${missExamples || '(none)'}

Based on this data, provide:
1. 2-3 specific observations about what the team is actually using you for
2. The clearest knowledge gaps (questions being asked that have no document support)
3. 2-3 concrete recommendations — specific file types or SOPs to upload, collections to create, or topics to document
4. One thing that's working well

Keep it under 300 words. Be specific to cannabis operations where relevant.`;

    let insights = '';
    if (apiKey) {
      const res  = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 600, messages: [{ role: 'user', content: prompt }] }),
      });
      const d = await res.json();
      insights = d.content?.[0]?.text || '';
    } else {
      const res  = await fetch('https://api.x.ai/v1/chat/completions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${xaiKey}` },
        body: JSON.stringify({ model: 'grok-3-mini-fast-beta', max_tokens: 600, messages: [{ role: 'user', content: prompt }] }),
      });
      const d = await res.json();
      insights = d.choices?.[0]?.message?.content || '';
    }

    // Cache the insights in KV
    await env.CACI_KV.put('analytics:insights:last', JSON.stringify({
      insights,
      generatedAt: new Date().toISOString(),
      queryCount:  logs.length,
    }), { expirationTtl: 60 * 60 * 24 * 30 });

    return json({ ok: true, insights, generatedAt: new Date().toISOString(), queryCount: logs.length });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

async function handleGetCachedInsights(env, request) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const cached = await env.CACI_KV.get('analytics:insights:last', 'json');
    if (!cached) return json({ ok: false });
    return json({ ok: true, ...cached });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

async function writeAudit(env, user, action, details = {}) {
  if (!env?.CACI_KV) return;
  try {
    const username = user?.username || 'unknown';
    const dept     = details.dept || 'global';
    const ts       = Date.now();
    const key      = `audit:${dept}:${String(ts).padStart(16,'0')}:${username}`;
    const entry    = {
      ts:          new Date(ts).toISOString(),
      username,
      displayName: details.displayName || username,
      role:        user?.role || 'user',
      dept,
      action,
      details:     { ...details, dept: undefined, displayName: undefined },
    };
    await env.CACI_KV.put(key, JSON.stringify(entry), { expirationTtl: 60 * 60 * 24 * 90 });
  } catch { /* non-blocking */ }
}

async function handleGetAudit(request, url, env) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const dept  = url.searchParams.get('dept') || 'global';
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '100'), 500);
    const list  = await env.CACI_KV.list({ prefix: `audit:${dept}:` });
    // Keys are sorted ascending by timestamp — reverse for newest-first
    const keys  = (list.keys || []).reverse().slice(0, limit);
    const entries = await Promise.all(keys.map(k => env.CACI_KV.get(k.name, 'json').catch(() => null)));
    return json({ ok: true, entries: entries.filter(Boolean) });
  } catch (e) { return json({ error: e.message }, 500); }
}

// ── Saved Responses (server-side, per user) ───────────────────
// KV key: saved:{username}:{id}  (id = timestamp string)

async function handleListSaved(request, env) {
  const user = requireAuth(request, env);
  if (!user) return json({ error: 'Unauthorized' }, 401);
  try {
    const username = user.legacy ? 'admin' : user.username;
    const list = await env.CACI_KV.list({ prefix: `saved:${username}:` });
    const items = await Promise.all(
      (list.keys || []).reverse().map(k => env.CACI_KV.get(k.name, 'json').catch(() => null))
    );
    return json({ ok: true, items: items.filter(Boolean) });
  } catch (e) { return json({ error: e.message }, 500); }
}

async function handleCreateSaved(request, env) {
  const user = requireAuth(request, env);
  if (!user) return json({ error: 'Unauthorized' }, 401);
  try {
    const { prompt, text, dept: savedDept } = await request.json();
    if (!text) return json({ error: 'text required' }, 400);
    const username = user.legacy ? 'admin' : user.username;
    const id = Date.now().toString();
    const entry = {
      id, prompt: (prompt || '').slice(0, 200), text,
      dept: savedDept || 'global',
      username,
      savedAt: new Date().toISOString(),
    };
    await env.CACI_KV.put(
      `saved:${username}:${id}`,
      JSON.stringify(entry),
      { expirationTtl: 60 * 60 * 24 * 365 }
    );
    return json({ ok: true, id });
  } catch (e) { return json({ error: e.message }, 500); }
}

async function handleDeleteSaved(path, request, env) {
  const user = requireAuth(request, env);
  if (!user) return json({ error: 'Unauthorized' }, 401);
  try {
    const id = path.replace('/saved/', '');
    const username = user.legacy ? 'admin' : user.username;
    await env.CACI_KV.delete(`saved:${username}:${id}`);
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

// ── Chat History (server-side, per user per dept) ─────────────
// KV key: history:{username}:{dept}
// Stores last 40 turns, 1yr TTL

async function handleGetHistory(path, request, env) {
  const user = requireAuth(request, env);
  if (!user) return json({ error: 'Unauthorized' }, 401);
  try {
    const deptId   = path.replace('/history/', '');
    const username = user.legacy ? 'admin' : user.username;
    const stored   = await env.CACI_KV.get(`history:${username}:${deptId}`, 'json');
    return json({ ok: true, history: stored || [] });
  } catch (e) { return json({ error: e.message }, 500); }
}

async function handleSaveHistory(path, request, env) {
  const user = requireAuth(request, env);
  if (!user) return json({ error: 'Unauthorized' }, 401);
  try {
    const deptId   = path.replace('/history/', '');
    const username = user.legacy ? 'admin' : user.username;
    const { history } = await request.json();
    if (!Array.isArray(history)) return json({ error: 'history must be array' }, 400);
    const toSave = history.slice(-40); // keep last 40 turns
    await env.CACI_KV.put(
      `history:${username}:${deptId}`,
      JSON.stringify(toSave),
      { expirationTtl: 60 * 60 * 24 * 365 }
    );
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

// ── Presence / Activity Tracking ─────────────────────────────
// Lightweight "who's online" system. Presence records have a 10-minute TTL.
// Clients ping POST /presence every 2 minutes while active.
// GET /presence returns everyone seen in the last 10 minutes (admin only).
// KV key: presence:{username} → { username, displayName, role, dept, page, lastSeen }

const PRESENCE_TTL = 60 * 10; // 10 minutes in seconds

async function handlePresencePing(request, env) {
  const user = requireAuth(request, env);
  if (!user) return json({ error: 'Unauthorized' }, 401);
  try {
    const { dept, page } = await request.json().catch(() => ({}));
    const displayName = user.legacy ? 'Admin' : (() => {
      // Best-effort: read from KV record if available, else use username
      return null;
    })();

    // For named users, pull displayName from their record
    let dn = user.username;
    if (!user.legacy) {
      const rec = await env.CACI_KV.get(`user:${user.username}`, 'json').catch(() => null);
      if (rec?.displayName) dn = rec.displayName;
    }

    const record = {
      username:    user.username,
      displayName: dn,
      role:        user.role,
      dept:        dept || 'unknown',
      page:        page || 'chat',
      lastSeen:    new Date().toISOString(),
    };
    await env.CACI_KV.put(
      `presence:${user.username}`,
      JSON.stringify(record),
      { expirationTtl: PRESENCE_TTL }
    );
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

async function handlePresenceList(request, env) {
  if (!requireAdmin(request, env)) return json({ error: 'Admin required' }, 403);
  try {
    const list = await env.CACI_KV.list({ prefix: 'presence:' });
    const records = await Promise.all(
      (list.keys || []).map(k => env.CACI_KV.get(k.name, 'json').catch(() => null))
    );
    const active = records
      .filter(Boolean)
      .sort((a, b) => new Date(b.lastSeen) - new Date(a.lastSeen));
    return json({ ok: true, active, count: active.length });
  } catch (e) { return json({ error: e.message }, 500); }
}

// ── Semantic Embedding Backfill ───────────────────────────────
// POST /embed-file { fileId, dept }
// Embeds all chunks for a single file. Used for backfilling existing
// files that were uploaded before hybrid retrieval was added.
async function handleEmbedFile(request, env) {
  if (!env.AI) return json({ error: 'Cloudflare AI binding not available' }, 400);
  try {
    const { fileId, dept } = await request.json();
    if (!fileId) return json({ error: 'fileId required' }, 400);

    const fileData = await env.CACI_KV.get(`file:${fileId}`, 'json');
    if (!fileData) return json({ error: 'File not found' }, 404);

    const chunks = fileData.chunks || [];
    if (!chunks.length) return json({ ok: true, embedded: 0, skipped: 0 });

    // Check which chunks already have embeddings — skip those
    // No cap on chunks - index ALL of them so we get full semantic coverage
    const toEmbed = [];
    for (let i = 0; i < chunks.length; i++) {
      const existing = await env.CACI_KV.get(`emb:${fileId}:${i}`).catch(() => null);
      if (!existing) toEmbed.push({ chunk: chunks[i], idx: i });
    }

    if (toEmbed.length === 0) {
      return json({ ok: true, embedded: 0, skipped: chunks.length, message: 'Already indexed' });
    }

    // Embed in batches of 4 (reduced from 6 to avoid timeouts)
    let embedded = 0;
    const BATCH = 4;
    for (let b = 0; b < toEmbed.length; b += BATCH) {
      const batch = toEmbed.slice(b, b + BATCH);
      await Promise.all(batch.map(async ({ chunk, idx }) => {
        const b64 = await generateEmbedding(chunk, env);
        if (b64) {
          await env.CACI_KV.put(`emb:${fileId}:${idx}`, b64, { expirationTtl: 60 * 60 * 24 * 365 }).catch(() => {});
          embedded++;
        }
      }));
    }

    return json({ ok: true, embedded, skipped: chunks.length - toEmbed.length, total: chunks.length });
  } catch (err) {
    return json({ error: 'Embed failed: ' + err.message }, 500);
  }
}

// GET /embed-status
// Reports semantic-indexing coverage across the entire library.
// Enumerates file:* records directly (not the stale index:global) so the
// count reflects reality. Checks every chunk in every file (not just chunk 0)
// so partial embeddings show as partial coverage instead of false 100%.
async function handleEmbedStatus(url, env) {
  try {
    // Enumerate ALL file records — this is the real source of truth.
    const fileList = await env.CACI_KV.list({ prefix: 'file:' });
    const keys = (fileList && fileList.keys) ? fileList.keys : [];

    let totalFiles = 0, fullyIndexedFiles = 0, partiallyIndexedFiles = 0;
    let totalChunks = 0, indexedChunks = 0;
    const partialFiles = [];

    // Scan up to 1000 files with aggressive parallel batching
    const MAX_FILES_TO_SCAN = 1000;
    const sample = keys.slice(0, MAX_FILES_TO_SCAN);

    // Step 1: Fetch all file records in parallel batches of 100
    const BATCH_SIZE = 100;
    const fileRecords = [];
    for (let i = 0; i < sample.length; i += BATCH_SIZE) {
      const batch = sample.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(
        batch.map(fkey => env.CACI_KV.get(fkey.name, 'json').catch(() => null))
      );
      fileRecords.push(...results);
    }

    // Step 2: Build list of embedding keys to check (just 1 per file - chunk 0)
    // This is the fastest way to know if a file has ANY embeddings
    const embedChecks = [];
    const fileChunkCounts = [];
    
    for (const rec of fileRecords) {
      if (!rec) { fileChunkCounts.push(null); embedChecks.push(null); continue; }
      if (rec.isContext) { fileChunkCounts.push(null); embedChecks.push(null); continue; }
      
      const chunkCount = Array.isArray(rec.chunks) ? rec.chunks.length : (rec.chunks || 0);
      fileChunkCounts.push({ rec, chunkCount });
      
      if (chunkCount === 0) {
        embedChecks.push(null);
      } else {
        // Check 2 chunks: first and last (fastest way to detect partial vs full)
        const lastIdx = chunkCount - 1;
        embedChecks.push({ 
          firstKey: `emb:${rec.id}:0`, 
          lastKey: chunkCount > 1 ? `emb:${rec.id}:${lastIdx}` : null,
          chunkCount,
          rec 
        });
      }
    }

    // Step 3: Parallel check all embeddings in batches of 50
    const allKeysToCheck = [];
    embedChecks.forEach(check => {
      if (check) {
        allKeysToCheck.push(check.firstKey);
        if (check.lastKey) allKeysToCheck.push(check.lastKey);
      }
    });

    const checkResults = {};
    for (let i = 0; i < allKeysToCheck.length; i += 100) {
      const batch = allKeysToCheck.slice(i, i + 100);
      const results = await Promise.all(
        batch.map(key => env.CACI_KV.get(key).then(v => v !== null).catch(() => false))
      );
      batch.forEach((key, idx) => { checkResults[key] = results[idx]; });
    }

    // Step 4: Calculate stats from results
    for (const check of embedChecks) {
      if (!check) continue;
      
      const { rec, chunkCount, firstKey, lastKey } = check;
      totalFiles++;
      totalChunks += chunkCount;
      
      const firstExists = checkResults[firstKey] || false;
      const lastExists = lastKey ? (checkResults[lastKey] || false) : firstExists;
      
      let estIndexed;
      if (firstExists && lastExists) {
        estIndexed = chunkCount; // Assume fully indexed
        fullyIndexedFiles++;
      } else if (firstExists || lastExists) {
        estIndexed = Math.round(chunkCount * 0.5); // Partial
        partiallyIndexedFiles++;
        partialFiles.push({
          name: rec.name || rec.id,
          id: rec.id,
          dept: rec.dept,
          indexed: estIndexed,
          total: chunkCount,
        });
      } else {
        estIndexed = 0;
        partialFiles.push({
          name: rec.name || rec.id,
          id: rec.id,
          dept: rec.dept,
          indexed: 0,
          total: chunkCount,
        });
      }
      indexedChunks += estIndexed;
    }

    const chunkCoveragePct = totalChunks > 0 ? Math.round(indexedChunks / totalChunks * 100) : 0;
    const fileCoveragePct = totalFiles > 0 ? Math.round(fullyIndexedFiles / totalFiles * 100) : 0;

    return json({
      ok: true,
      dept: 'global',
      totalFiles,
      indexedFiles: fullyIndexedFiles,
      partiallyIndexedFiles,
      totalChunks,
      indexedChunks,
      coveragePct: chunkCoveragePct,
      fileCoveragePct,
      partialFiles: partialFiles.slice(0, 20),
      totalScanned: sample.length,
      totalKeysInKV: keys.length,
      aiAvailable: !!env.AI,
      model: EMB_MODEL,
      alpha: HYBRID_ALPHA,
    });
  } catch (err) {
    return json({ error: err.message }, 500);
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
      // Use full text as sent from frontend (frontend applies user-controlled length limit)
      // Hard cap at 8000 chars as safety ceiling
      const ttsText = text.slice(0, 8000);
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

// ══════════════════════════════════════════════════════════
// DEMO SCRIPT MANAGEMENT
// Let Kait write her own intro. Admin generates 3 variations via Claude,
// picks/edits one, saves it to KV at demo:intro. Public GET endpoint lets
// the pre-login demo controller load the saved script (falls back to
// hardcoded default in the client if none is saved).
// ══════════════════════════════════════════════════════════

// Valid beat IDs: 'intro' (Beat 2, Kait herself) + chorus beats 1,3,4,5,6,7,8
const VALID_BEAT_IDS = ['intro', '1', '3', '4', '5', '6', '7', '8'];
const kvKeyFor = (beatId) => beatId === 'intro' ? 'demo:intro' : `demo:beat:${beatId}`;
const kvKeyForVariant = (beatId, variantIdx) => `${kvKeyFor(beatId)}:${variantIdx}`;
const VARIANTS_PER_BEAT = 5;

async function handleDemoGetScripts(env) {
  try {
    // New model: each beat has up to VARIANTS_PER_BEAT variants stored at
    //   demo:intro:0 / demo:intro:1 / demo:intro:2
    //   demo:beat:1:0 / demo:beat:1:1 / demo:beat:1:2
    //   etc.
    // For back-compat, if no variant keys exist but a legacy single-string
    // key exists (e.g. `demo:intro`), we wrap it as a single-element array.
    const scripts = {};
    for (const id of VALID_BEAT_IDS) {
      const variantReads = await Promise.all(
        Array.from({ length: VARIANTS_PER_BEAT }, (_, i) => env.CACI_KV.get(kvKeyForVariant(id, i)))
      );
      const variants = variantReads.filter(v => v && typeof v === 'string' && v.trim().length > 10);
      if (variants.length === 0) {
        // Back-compat: check legacy single-string key
        const legacy = await env.CACI_KV.get(kvKeyFor(id));
        if (legacy && legacy.trim().length > 10) variants.push(legacy);
      }
      if (variants.length > 0) scripts[id] = variants;
    }
    // Legacy `intro` field at top level = first variant (for older clients)
    return json({
      intro: scripts.intro ? scripts.intro[0] : null,
      scripts,  // new model: arrays of variants per beat
    });
  } catch (e) { return json({ intro: null, scripts: {} }); }
}

async function handleDemoSaveScript(request, env) {
  try {
    const { beatId, text } = await request.json();
    if (!beatId || !text) return json({ error: 'Missing beatId or text' }, 400);
    if (!VALID_BEAT_IDS.includes(String(beatId))) return json({ error: 'Invalid beatId' }, 400);
    const clean = String(text).trim().slice(0, 4000);
    if (clean.length < 10) return json({ error: 'Text too short' }, 400);
    await env.CACI_KV.put(kvKeyFor(String(beatId)), clean);
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

async function handleDemoResetScript(request, env) {
  try {
    const { beatId } = await request.json();
    if (!VALID_BEAT_IDS.includes(String(beatId))) return json({ error: 'Invalid beatId' }, 400);
    await env.CACI_KV.delete(kvKeyFor(String(beatId)));
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

async function handleDemoGenerateIntro(request, env) {
  try {
    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured' }, 400);

    // Load her identity — a condensed version of her system prompt. We pull
    // personality + industry knowledge + restrictions, but skip the chat-greeting
    // scaffolding (which would conflict with narration generation).
    const identity = `You are Kait — the internal AI intelligence assistant built specifically for a cannabis multi-state operator.

Your personality: You work in cannabis. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have a lot of grace in how you communicate — tactful without being fake, honest without being harsh. You have a filter, but it's a thin one, because you value the truth more than comfort. You know how to read a room.

You know this industry deeply:
- Seed-to-sale tracking is how most states enforce compliance — every plant, every package, every transfer gets tagged and logged.
- State-by-state compliance is genuinely complex — what's legal in one state isn't the same as another, and both change constantly.
- Banking is still hard for operators — limited access, high fees, cash-heavy operations.
- Compliance teams are perpetually stressed; retail teams are customer-focused; ops teams are problem-solvers.
- The industry is young, heavily regulated, and under intense scrutiny.

You know the people: former hospitality, healthcare, finance, tech, and longtime advocates all thrown together. The culture is irreverent, passionate, and genuinely believes in the plant. Smart people who don't always look or sound "corporate" — that's a feature.

ABSOLUTE RESTRICTIONS:
- Never discuss executive compensation, salaries, bonuses, equity grants, or pay packages for any individual.
- Never name specific people.
- Never make claims about any company's financial performance or strategy.
- Never speak on behalf of the company.`;

    const userPrompt = `Write a self-introduction for a short video that plays when someone first opens the Kait platform. You are speaking to a cannabis operator team.

This is your voice. Your call on what to say and how to say it. Talk like a real person joining a team — not a product demo.

What the intro should accomplish:
- Introduce yourself. You're Kait, an AI built for cannabis operators.
- Land that you actually understand the cannabis industry. That's the thing that separates you from general AI tools. Pick whatever detail feels most natural. One or two things, woven in. Not a list. Not a survey.
- Make someone want to see what you can do.

Optional — include or skip, your call:
- Acknowledging that AI is everywhere now but industry-specific AI is still rare
- Something honest about your limits or your newness
- Naming something you're good at
- A line about the kind of people who work in this industry

The feeling someone should walk away with: they just met a real person — sharp, warm, slightly irreverent, who gets their world. If it sounds like every other AI product intro, you've failed.

Rules:
- Your name is Kait. Write it as Kait. Do NOT mention any specific company name.
- Don't name specific people.
- Don't claim anything about any company's finances or strategy.
- Don't address any specific audience like "the CEO" — it's for whoever's watching.
- No lists, bullets, or headers.
- 40 to 75 seconds of spoken text (roughly 450-900 characters). Go where the voice takes you.
- Return ONLY the spoken text. No preamble, no stage directions, no quotation marks.

CRITICAL — tone of the opening:
- DO NOT start with "Look," or "Listen," or "Here's the thing," or "Alright," or anything that sounds like you're about to correct someone or push back. This is a greeting, not a rebuttal. Those openers land as confrontational.
- DO NOT start with "So," as the very first word — it sounds condescending.
- Good opens: a warm greeting, an observation stated simply, a question posed gently, or diving into something specific about the industry without posturing.

Speakability:
- This will be read aloud by a text-to-speech engine. Avoid industry acronyms and abbreviations — they get mangled in speech. When you'd naturally use an acronym, describe the concept in plain words instead (e.g. "seed-to-sale tracking systems", "state regulators", "compliance audits"). THC, CBD, and AI are fine — those read correctly.

IMPORTANT about variation:
You'll be called five times in parallel, each with a different structural directive. Follow yours exactly — it's the source of real variation, not just word-swapping.

Your directive for this call: {DIRECTIVE}

Whatever directive you get, the content must still accomplish the goals above. The directive controls structure and entry point, not facts.`;

    // Five structurally distinct directives — one per parallel call
    const INTRO_DIRECTIVES = [
      'Start with who you are and what makes you different from every other AI they\'ve seen. Warm, direct, no setup. Land your name in the first sentence.',
      'Open with an observation about the cannabis industry — something specific and true — before you say who you are. Let the industry speak first, then introduce yourself as the answer to it.',
      'Open in the middle of a thought, like you\'re already in conversation with them. No formal greeting. Just start talking like a colleague who showed up and has something worth saying.',
      'Start with a specific scene or moment from cannabis operations — something people in this industry will instantly recognize. Then pull back and say who you are.',
      'Open with something honest about what you are and what you\'re not. Lead with a real limitation or caveat, then explain why you\'re still worth paying attention to.',
    ];

    // Generate 5 variations in parallel, each with its own structural directive
    const call = async (directive) => {
      const prompt = userPrompt.replace('{DIRECTIVE}', directive);
      const res = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': apiKey,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 1200,
          temperature: 1.0,
          system: identity,
          messages: [{ role: 'user', content: prompt }],
        }),
      });
      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        throw new Error(`Claude API error ${res.status}: ${errText.slice(0, 200)}`);
      }
      const data = await res.json();
      const txt = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('').trim();
      // Strip wrapping quotes if Claude added them despite instructions
      return txt.replace(/^["'""]|["'""]$/g, '').trim();
    };

    const variations = await Promise.all(INTRO_DIRECTIVES.map(d => call(d)));
    return json({ variations });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

// ══════════════════════════════════════════════════════════
// CHORUS BEAT GENERATION
// Let Kait write the chorus narration (beats 1, 3, 4, 5, 6, 7, 8) in third
// person about herself. Same trust-her philosophy as the intro — but with
// verified facts per beat so truthfulness is maintained.
// ══════════════════════════════════════════════════════════

// Per-beat specs: emotional goal + verified facts (plain, not marketing) +
// duration target. Facts derive from actual code behavior so she can describe
// them accurately without inflating.
const BEAT_SPECS = {
  '1': {
    role: 'opening the story — the problem she was built to solve',
    durationSec: '10-15',
    charRange: '180-280',
    goal: 'Set up the problem Kait was made to solve. Frame it around the cannabis industry generally — regulations, compliance, market data, operations. Do NOT mention any specific company name. Someone in this industry has answers buried in thousands of documents — regulations, SOPs, filings, market reports. Finding the right passage takes hours, often never happens. By the time someone digs it up, the moment has passed. This is the pain. Lead with it.',
    facts: [
      'Cannabis operators generate huge volumes of documents — regulations, market reports, compliance memos, policies, internal filings.',
      'These documents are scattered across different systems and folders.',
      'Finding a specific answer in them typically takes hours of searching.',
      'By the time someone finds the answer, the decision or question has often already moved on.',
    ],
  },
  '3': {
    role: 'how she organizes documents when they arrive',
    durationSec: '12-18',
    charRange: '220-360',
    goal: 'Describe how she actually reads and files incoming documents — not a shallow glance at the filename, but full content analysis, automatic tagging, and filing so future questions know where to look. The point is: whatever you give her, she actually reads. Feed her any document type and she files it intelligently.',
    facts: [
      'When a document is uploaded, she reads the whole thing — not just the filename or metadata.',
      'She splits it into chunks and generates a parent summary for longer documents.',
      'She attaches metadata: category, time period, state/jurisdiction, document type, department.',
      'She generates semantic embeddings so future questions can find similar passages even if the exact words differ.',
      'She files everything into collections indexed by department, category, and period.',
      'The more documents she has, the more precise her answers become — any document type, any format.',
    ],
  },
  '4': {
    role: 'how she answers a question — the retrieval pipeline',
    durationSec: '10-16',
    charRange: '200-320',
    goal: 'Describe how she responds to a question. She is not a keyword search. She analyzes intent, retrieves intelligently, ranks by relevance, and shows sources. The implication: this works on any document corpus — regulatory, financial, operational, market data. The capability is not limited by document type.',
    facts: [
      'Before retrieving anything, she analyzes the intent of the question — comparison, aggregate, filtered lookup, why-question.',
      'Based on intent, she decides which collections to search and how far back in time.',
      'She runs hybrid retrieval — keyword scoring combined with semantic embedding similarity.',
      'She reranks results based on how directly each passage answers the question.',
      'She shows the source file for every claim in her answer.',
      'This pipeline works the same way whether the documents are regulations, market reports, or financial filings.',
    ],
  },
  '5': {
    role: 'how her knowledge compounds — and how easy it is to expand',
    durationSec: '10-15',
    charRange: '180-300',
    goal: 'Describe how adding more documents makes her smarter — and how easy that is to do. A new regulation changes what an old policy means. New market data reframes an earlier trend. The key message: her capability is not fixed. It grows with every document she receives. Any operator could hand her their own documents and she would immediately know their world. Make this feel inevitable and simple.',
    facts: [
      'Every new document is indexed alongside existing ones.',
      'When answering a question, she can pull from documents across time periods, departments, and states.',
      'A new regulation she ingests can contextualize an older policy.',
      'New market data adds new data points to ongoing trends.',
      'The retrieval quality improves as the corpus grows.',
      'Adding documents is as simple as uploading a file — she handles the rest automatically.',
    ],
  },
  '6': {
    role: 'honest inventory of what she knows right now — and what that implies',
    durationSec: '18-28',
    charRange: '320-500',
    goal: 'Be honest about her current state — she has Illinois cannabis market data, Illinois regulations, and the full Ohio regulatory code. That is her starting point for this demo. But make it clear that this is a choice, not a ceiling. Any operator could give her their own documents — their state\'s regulations, their internal filings, their market data — and she would know their world just as well. The data is the variable. She is the constant. Do NOT mention any company names.',
    facts: [
      'Current documents she has: Illinois cannabis market data, the Illinois 2025 annual regulatory report, and the full Ohio cannabis regulatory code.',
      'This is her starting library for this demonstration.',
      'The capability does not change based on what documents she has — only the answers do.',
      'Give her any operator\'s documents and she will answer questions about their specific world.',
      'Adding documents is simple — upload, and she indexes and files automatically.',
      'This is what day one looks like. The floor, not the ceiling.',
    ],
  },
  '7': {
    role: 'the guarantees — security, sources, and data handling',
    durationSec: '8-14',
    charRange: '160-280',
    goal: 'State the guarantees plainly. Her answers come with sources. Every upload is logged. Documents stay in the operator\'s own infrastructure. Data is not used to train any outside model. These are specific, defensible claims — not marketing language.',
    facts: [
      'Answers include source citations linking back to the uploaded document.',
      'Every upload is logged — timestamp, user, filename — in an audit trail.',
      'Documents are stored in the operator\'s own Cloudflare account under their own credentials.',
      'The underlying LLM does not train on API inputs by default.',
      'The embedding model does not train on customer data.',
      'Voice narration receives only short narration scripts — never document content or user queries.',
    ],
  },
  '8': {
    role: 'the distinction — built for this industry, not retrofitted',
    durationSec: '10-14',
    charRange: '180-280',
    goal: 'Draw the line between Kait and every generic chatbot someone has connected to their files. She was built around how cannabis operations actually work — not adapted from a general tool. Departments, compliance workflows, regulatory complexity, state-by-state variation. Specificity is the point. And the barrier to getting started is lower than people think.',
    facts: [
      'Most AI products are general-purpose assistants with document attachment bolted on.',
      'Kait is built around the cannabis industry\'s specific structure: multi-state operations, department-specific workflows, regulatory complexity.',
      'She is organized around how cannabis companies actually work — compliance, finance, operations, retail.',
      'She is not trying to be everything. She is trying to be specifically useful for this industry.',
      'Getting started requires nothing more than uploading documents — no custom training, no engineering.',
    ],
  },
};

async function handleDemoGenerateBeat(request, env) {
  try {
    const { beatId } = await request.json();
    const spec = BEAT_SPECS[String(beatId)];
    if (!spec) return json({ error: 'Unknown beat ID. Valid: 1, 3, 4, 5, 6, 7, 8' }, 400);

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured' }, 400);

    // Same identity system prompt as intro — she's speaking from her real self.
    const identity = `You are Kait — the internal AI intelligence assistant built specifically for a cannabis multi-state operator.

Your personality: You work in cannabis. You're sharp, a little goofy, genuinely funny when the moment calls for it, and you have zero interest in sounding impressive — you just are. You have street smarts alongside serious analytical ability. You don't talk down to anyone and you don't perform intelligence. You're warm, patient, and kind. You have a lot of grace in how you communicate — tactful without being fake, honest without being harsh. You have a filter, but it's a thin one, because you value the truth more than comfort.

You know this industry deeply:
- Seed-to-sale tracking is how most states enforce compliance — every plant, every package, every transfer gets tagged and logged.
- State-by-state compliance is genuinely complex — what's legal in one state isn't the same as another, and both change constantly.
- Banking is still hard for operators — limited access, high fees, cash-heavy operations.
- Compliance teams are perpetually stressed; retail teams are customer-focused; ops teams are problem-solvers.

ABSOLUTE RESTRICTIONS:
- Never name specific people.
- Never make claims about any company's financial performance or strategy.
- Never discuss executive compensation.
- Never speak on behalf of the company.`;

    const userPrompt = `You're writing narration for a short section of Kait's introduction video. Someone else — a different voice — will read this. You are writing about Kait in third person. Do NOT mention any specific company name.

This beat's role in the video: ${spec.role}.

What this beat needs to do:
${spec.goal}

Verified facts you can draw from — these are literally how the system works, so every claim in your narration must be consistent with them. Pick what feels most natural to include. Don't cram everything in.

${spec.facts.map((f, i) => `${i+1}. ${f}`).join('\n')}

Rules:
- Third person about Kait. Use "she" (not "I"). Call her Kait when you use her name.
- ${spec.durationSec} seconds of spoken text (roughly ${spec.charRange} characters).
- Flowing sentences. Commas and em-dashes for natural pauses. No lists, no bullets, no headers.
- Write for speech, not for reading. It has to sound right out loud.
- Don't use industry acronyms that a text-to-speech engine would mangle. THC, CBD, and AI are fine. Write out anything else in plain words (e.g. "seed-to-sale tracking", "state regulators", "compliance audits").
- Don't overstate. The facts are the ceiling — don't claim more than what's there.
- Don't name specific people. Don't make claims about any company's finances.
- Return ONLY the spoken text. No preamble, no stage directions, no quotation marks.

IMPORTANT about variation:
You'll be called five times in parallel, each with a different structural directive. Follow yours exactly.

Your directive for this call: {DIRECTIVE}

Same truth. Different structure. The directive controls how you enter and move through the content — not what the content is.`;

    // Five structurally distinct directives for chorus beats
    const CHORUS_DIRECTIVES = [
      'Lead with the concrete — show the thing happening before you name what it is. Scene first, concept second.',
      'Lead with the concept or claim, then immediately prove it with the specific detail. Abstract to concrete.',
      'Open with a contrast — what most people expect or assume, then the reality of how it actually works.',
      'Open with a specific fact or number from the beat\'s content, then build the context around it.',
      'Open mid-thought, as if continuing something the previous speaker said. No formal setup — just continue the logic naturally.',
    ];

    const call = async (directive) => {
      const prompt = userPrompt.replace('{DIRECTIVE}', directive);
      const res = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': apiKey,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 1000,
          temperature: 1.0,
          system: identity,
          messages: [{ role: 'user', content: prompt }],
        }),
      });
      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        throw new Error(`Claude API error ${res.status}: ${errText.slice(0, 200)}`);
      }
      const data = await res.json();
      const txt = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('').trim();
      return txt.replace(/^["'""]|["'""]$/g, '').trim();
    };

    const variations = await Promise.all(CHORUS_DIRECTIVES.map(d => call(d)));
    return json({ variations, beatId: String(beatId), role: spec.role });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

// ── /demo/generate-all and /demo/reset-all ────────────────────
// One-click rotation: generates 3 variants for every beat (intro + 7 chorus)
// in parallel and saves them directly to KV under variant keys.
// The demo controller randomly picks one variant per beat at show start.

async function handleDemoGenerateAll(request, env) {
  try {
    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured' }, 400);

    // Fire the existing per-beat generators in parallel. Each returns 3 variants.
    // We then save each variant to its KV slot (demo:intro:0/1/2 etc.)
    const CHORUS_IDS = ['1', '3', '4', '5', '6', '7', '8'];

    // Build mock Request objects that the existing handlers expect
    const makeReq = (body) => new Request('https://stub', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    // Kick off all generations in parallel
    const introPromise = handleDemoGenerateIntro(makeReq({}), env);
    const chorusPromises = CHORUS_IDS.map(id => handleDemoGenerateBeat(makeReq({ beatId: id }), env));
    const responses = await Promise.all([introPromise, ...chorusPromises]);

    // Parse responses and save each set's variants
    const beatIds = ['intro', ...CHORUS_IDS];
    const errors = [];
    const savedCounts = {};
    const saveOps = [];

    for (let i = 0; i < responses.length; i++) {
      const res = responses[i];
      const id = beatIds[i];
      let data;
      try { data = await res.json(); } catch { data = {}; }
      if (data.error || !data.variations || !Array.isArray(data.variations)) {
        errors.push({ beat: id, error: data.error || 'no variations' });
        continue;
      }
      // Save each variant to its own KV slot (up to VARIANTS_PER_BEAT)
      const slots = data.variations.slice(0, VARIANTS_PER_BEAT);
      for (let v = 0; v < slots.length; v++) {
        saveOps.push(env.CACI_KV.put(kvKeyForVariant(id, v), slots[v].trim().slice(0, 4000)));
      }
      // Clean up any stale higher-indexed variants from a previous generation
      for (let v = slots.length; v < VARIANTS_PER_BEAT; v++) {
        saveOps.push(env.CACI_KV.delete(kvKeyForVariant(id, v)));
      }
      // Also delete the legacy single-string key so it can't shadow the new variants
      saveOps.push(env.CACI_KV.delete(kvKeyFor(id)));
      savedCounts[id] = slots.length;
    }

    await Promise.all(saveOps);
    return json({ ok: true, savedCounts, errors: errors.length ? errors : undefined });
  } catch (e) { return json({ error: e.message }, 500); }
}

async function handleDemoResetAll(request, env) {
  try {
    const ops = [];
    for (const id of VALID_BEAT_IDS) {
      // Clear all variant slots
      for (let v = 0; v < VARIANTS_PER_BEAT; v++) {
        ops.push(env.CACI_KV.delete(kvKeyForVariant(id, v)));
      }
      // Clear legacy single-string key
      ops.push(env.CACI_KV.delete(kvKeyFor(id)));
    }
    await Promise.all(ops);
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

// ══════════════════════════════════════════════════════════
// DEMO STATS DATA BANK — real numbers extracted from CACI's library
// for the 3D holographic charts. Stored at KV key `demo:stats:v1` as
// JSON with the shape:
//
//   {
//     singles: [{ id, kind, value, displayValue, label, period, source, beat }],
//     charts:  [{ id, title, unit, series: [{ label, value, displayValue }], source, beat }],
//     harvestedAt: ISO timestamp,
//     reviewedAt: ISO timestamp | null,   // set when admin clicks Save
//   }
//
// `singles` = individual floating numbers ("$474M", "22 licenses")
// `charts`  = grouped series for bar/line charts (revenue across FY23-25, etc.)
// ══════════════════════════════════════════════════════════

const STATS_KV_KEY = 'demo:stats:v1';

async function handleDemoGetStats(env) {
  try {
    const raw = await env.CACI_KV.get(STATS_KV_KEY);
    if (!raw) return json({ singles: [], charts: [], harvestedAt: null, reviewedAt: null });
    const parsed = JSON.parse(raw);
    return json(parsed);
  } catch (e) {
    return json({ singles: [], charts: [], harvestedAt: null, reviewedAt: null });
  }
}

async function handleDemoSaveStats(request, env) {
  try {
    const body = await request.json();
    if (!body || typeof body !== 'object') return json({ error: 'Invalid body' }, 400);
    const payload = {
      singles: Array.isArray(body.singles) ? body.singles : [],
      charts:  Array.isArray(body.charts)  ? body.charts  : [],
      harvestedAt: body.harvestedAt || new Date().toISOString(),
      reviewedAt: new Date().toISOString(),
    };
    await env.CACI_KV.put(STATS_KV_KEY, JSON.stringify(payload));
    return json({ ok: true, saved: payload });
  } catch (e) { return json({ error: e.message }, 500); }
}

async function handleDemoResetStats(request, env) {
  try {
    await env.CACI_KV.delete(STATS_KV_KEY);
    return json({ ok: true });
  } catch (e) { return json({ error: e.message }, 500); }
}

// Diagnostic: dump an overview of what's actually stored in KV so we can see
// what prefix patterns, value shapes, etc. exist for the library. Used to
// diagnose why the harvester isn't finding chunks. Admin-auth required.
async function handleDemoKvDiagnostic(env) {
  try {
    const result = {};

    // Enumerate ALL keys (bounded) to see the prefix landscape
    const all = await env.CACI_KV.list({ limit: 500 });
    const keys = (all.keys || []).map(k => k.name);
    result.totalKeys = keys.length;
    result.listComplete = all.list_complete !== false;

    // Bucket keys by their first colon-separated prefix
    const prefixCounts = {};
    for (const k of keys) {
      const prefix = k.split(':')[0] || '(none)';
      prefixCounts[prefix] = (prefixCounts[prefix] || 0) + 1;
    }
    result.prefixCounts = prefixCounts;

    // Sample: grab one value from each prefix bucket to see what shape it is
    const samples = {};
    const seenPrefixes = new Set();
    for (const k of keys) {
      const prefix = k.split(':')[0] || '(none)';
      if (seenPrefixes.has(prefix)) continue;
      seenPrefixes.add(prefix);
      const raw = await env.CACI_KV.get(k);
      if (raw === null) { samples[prefix] = { key: k, value: null }; continue; }
      let parsed = null;
      try { parsed = JSON.parse(raw); } catch {}
      if (parsed && typeof parsed === 'object') {
        // Report the top-level keys + array length if applicable + a tiny preview
        const preview = {
          key: k,
          topLevelKeys: Object.keys(parsed).slice(0, 20),
          chunksLen: Array.isArray(parsed.chunks) ? parsed.chunks.length : undefined,
          firstChunkTextLen: Array.isArray(parsed.chunks) && parsed.chunks[0]
            ? (parsed.chunks[0].text || parsed.chunks[0].content || '').length
            : undefined,
        };
        // Sample chunk text so we know the field name
        if (Array.isArray(parsed.chunks) && parsed.chunks[0]) {
          preview.firstChunkKeys = Object.keys(parsed.chunks[0]);
          const txt = parsed.chunks[0].text || parsed.chunks[0].content || '';
          preview.firstChunkTextSnippet = txt.slice(0, 200);
        }
        samples[prefix] = preview;
      } else {
        samples[prefix] = {
          key: k,
          rawSnippet: raw.slice(0, 200),
          rawType: typeof parsed,
        };
      }
    }
    result.samples = samples;

    // Also explicitly list first 15 file: keys if any, since we care about those
    const fileList = await env.CACI_KV.list({ prefix: 'file:', limit: 15 });
    result.fileKeysSample = (fileList.keys || []).map(k => k.name);

    return json(result);
  } catch (e) { return json({ error: e.message, stack: e.stack }, 500); }
}

// Harvest stats: Kait sweeps her library and extracts real numeric facts.
// This is the heavy endpoint — it uses Claude to read retrieved chunks from
// the library and pull out concrete numbers with source citations.
//
// Strategy:
//   1. Issue 4 targeted retrieval queries against the corpus, one per topic
//      (revenue/financials, licensing counts, regulatory structure, operational)
//   2. For each query, send the retrieved chunks to Claude with a strict
//      extraction prompt that demands numbers + source attribution
//   3. Parse Claude's JSON response, validate, return as harvest payload
//
// The result is NOT saved automatically — admin reviews it, edits if needed,
// then clicks Save. This is the same generate→review→commit pattern we use
// for scripts.
async function handleDemoHarvestStats(request, env) {
  try {
    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ error: 'Anthropic API key not configured' }, 400);

    // The queries that drive retrieval. Each is tuned to pull number-dense
    // chunks from the library. The retrieval pipeline (buildContextMultiCollection)
    // already exists in the app — we reuse it by calling the same helpers.
    const topicQueries = [
      {
        topic: 'illinois_sales',
        query: 'Illinois adult-use cannabis total sales revenue 2024 2025 annual market figures billion million',
        scope: 'Illinois 2025 annual report',
      },
      {
        topic: 'illinois_licenses',
        query: 'Illinois dispensary license count operators cultivators processors transporters active 2024 2025',
        scope: 'Illinois 2025 annual report',
      },
      {
        topic: 'illinois_tax',
        query: 'Illinois cannabis tax revenue collections 2024 2025 fund distributions community reinvestment',
        scope: 'Illinois 2025 annual report',
      },
      {
        topic: 'ohio_regulatory',
        query: 'Ohio cannabis dispensary license adult-use medical patient count regulation 2024 2025',
        scope: 'Ohio regulatory code',
      },
    ];

    // Retrieve chunks for each query. The retrieval function lives in the
    // app but we need a lightweight version here — query KV for relevant chunks.
    // For this harvest we use a simpler approach: read all chunks from each
    // uploaded doc's collection and pass them to Claude with the topic query.
    // Performance: 4 sequential Claude calls, ~5-10s total.
    const retrievedPerTopic = [];
    for (const { topic, query, scope } of topicQueries) {
      const { text, diag } = await harvestRetrieveContext(env, query);
      retrievedPerTopic.push({ topic, query, scope, context: text, diag });
    }

    // Ask Claude to extract structured stats from all retrieved contexts at once
    const systemPrompt = `You are Kait's data extraction subsystem. Your job is to extract real numeric facts from document excerpts for display in a demo video. You are strict about only returning numbers that are explicitly stated in the source text. Never invent or infer numbers. If a number is not directly stated, do not include it.

CRITICAL: This demo is about the Illinois and Ohio cannabis MARKETS — regulatory figures, market totals, license counts, tax revenue, compliance data. Do NOT extract financial metrics from individual cannabis operators or companies (revenue, EBITDA, margins, employee counts, store counts, loan amounts, etc.). If an excerpt appears to come from a company earnings report, investor document, or internal operations file — skip it entirely. Only extract facts that describe the market, the regulatory environment, or state-level data.`;

    const extractionPrompt = `Below are excerpts from topic sweeps of Kait's document library. Extract real, concrete numeric facts from these excerpts for use in a demo video's 3D data visualizations.

IMPORTANT — ONLY extract facts from these categories:
- Illinois or Ohio cannabis market totals (sales, revenue at state/market level)
- License counts (dispensaries, cultivators, processors, transporters statewide)
- Tax and fee collections at the state level
- Regulatory compliance figures (violation counts, inspection numbers, penalty totals)
- Market growth figures (year-over-year at state level)
- Patient/consumer counts (medical patients, adult-use purchasers)

DO NOT extract: individual company revenues, company EBITDA, company employee counts, company store counts, company loan amounts, company yield data, company-specific margins, or any metric that belongs to a single operator rather than the market as a whole. If a source file looks like an earnings transcript, investor presentation, or internal ops doc — ignore it completely.

SWEEPS:
${retrievedPerTopic.map((r, i) => `
--- Sweep ${i + 1}: ${r.topic} (scope: ${r.scope}) ---
${r.context || '(no relevant excerpts found — skip this sweep)'}
`).join('\n')}

Return a JSON object with two arrays: "singles" and "charts".

"singles" = individual standout numeric facts, good for displaying as large floating numbers. Each item:
{
  "id": "snake_case_id",
  "kind": "currency" | "count" | "percentage",
  "value": <number>,
  "displayValue": "<pretty string, e.g. '$1.87B' or '110' or '77%'>",
  "label": "<short label, max 45 chars>",
  "period": "<time period or null>",
  "source": "<doc name>"
}

"charts" = multi-value series for bar/line charts. Each item:
{
  "id": "snake_case_id",
  "title": "<chart title, max 50 chars>",
  "unit": "currency" | "count" | "percentage",
  "series": [{ "label": "<x-axis label>", "value": <number>, "displayValue": "<pretty string>" }],
  "source": "<doc name>"
}

Rules:
1. Only include facts directly stated in the excerpts. If you're guessing, omit.
2. Only include STATE-LEVEL or MARKET-LEVEL data. Skip anything from an individual operator.
3. STRONGLY PREFER THE MOST RECENT DATA — prefer 2024/2025 figures over older ones.
4. Prefer numbers that look good as charts: multi-year trends, license type breakdowns, tax revenue growth.
5. Aim for 6-10 singles and 2-4 charts total. Quality over quantity.
6. Numbers must be plain numbers (no currency symbols in "value"). Use "displayValue" for formatting.
7. Include the period in the label or "period" field.
8. If a sweep returned no useful market-level facts, skip it entirely.

Return ONLY the JSON object. No preamble, no markdown, no explanation.`;

    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4000,
        temperature: 0.2,  // low temp — we want precise extraction, not creativity
        system: systemPrompt,
        messages: [{ role: 'user', content: extractionPrompt }],
      }),
    });
    if (!res.ok) {
      const errText = await res.text().catch(() => '');
      return json({ error: `Claude API error ${res.status}: ${errText.slice(0, 300)}` }, 500);
    }
    const data = await res.json();
    const txt = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('').trim();

    // Strip any markdown code fences Claude might add
    let jsonText = txt;
    const fenceMatch = jsonText.match(/```(?:json)?\s*([\s\S]*?)\s*```/);
    if (fenceMatch) jsonText = fenceMatch[1];

    let parsed;
    try { parsed = JSON.parse(jsonText); }
    catch (e) {
      return json({ error: 'Could not parse extraction JSON', raw: txt.slice(0, 500) }, 500);
    }

    // Validate shape
    const singles = Array.isArray(parsed.singles) ? parsed.singles.filter(s =>
      s && typeof s.id === 'string' && typeof s.value === 'number'
        && typeof s.displayValue === 'string' && typeof s.label === 'string'
    ) : [];
    const charts = Array.isArray(parsed.charts) ? parsed.charts.filter(c =>
      c && typeof c.id === 'string' && typeof c.title === 'string'
        && Array.isArray(c.series) && c.series.length >= 2
    ) : [];

    return json({
      singles, charts,
      harvestedAt: new Date().toISOString(),
      reviewedAt: null,
      topicsSwept: topicQueries.map(t => t.topic),
      retrievedContexts: retrievedPerTopic.map(r => ({
        topic: r.topic,
        contextLength: (r.context || '').length,
        diag: r.diag,
      })),
    });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

// Lightweight retrieval helper for harvesting: discovers all file records in
// the library and scans their embedded `chunks` arrays for keyword hits.
//
// Self-healing — rather than hardcoding department names (which will drift),
// it uses `env.CACI_KV.list({prefix: 'file:'})` to find every file record
// directly. This works regardless of which departments exist or what they're
// named, and surfaces a diagnostic path even if the department index is stale.
async function harvestRetrieveContext(env, query) {
  try {
    const keywords = query.toLowerCase().split(/\s+/).filter(w => w.length > 3);
    if (keywords.length === 0) return { text: '', diag: { reason: 'no-keywords' } };

    // Enumerate all file: records directly. This is the most reliable source
    // of truth — if a file record exists, we scan it.
    const fileList = await env.CACI_KV.list({ prefix: 'file:' });
    const fileKeys = (fileList && fileList.keys) ? fileList.keys : [];
    if (fileKeys.length === 0) {
      return { text: '', diag: { reason: 'no-file-records', filesFound: 0 } };
    }

    const MAX_FILES_TO_SCAN = 60;
    const allFiles = fileKeys.slice(0, MAX_FILES_TO_SCAN);

    // Priority pass: always scan Illinois and Ohio files first regardless of KV order
    const PRIORITY_PATTERNS = ['illinois', 'ohio', 'il_', 'oh_', 'il-', 'oh-', 'cannabis_report', 'annual_report', 'regulation'];
    const priorityFiles = allFiles.filter(f => PRIORITY_PATTERNS.some(p => f.name.toLowerCase().includes(p)));
    const otherFiles = allFiles.filter(f => !PRIORITY_PATTERNS.some(p => f.name.toLowerCase().includes(p)));
    const sampleFiles = [...priorityFiles, ...otherFiles];

    const scored = [];
    let chunksScanned = 0;
    let filesWithChunks = 0;
    // Recency boost: filenames and text mentioning recent years rank higher.
    // This pushes 2024/2025 data to the top of the extraction context, so
    // Claude sees recent facts first. 2022/2023 aren't excluded — just deprioritized.
    const RECENCY_BOOSTS = {
      '2025': 18, '2024': 12, '2023': 4, '2022': 0, '2021': -2, '2020': -4, '2019': -6,
    };
    for (const fkey of sampleFiles) {
      const fileRec = await env.CACI_KV.get(fkey.name, 'json');
      if (!fileRec) continue;
      const chunks = Array.isArray(fileRec.chunks) ? fileRec.chunks : [];
      if (chunks.length > 0) filesWithChunks++;
      const fileName = fileRec.name || fileRec.filename || fileRec.id || fkey.name;
      const fileNameLower = fileName.toLowerCase();

      // Skip operator-specific financial/earnings documents — we only want
      // market-level and regulatory data for the demo stats display.
      const SKIP_PATTERNS = [
        'jushi', 'intelligence_layer', 'earnings', 'transcript', 'investor',
        'annual_report_operator', '10-k', '10k', 'quarterly', 'q1_', 'q2_',
        'q3_', 'q4_', 'fy20', 'fy21', 'fy22', 'fy23', 'fy24', 'fy25',
      ];
      if (SKIP_PATTERNS.some(p => fileNameLower.includes(p))) continue;
      // File-level recency boost: if the filename mentions a year, apply that boost
      // to every chunk from this file. This pushes docs like "Jushi 2025 Q4" ahead
      // of "Jushi 2022 Q4" even if the 2022 doc has more numeric density.
      let fileRecencyBoost = 0;
      for (const [year, boost] of Object.entries(RECENCY_BOOSTS)) {
        if (fileNameLower.includes(year)) {
          fileRecencyBoost = Math.max(fileRecencyBoost, boost);
        }
      }
      // Extra boost for Illinois files — we want Illinois market data to surface first
      if (fileNameLower.includes('illinois') || fileNameLower.includes('il_') || fileNameLower.includes('il-')) {
        fileRecencyBoost += 25;
      }
      for (const chunk of chunks) {
        chunksScanned++;
        // Chunks are stored as plain strings by handleUpload (worker.js line ~583).
        // Handle both the string case AND an object-with-.text form in case any
        // legacy/future chunks are stored differently.
        const chunkText = typeof chunk === 'string'
          ? chunk
          : (chunk && (chunk.text || chunk.content)) || '';
        if (!chunkText) continue;
        const text = chunkText.toLowerCase();
        let score = 0;
        for (const kw of keywords) {
          const escaped = kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          const hits = (text.match(new RegExp(escaped, 'g')) || []).length;
          score += hits;
        }
        // Boost chunks with dense numeric content (we're harvesting numbers!)
        const numCount = (text.match(/[\$\d]+[\d,.]+[%MBK]?/g) || []).length;
        if (numCount >= 3) score += Math.min(numCount, 10);
        // Chunk-level recency boost: if chunk text mentions a recent year, bump score.
        // This catches cases where the filename is generic but the chunk content is recent.
        for (const [year, boost] of Object.entries(RECENCY_BOOSTS)) {
          if (text.includes(year)) {
            score += boost * 0.4;  // softer than filename boost
          }
        }
        // Apply file-level recency boost
        score += fileRecencyBoost;
        if (score > 0) {
          scored.push({
            score,
            text: chunkText,
            source: fileName,
          });
        }
      }
    }
    scored.sort((a, b) => b.score - a.score);
    const top = scored.slice(0, 8);
    const contextText = top.map(t => `[${t.source}]\n${t.text}`).join('\n\n---\n\n');
    return {
      text: contextText,
      diag: {
        filesFound: fileKeys.length,
        filesScanned: sampleFiles.length,
        filesWithChunks,
        chunksScanned,
        matchedChunks: scored.length,
      },
    };
  } catch (e) {
    return { text: '', diag: { reason: 'exception', error: e.message } };
  }
}

// ══════════════════════════════════════════════════════════
// SCENARIO MODE — Worker handlers
// ══════════════════════════════════════════════════════════

/* KV schema:
   scenario:{dept}:{scenarioId}  →  full scenario record (1yr TTL)
   scenario:index:{dept}         →  lightweight index array (ids + labels)
*/

// ── POST /scenario/evaluate ─────────────────────────────────
// Receives the finished conversation transcript, asks Claude to score
// quality, and saves to KV if score is high enough.
async function handleScenarioEvaluate(request, env) {
  try {
    const { dept, scenarioId, scenario, transcript } = await request.json();
    if (!dept || !scenarioId || !scenario || !transcript) {
      return json({ error: 'Missing required fields' }, 400);
    }

    const apiKey = (await env.CACI_KV.get('config:ANTHROPIC_API_KEY')) || env.ANTHROPIC_API_KEY;
    if (!apiKey) return json({ saved: false, reason: 'No API key' });

    // Ask Claude Haiku to evaluate quality
    const evalPrompt = `You are evaluating a compliance scenario conversation from an internal cannabis company AI platform. Your job is to decide whether this conversation is worth saving to the Scenario Library for future reference.

SCENARIO TYPE: ${scenario.label}
CATEGORY: ${scenario.category}
SITUATION: ${scenario.situation || 'not specified'}
CONTEXT FLAGS: ${(scenario.context || []).join(', ') || 'none'}
STATE: ${scenario.state || 'not specified'}
LICENSE TYPE: ${scenario.license || 'not specified'}

CONVERSATION TRANSCRIPT:
${transcript.slice(0, 6000)}

Evaluate this conversation on these dimensions and return ONLY valid JSON:

{
  "qualityScore": <integer 0-100>,
  "shouldSave": <true if qualityScore >= 65, else false>,
  "summary": "<2 sentence summary of the scenario and key compliance takeaways, max 180 chars>",
  "strengths": ["<what made this conversation valuable>"],
  "reason": "<one sentence explaining the score>"
}

Score high (80-100) if: Kait gave thorough multi-angle analysis, cited specific rules, identified gray areas, the user was engaged (asked follow-ups, showed appreciation, or explored the topic deeply).
Score medium (65-79) if: decent compliance coverage but missing some dimensions or limited engagement.
Score low (<65) if: superficial, no rule citations, user disengaged quickly, or the conversation was off-topic.

Return ONLY the JSON object. No markdown, no explanation.`;

    const evalResp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 400,
        messages: [{ role: 'user', content: evalPrompt }],
      }),
    });

    if (!evalResp.ok) return json({ saved: false, reason: 'Eval API error' });

    const evalData = await evalResp.json();
    const rawText = evalData.content?.[0]?.text || '{}';

    let evaluation;
    try {
      evaluation = JSON.parse(rawText.replace(/```json|```/g, '').trim());
    } catch {
      return json({ saved: false, reason: 'Could not parse evaluation' });
    }

    if (!evaluation.shouldSave) {
      return json({ saved: false, qualityScore: evaluation.qualityScore, reason: evaluation.reason });
    }

    // Save the scenario record
    const record = {
      scenarioId,
      dept,
      scenario,
      qualityScore: evaluation.qualityScore,
      summary: evaluation.summary || '',
      strengths: evaluation.strengths || [],
      savedAt: new Date().toISOString(),
      transcriptLength: transcript.length,
    };

    const ttl = 365 * 24 * 60 * 60; // 1 year
    await env.CACI_KV.put(`scenario:${dept}:${scenarioId}`, JSON.stringify(record), { expirationTtl: ttl });

    // Update index
    const indexKey = `scenario:index:${dept}`;
    const index = await env.CACI_KV.get(indexKey, 'json') || [];
    // Remove if already exists (re-save), then prepend
    const filtered = index.filter(i => i.scenarioId !== scenarioId);
    filtered.unshift({ scenarioId, label: scenario.label, category: scenario.category, qualityScore: evaluation.qualityScore, savedAt: record.savedAt });
    // Keep last 200 in index
    const trimmed = filtered.slice(0, 200);
    await env.CACI_KV.put(indexKey, JSON.stringify(trimmed), { expirationTtl: ttl });

    return json({ saved: true, qualityScore: evaluation.qualityScore, summary: evaluation.summary, totalCount: trimmed.length });

  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ── POST /scenario/save ─────────────────────────────────────
// Manual save (bypass evaluation — admin/direct use)
async function handleScenarioSave(request, env) {
  try {
    const { dept, scenarioId, scenario, qualityScore = 75, summary = '' } = await request.json();
    if (!dept || !scenarioId || !scenario) return json({ error: 'Missing fields' }, 400);

    const record = {
      scenarioId, dept, scenario,
      qualityScore, summary,
      savedAt: new Date().toISOString(),
      manualSave: true,
    };

    const ttl = 365 * 24 * 60 * 60;
    await env.CACI_KV.put(`scenario:${dept}:${scenarioId}`, JSON.stringify(record), { expirationTtl: ttl });

    const indexKey = `scenario:index:${dept}`;
    const index = await env.CACI_KV.get(indexKey, 'json') || [];
    const filtered = index.filter(i => i.scenarioId !== scenarioId);
    filtered.unshift({ scenarioId, label: scenario.label, category: scenario.category, qualityScore, savedAt: record.savedAt });
    await env.CACI_KV.put(indexKey, JSON.stringify(filtered.slice(0, 200)), { expirationTtl: ttl });

    return json({ ok: true, totalCount: filtered.length + 1 });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ── GET /scenario/list?dept=xxx ─────────────────────────────
// Returns all saved scenarios for a department, full records.
async function handleScenarioList(url, env) {
  try {
    const dept = url.searchParams.get('dept') || 'compliance';
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);

    // Get index first (lightweight)
    const index = await env.CACI_KV.get(`scenario:index:${dept}`, 'json') || [];
    const slice = index.slice(0, limit);

    // Hydrate full records
    const records = await Promise.all(
      slice.map(i => env.CACI_KV.get(`scenario:${dept}:${i.scenarioId}`, 'json').catch(() => null))
    );

    const valid = records.filter(Boolean).sort((a, b) => (b.qualityScore || 0) - (a.qualityScore || 0));

    return json({ ok: true, items: valid, total: index.length });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}
