CACI Intelligence Platform
Internal Operational Intelligence System for Jushi Holdings
Version 6.1 — April 2026


WHAT CACI IS

CACI (pronounced "Cassie") is Jushi's internal AI intelligence platform. She
is not a general-purpose chatbot. She is a document-grounded intelligence
layer that turns everything the team uploads — sales reports, shrink files,
compliance materials, SOPs, financial summaries, earnings transcripts, and
more — into something you can talk to in plain English.

Every answer she gives comes directly from documents the team has uploaded.
She does not guess, does not pull from the internet, and she will clearly tell
you when she does not have enough information.

What makes her different is how well she understands the Jushi world: 280E,
METRC, shrink, state-by-state regulatory complexity, the difference between
medical and adult-use markets, Virginia and Pennsylvania strategic context,
capital allocation philosophy, brand portfolio, leadership team, and the mix
of people who actually run the business day-to-day.

She is sharp when needed, patient when you are figuring something out, and has
just enough personality to feel like a colleague rather than a tool.


DEPARTMENTS

CACI is multi-tenant by department. Every collection, document library,
scoring weight, and context doc is isolated per department. Departments:

  Retail · Compliance · Commercial · Human Resources · Finance
  Operations · Technology · Global

The Global department is shared across all departments — files uploaded there
are accessible company-wide.



HOW TO USE CACI

Open CACI, choose your department, and start asking questions. You can keep
the scope broad ("across all files") or narrow it using the scope bar at the
top — to a specific collection or even a single document.

She handles natural time expressions ("last quarter", "March data", "Q1 vs
last year") and understands comparative, aggregate, causal, and filtered
queries.

REPORT GENERATION: Asking for a report in chat automatically triggers the full
structured report pipeline. Phrases like "generate a report," "executive
summary," "quarterly analysis," "full breakdown," or "summarize everything"
will produce a structured 7-section Markdown report with executive summary,
key findings, detailed analysis, period comparisons, state/store breakdown,
recommendations, and data sources. The report comes back in the chat window
and can be saved, copied, or downloaded.

VOICE: Use the microphone button to speak your questions. Enable auto-speak in
AI Settings to have CACI read her responses aloud. Voice length is adjustable
(Short ~10s / Medium ~20s / Standard ~40s / Long ~60s / Full).

SAVED RESPONSES: Use the Save button on any AI response to archive it to the
Responses tab. Saved responses are searchable by keyword and filterable by
auto-generated topic chips derived from your actual response content.


AI MODELS

Four models are available and selectable from the Intelligence Engine modal:

  Claude Sonnet 4 (Anthropic) — Primary model. Best reasoning and analysis.
  Grok 3 Mini Fast (xAI)      — Fast and sharp. Also powers TTS voice.
  Cloudflare AI / Llama 4     — Fast and free. No API key required.
  Ollama (Local)              — Fully private, runs locally.

Lightweight internal tasks (query rewriting, AI classification, answer
verification, feedback analysis) always use Claude Haiku for speed and cost
efficiency regardless of the selected model.


THE DOCUMENT SYSTEM

Supported file types: PDF, DOCX, CSV, XLSX, XLS, TXT, MD

All text extraction runs client-side in the browser — PDF.js for PDFs,
Mammoth for Word, SheetJS for Excel/CSV. Nothing goes to a third-party
extraction service.

TABULAR INTELLIGENCE
For spreadsheets and CSVs, CACI automatically computes statistical summaries
at upload time: sums, averages, min/max per numeric column, and complete
categorical value inventories for low-cardinality columns. These become a
"FILE SUMMARY" block at the top of every tabular document, allowing CACI to
answer aggregate questions directly from authoritative pre-computed numbers
rather than scanning partial row chunks.

CHUNKING STRATEGY
Prose documents use sentence/paragraph-aware chunking with 150-character
overlap to preserve context across boundaries. Tabular data preserves natural
row structure with --- separators between self-contained chunks.

AI AUTO-CLASSIFICATION (optional toggle)
When enabled, Claude Haiku reviews each uploaded file and suggests: report
name, category, period, report type, suggested collection, and whether the
file should be treated as a Context Document. Suggestions appear in the AI
Classification panel for one-click review and apply.

COLLECTIONS
Documents are organized into Collections per department. Collections can be
given names, categories, and descriptions. CACI's AI can auto-generate a
collection description from the file manifest.

CONTEXT DOCUMENTS
Special documents — regulations, SOPs, policies, glossaries — can be marked
as Context Documents. Once marked, they are loaded in full for every query in
that collection, giving CACI permanent foundational knowledge before answering.

INTERNAL REFERENCE COLLECTION
Context documents in the "Internal Reference" collection are injected into
every single query across all departments and scopes, regardless of what
collection a user is working in. This is where company-wide background
knowledge lives — Jushi strategic context, brand references, leadership info,
earnings intelligence, etc.

PDF SPLITTER
Large regulatory PDFs can be split at Article, Chapter, Section, or §
boundaries into individually named files. This dramatically improves retrieval
accuracy for dense legal/regulatory documents.

DUPLICATE DETECTION
Before upload, CACI checks for files with matching names already in the
collection and warns the user.

METADATA EDITING
After upload, any file's report name, category, period, and report type can be
edited directly in the Documents panel.

FILTER DROPDOWNS
The Documents panel filter bar auto-populates with the actual categories,
periods, and states from uploaded files — no manual configuration required.


TECHNICAL ARCHITECTURE & INTELLIGENCE LAYER

STORAGE
  Cloudflare R2    — Raw file binary storage
  Cloudflare KV    — All intelligence data, structured by key schema:
    file:[id]                    Complete record: chunks, metadata, stats (1yr TTL)
    index:[dept]                 Lightweight rolling index, last 500 files (no chunks)
    col:[dept]:[collection]      Per-collection file index
    colreg:[dept]                Collection registry with name, category, description
    ctx:[dept]:[collection]      Context document index
    library:map:[dept]           Intelligent library map cache (2-min TTL)
    config:scoring-weights:[dept] Per-dept tunable retrieval scoring weights
    feedback:[dept]:[responseId] Individual feedback entries
    feedback:summary:[dept]      Rolling feedback counters
    config:tune-pending:[dept]   Pending AI-suggested weight adjustments

RETRIEVAL PIPELINE ("Reason Then Retrieve")
Every chat query runs a multi-stage pipeline before generating a response:

1. REPORT DETECTION — If the query is asking for a report, executive summary,
   or structured analysis, it routes directly to the report generation system
   with a 7-section structured format and 8,000 token budget.

2. PERSONAL QUERY DETECTION — Questions about CACI herself (personality,
   identity, opinions) bypass documents entirely and answer from the system
   prompt.

3. INTENT ANALYSIS (analyzeQueryIntent) — Classifies the query as comparative,
   aggregate, causal, or filtered. Directly shapes retrieval strategy.

4. QUERY REWRITING — For complex queries, Claude Haiku generates 2-3
   retrieval variants using synonyms, explicit date expansion, and domain
   terminology. These blend with the original query for retrieval scoring.

5. RETRIEVAL PLANNING — A lightweight LLM call identifies which collections
   are most relevant and why. The plan is injected into the final system
   prompt so CACI knows what she's looking for before she starts answering.

6. LIBRARY MAP AWARENESS (buildLibraryMap) — CACI loads a cached intelligent
   map of the entire department library profiling every collection (years,
   states, file types, row counts) and detecting cross-collection relationships
   and notable gaps. Rebuilt on changes, cached for 2 minutes.

7. FILE SCORING — Files are scored using tunable weights:
     Date range match:     +22 pts (highest priority)
     Year match:           +8 to +26 pts (recency-weighted)
     Month/quarter match:  +7 pts
     Keyword in filename:  +6 pts per keyword
     Category col match:   +8 pts
     Recency bonus:        up to +3 pts
     Annual match:         +6 pts
     XLSX slight penalty:  -1 pt

8. CHUNK SELECTION & RERANKING — A dynamic chunk budget is allocated. Every
   selected file is guaranteed at least one chunk. FILE SUMMARY blocks are
   always included for tabular files. Chunks are scored by keyword density
   (normalized by length) then go through a reranking pass adding bonuses for:
     Numeric density (aggregate queries)
     Direct-answer signals (number near a matched keyword)
     Multi-year presence (comparative queries)
     Deduplication penalty for near-identical chunks

9. ANSWER VERIFICATION — For numeric aggregate/comparative responses, Claude
   Haiku checks the answer against pre-computed stats and appends a correction
   note if a specific number contradicts the authoritative data.

10. CONTEXT ASSEMBLY — Final system prompt is built in strict layered order:
      Library map (relationships and gaps)
      Retrieval plan
      Pre-computed statistical summaries (labeled authoritative for aggregates)
      Internal Reference context docs (always present)
      Active collection context docs (if scoped to a collection)
      Ranked document chunks (labeled with collection and filename)

DOMAIN INTELLIGENCE
Built-in synonym expansion covers: state abbreviations (PA/IL/NV/VA/NJ/OH/MA),
cannabis product categories, business metrics (basket size, sell-through,
shrinkage, conversion rate, run rate, etc.), and retail/wholesale terminology.
Relative date parsing handles: last month, this quarter, YTD, last N months,
last N quarters, and more.


FEEDBACK & SELF-IMPROVEMENT

Every AI response has three feedback signals:
  Sharp      — Retrieval was correct and the answer was good
  Missed     — Wrong documents retrieved, answer missed the mark
  Incomplete — Right direction but not enough depth

Feedback is stored per department with rolling summary counters. When enough
negative signals accumulate, Claude Haiku analyzes patterns and suggests
specific scoring weight adjustments. An admin can review and approve or reject
each suggestion individually. All applied changes are logged with a full audit
trail.

13 TUNABLE SCORING WEIGHTS (adjustable per department in AI Settings):
  yearMatchBase, yearRecencyMultiplier, monthMatchBonus, rangeMatchBonus,
  quarterMatchBonus, annualBonus, keywordFilenameBonus, recencyMaxBonus,
  categoryColBonus, fileSummaryBonus, rerankDirectAnswerBonus,
  rerankNumericBonus, rerankComparativeBonus, xlsxPenalty


VOICE (TTS)

Primary provider: xAI Grok TTS
  Voices: Eve (expressive), Ara (warm), Sal (clear), Leo (deep), Rex (authoritative)

Fallback provider: Cloudflare Deepgram Aura 2

Response length control (user-adjustable in AI Settings):
  Short ~10s / Medium ~20s / Standard ~40s / Long ~60s / Full (no limit)

Smart sentence-end truncation prevents mid-sentence cutoffs. Hard ceiling of
8,000 characters on the worker side regardless of setting.


INTEGRATIONS & CONNECTORS (configured, credential storage active)

MICROSOFT 365 INTEGRATIONS
  Excel, Word, Teams, Power BI

QUICKBASE
  Asset-based data integration

CANNABIS PLATFORM CONNECTORS (live API verification on save)
  Dutchie       — Retail POS
  METRC         — Seed-to-sale tracking (per-state keys)
  iHeartJane    — Menu and retail
  LeafTrade     — Wholesale B2B
  MJ Freeway    — Seed-to-sale ERP

Note: Credential storage and API verification are active for all connectors.
Live data injection into chat queries is the next phase of development.


CHAT HISTORY & SESSION MANAGEMENT

Chat history persists across sessions per department using localStorage. On
refresh, previous conversations are restored with a subtle "Session restored"
indicator. The greeting fires fresh on every load — it is not saved to history.
"New Conversation" clears history and resets scope. Changing departments clears
history for the previous department.


DEPLOYMENT

Frontend: Cloudflare Pages (single HTML file, ~404 KB, ~10,400 lines)
Backend:  Cloudflare Workers v6.1 (~141 KB, ~2,765 lines, 13.8% of free tier)
Storage:  Cloudflare KV + R2
Auth:     Session token via SESSION_SECRET environment variable

Worker size headroom: ~86% remaining on free tier (6.9x growth capacity).
