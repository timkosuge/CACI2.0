CACI Intelligence Platform
Internal Operational Intelligence System for Jushi Holdings
Version 2.0 — April 2026

CACI (pronounced “Cassie”) is our internal AI intelligence platform, built specifically for Jushi. She’s not a general-purpose chatbot. She’s a document-grounded intelligence layer that turns everything we upload — sales reports, shrink files, compliance materials, SOPs, financial summaries, and more — into something we can actually talk to in plain English.
Every answer she gives comes directly from documents the team has uploaded. She doesn’t guess, doesn’t pull from the internet, and she will clearly tell you when she doesn’t have enough information. What makes her feel different is how well she understands our world: 280E, METRC, shrink, state-by-state regulatory complexity, the difference between medical and adult-use markets, and the mix of people who actually run the business day-to-day. She’s sharp when needed, patient when you’re figuring something out, and has just enough personality to feel like a colleague rather than a tool.

How you use her

Open CACI, choose your department, and start asking questions. You can keep the scope broad (“across all files”) or narrow it down using the scope bar at the top — to a specific collection or even a single document. She handles natural time expressions (“last quarter”, “March data”, “Q1 vs last year”) and understands when you’re asking for comparisons, totals, trends, or explanations.
There’s also a special Compliance Scan mode for high-stakes regulatory or legal questions. When activated, she reads every single document in the collection without skipping anything. It’s slower and uses more tokens, but it’s there when missing even one detail could matter. The mode automatically turns off after your query.
You can speak to her using the microphone button, and she can read her responses aloud if you enable auto-speak. Everything stays inside our Cloudflare environment except for the AI model calls themselves.
The Document System
When you upload a file, a lot happens behind the scenes:

All text extraction runs client-side in your browser (PDF.js for PDFs, Mammoth for Word, SheetJS for Excel/CSV). Nothing gets sent to a third-party extraction service.
For spreadsheets and CSVs, CACI doesn’t just pull the text — she automatically calculates statistical summaries (sums, averages, min/max per column) and captures important category values. This creates a “FILE SUMMARY” block at the top of each tabular document so she can answer aggregate questions quickly and accurately without scanning every row.
Documents are organized into Collections. After upload, CACI’s AI (Claude Haiku) reviews each file and suggests the best report name, category, period, and whether it should be treated as a Context Document. You review these suggestions in the AI Classification panel and apply them with one click (or “Apply All”).

Context Documents are special. These are things like regulations, SOPs, policies, and glossaries. Once marked as context, they’re loaded in full for every question in that collection so CACI always has the foundational rules and background knowledge available before answering.
For large regulatory PDFs, we have a PDF Splitter tool. It automatically detects Article, Chapter, Section, or § breaks and splits the document into separate, precisely named files. This dramatically improves retrieval accuracy — instead of one giant 300-page blob, CACI can pull exactly the right section when you ask about it.

Technical Architecture & Implementation Details
CACI is built with a deliberately minimal, auditable, and edge-native architecture on Cloudflare:
Core Components

Frontend: Single index.html file containing all HTML, CSS, and vanilla JavaScript. No build step, no frameworks, extremely fast load times.
Backend: Single Cloudflare Worker (worker.js) that exposes all API routes (/upload, /chat, /ai-classify, /collection-analyze, /files, /collections, etc.).
Storage:
R2: Raw file binaries stored at paths like dept/collection/[uuid]/filename.ext
KV: All structured data with carefully designed keys:
file:[id] — Complete record including all text chunks, full metadata, and pre-computed stats (expires after 1 year)
index:[dept] — Lightweight department-level index (capped at 500 most recent records, no chunks)
col:[dept]:[collection] — Per-collection index of file records
colreg:[dept] — Collection registry containing name, category, description, and file count
ctx:[dept]:[collection] — Separate index for Context Documents
library:map:[dept] — Cached intelligent profile of the entire library (rebuilt on changes, cached for 2 minutes)

Text Extraction & Chunking (Client-Side)

PDFs: PDF.js (Mozilla)
Word (.docx): Mammoth
Excel/CSV: SheetJS with two parallel pipelines:
computeTabularStats(): Analyzes every column. Numeric columns (≥70% numeric) get sum/avg/min/max/count. Low-cardinality text columns (<30 unique values) capture full unique value sets.
rowsToChunkedText(): Generates self-contained chunks starting with Columns: ... followed by numbered rows. Chunk size adapts to column width (smaller batches for wide sheets). Each spreadsheet response always begins with a FILE SUMMARY block.

Prose documents use sentence/paragraph boundary-aware chunking with 150-character overlap between chunks. Tabular data preserves natural newlines and uses --- separators.
AI Classification & Collection Intelligence

Per-file classification uses Claude Haiku via /ai-classify. It receives the first ~2,500 characters (or FILE SUMMARY + first 25 rows for spreadsheets) and returns structured JSON: reportName, category, period, reportType, suggestedCollection, isContextDoc, confidence.
Low-confidence results are discarded. High/medium results appear in the AI Review Panel for manual approval (individual or “Apply All”).
After a batch upload, /collection-analyze sends a manifest of all files (names, periods, row counts, columns) to Claude. It generates a 1-2 sentence description, primary category, and ultra-short summary, which is then PATCHed to the collection record via /collections/describe.

Retrieval Engine (“Reason Then Retrieve”)
Every chat request follows a multi-stage pipeline:

Intent Analysis (analyzeQueryIntent): Regex-based classification into comparative, aggregate, causal, or filtered queries. Influences chunk budget, collection count, and scoring weights.
Retrieval Planning: For non-trivial queries, a lightweight LLM call generates a 2-3 sentence plan identifying the most relevant collections and expected information types. This plan is injected into the final system prompt.
Collection Routing & Context Building:
buildContextMultiCollection for broad scope: scores collections by keyword overlap, category, description, and intent, then pulls top N.
buildContextTwoPass for single-collection scope: first scores files on index records (date range +30, year match weighted by recency, month +8, etc.), then loads full records only for top candidates.
Single-file scope bypasses collection scoring entirely.

File Scoring: Combines date-range matching (+30), year matching (+10–28 based on recency), month/quarter matching (+8), keyword presence (+2), category column value matches (+5), and recency boost (up to +3). PDF files get slight preference over XLSX.
Chunk Selection: Dynamic budget (typically 18–32 chunks) distributed across selected files. Every file guaranteed at least one chunk. FILE SUMMARY chunks are always included. Chunks are then scored with scoreChunk (keyword density, normalized by length) and expanded with domain synonyms.
Reranking (rerankChunks): Final pass adds bonuses for:
Numeric density (aggregate queries)
Direct-answer signals (number near keyword)
Multi-year presence (comparative queries)
Heavy penalty for near-duplicate chunks

Context Assembly: System prompt is built in strict order:
Library map (with relationships and gaps)
Retrieval plan
Pre-computed stats summaries (explicitly labeled as authoritative for aggregates)
Context documents (loaded in full)
Ranked document chunks (with clear labeling: [collection / filename])

The final LLM call includes the last 20 turns of conversation history and uses the user-selected model (Claude Sonnet 4 default).
Library Map (buildLibraryMap)
Rebuilt on changes and cached for 2 minutes. Profiles each collection (file count, context docs, years, states, file types, total rows). Automatically detects:

Cross-collection relationships (regulatory vs operational data sharing states/years)
Gaps (operational data exists for a state but no regulatory documents)

This map is injected into every system prompt so CACI begins with real situational awareness.
Voice Pipeline

STT: Browser Web Speech API (SpeechRecognition)
TTS: /tts endpoint proxies to xAI Grok TTS (voices: eve, ara, sal, leo, rex) or Cloudflare Deepgram Aura 2
Auto-speak: Configurable toggle, persists in localStorage

The entire system was designed to be fast, private, auditable, and cheap to run while delivering sophisticated, industry-aware retrieval and document intelligence.
