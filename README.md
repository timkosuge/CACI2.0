CACI Intelligence Platform
Internal Operational Intelligence System for Jushi Holdings
Version 2.0 — April 2026
Hey team,
CACI (pronounced “Cassie”) is our internal AI intelligence platform, built specifically for Jushi. She is not a general-purpose chatbot. She is a document-grounded intelligence layer that turns everything we upload — sales reports, shrink files, compliance materials, SOPs, financial summaries, and more — into something we can actually talk to in plain English.
Every answer she gives comes directly from documents the team has uploaded. She does not guess, does not pull from the internet, and she will clearly tell you when she does not have enough information. What makes her different is how well she understands our world: 280E, METRC, shrink, state-by-state regulatory complexity, the difference between medical and adult-use markets, and the mix of people who actually run the business day-to-day. She is sharp when needed, patient when you’re figuring something out, and has just enough personality to feel like a colleague rather than a tool.
How you use her
Open CACI, choose your department, and start asking questions. You can keep the scope broad (“across all files”) or narrow it down using the scope bar at the top — to a specific collection or even a single document. She handles natural time expressions (“last quarter”, “March data”, “Q1 vs last year”) and understands when you’re asking for comparisons, totals, trends, or explanations.
There is also a special Compliance Scan mode for high-stakes regulatory or legal questions. When activated, she reads every single document in the collection without skipping anything. It is slower and uses more tokens, but it is there when missing even one detail could matter. The mode automatically turns off after your query.
You can speak to her using the microphone button, and she can read her responses aloud if you enable auto-speak.
The Document System
When you upload a file, a lot happens behind the scenes to make the intelligence possible:

All text extraction runs client-side in your browser (PDF.js for PDFs, Mammoth for Word, SheetJS for Excel/CSV). Nothing gets sent to a third-party extraction service.
For spreadsheets and CSVs, CACI doesn’t just pull the text — she automatically calculates statistical summaries (sums, averages, min/max per column) and captures important category values. This creates a “FILE SUMMARY” block at the top of each tabular document so she can answer aggregate questions quickly and accurately without scanning every row.
Documents are organized into Collections. After upload, CACI’s AI (Claude Haiku) reviews each file and suggests the best report name, category, period, and whether it should be treated as a Context Document. You review these suggestions in the AI Classification panel and apply them with one click (or “Apply All”).

Context Documents are special. These are things like regulations, SOPs, policies, and glossaries. Once marked as context, they’re loaded in full for every question in that collection so CACI always has the foundational rules and background knowledge available before answering.
For large regulatory PDFs, we have a PDF Splitter tool. It automatically detects Article, Chapter, Section, or § breaks and splits the document into separate, precisely named files. This dramatically improves retrieval accuracy — instead of one giant 300-page blob, CACI can pull exactly the right section when you ask about it.

Technical Architecture & Intelligence Layer
CACI is not just a nice interface wrapped around generic RAG. The real power lies in a carefully engineered retrieval and context system designed to deliver accurate, grounded answers from messy real-world cannabis data.
Storage & Indexing

R2 stores the raw file binary exactly as uploaded.
KV uses a deliberate key schema for performance and isolation:
file:[id] — Complete record with all text chunks, full metadata, and pre-computed stats (1-year TTL).
index:[dept] — Lightweight rolling index of the most recent 500 files per department (no chunks stored here).
col:[dept]:[collection] — Per-collection index of file records.
colreg:[dept] — Collection registry with name, category, description, and metadata.
ctx:[dept]:[collection] — Dedicated index for Context Documents.
library:map:[dept] — Intelligent cache rebuilt on changes (2-minute TTL).


Tabular Intelligence
Spreadsheet and CSV files receive special treatment. After parsing with SheetJS:

computeTabularStats() analyzes every column. Numeric columns (≥70% numeric values) get full aggregates (sum, avg, min, max, count). Low-cardinality text columns (<30 unique values) capture the complete set of unique values.
These statistics become the leading “FILE SUMMARY” block. This allows CACI to answer aggregate questions directly from authoritative pre-computed numbers instead of guessing from partial chunks.

Chunking Strategy

Prose documents use sentence/paragraph-aware chunking with 150-character overlap to preserve context.
Tabular data preserves natural structure and uses --- separators between self-contained chunks.
Chunk size is configurable at upload (Standard 1500 chars, Fine 800, Ultra-fine 400). Smaller chunks are recommended for dense legal and regulatory text.

The Retrieval Brain (“Reason Then Retrieve”)
When you send a message, CACI runs a multi-stage pipeline before generating an answer:

Intent Analysis (analyzeQueryIntent) — Classifies the query as comparative, aggregate, causal, or filtered using pattern matching. This directly shapes retrieval strategy (more files for comparative queries, heavier stats usage for aggregate queries, etc.).
Retrieval Planning — For non-trivial queries, a lightweight LLM call generates a short plan identifying the most relevant collections and expected information types. This plan is injected into the final system prompt.
Library Map Awareness (buildLibraryMap) — CACI loads (or uses a cached) intelligent map of the entire department library. It profiles every collection (years, states, file types, total rows) and automatically detects:
Cross-collection relationships (e.g., which regulatory collections govern which operational data)
Notable gaps (e.g., operational data exists for a state but no regulatory documents have been uploaded)
This gives her persistent situational awareness instead of rediscovering the library on every query.
File Scoring — Files in relevant collections are scored using:
Date range match: +30 points
Year match: +10 to +28 points (heavier weighting for recent years)
Month/quarter match: +8 points
Keyword presence in filename: +2 per keyword
Category column value matches: +5 boost
Recency bonus: up to +3 points
Slight preference for PDFs over XLSX

Chunk Selection & Reranking — A dynamic chunk budget (typically 18–32 chunks) is allocated. Every selected file is guaranteed at least one chunk, and the FILE SUMMARY block is always included for tabular files. Chunks are then scored with scoreChunk (keyword density normalized by length) and go through a final reranking pass (rerankChunks) that adds bonuses for:
Numeric density (especially useful for aggregate queries)
Direct-answer signals (number appearing near a matched keyword)
Multi-year presence (for comparative questions)
Deduplication penalty for near-identical chunks

Context Assembly — The final system prompt is built in strict layered order:
Library map (with relationships and gaps)
Retrieval plan
Pre-computed statistical summaries (explicitly labeled as authoritative for aggregates)
Context documents (loaded in full)
Ranked document chunks (clearly labeled with collection and filename)


The model is told exactly how to use each layer, especially to prefer the pre-computed stats for totals and averages rather than scanning partial row chunks.
This design is what gives CACI her “smart brain.” She reasons about what to retrieve, why it matters, and how to combine it with background knowledge before answering. The result is significantly more accurate and grounded responses than typical RAG systems, especially when working with messy, time-sensitive, multi-state cannabis data.

The entire system was built in roughly three days using Claude as the primary coding partner. The architecture was kept intentionally simple, auditable, and cheap to run while delivering unusually sophisticated retrieval intelligence tailored to real cannabis operations data.
