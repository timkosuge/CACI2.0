


DOCUMENT UPLOAD/CLASSIFICATION/STORAGE/RETRIEVAL

Document Storage
Every document uploaded to CACI gets stored in two places simultaneously on Cloudflare's infrastructure.
R2 (object storage) holds the raw file binary — the actual PDF, Excel, Word doc, or CSV exactly as you uploaded it. Think of this as a filing cabinet. Files sit there at a path like compliance/Illinois Cannabis Regulations/[id]/filename.pdf. R2 is cheap, durable, and essentially unlimited.
KV (key-value store) holds everything the AI actually uses — the extracted text, metadata, and computed statistics. Four separate KV entries are written for every file:

file:[id] — the complete file record including every text chunk, metadata (name, category, period, report type, states), and pre-computed statistics
index:[dept] — a department-level index of all files (up to 500 records), without chunks, used for fast listing and browsing
col:[dept]:[collection] — same lightweight record stored in the collection's own index
colreg:[dept] — the collection registry, which tracks what collections exist and their descriptions


Text Extraction
This all happens in the browser before anything is sent to the server. No file content ever goes to a third-party service for extraction — it's processed entirely client-side.
PDFs use PDF.js, a Mozilla library loaded from CDN. It reads each page and concatenates the text content items. The result is raw text — no formatting preserved, but all the words are there.
Word documents use Mammoth, which extracts raw text from the .docx XML structure. Same result — clean prose text.
Excel and CSV files go through the most sophisticated extraction. SheetJS reads the file, converts each sheet to row objects, then two things happen in parallel:
First, computeTabularStats analyzes every column. If a column is at least 70% numeric values, it computes sum, average, min, max, and count. If it's a low-cardinality text column (fewer than 30 unique values), it captures all the unique values. This is what later becomes the FILE SUMMARY block — a pre-computed statistical overview that the AI can use to answer aggregate questions without having to scan every row.
Second, rowsToChunkedText converts rows into self-contained text chunks. Each chunk starts with Columns: col1 | col2 | col3 so it's interpretable in isolation, then lists the rows with their values. The chunk size adapts to column count — wide sheets get smaller batches so chunks don't exceed ~2,000 characters. Chunks are separated by --- markers.
The final text for a spreadsheet always starts with the FILE SUMMARY block (total rows, column names, all numeric stats, all category values), then the row chunks follow. This means the AI always sees the forest before the trees.
For prose documents (PDF, Word), a 150-character overlap is preserved between chunks so context isn't lost at boundaries — the last 150 characters of one chunk appear at the start of the next.

The Upload Pipeline
After extraction completes in the browser, a FormData POST goes to the worker's /upload endpoint carrying: the raw file binary, the extracted text, the computed stats as JSON, and all the metadata you filled in (or that was pre-filled by AI classify).
The worker receives this and does five things:

Detects whether the text is tabular or prose and cleans whitespace accordingly — tabular text preserves newlines, prose gets collapsed
Chunks the clean text using the specified chunk size (Standard 1500 chars, Fine 800, Ultra-fine 400) with sentence/paragraph boundary awareness
Stores the raw file to R2
Writes the complete file record (including all chunks) to KV at file:[id]
Updates the three index entries (department, collection, collection registry)
Invalidates the library map cache so CACI's next session sees the new file


AI Classification
Two AI processes run after upload, both non-blocking — they don't slow down the upload itself.
Per-file classification fires immediately after each file stores successfully. The system takes the first 2,500 characters of extracted text (or for spreadsheets, the FILE SUMMARY plus the first 25 rows) and sends it to Claude Haiku via the /ai-classify worker endpoint. Haiku reads this sample and returns a JSON object: reportName, category, period, reportType, suggestedCollection, isContextDoc, and confidence. Low confidence results are discarded silently. High and medium confidence results appear in the AI Classification review panel — a panel that slides open on the right side of the Documents view showing each file's suggested metadata. You can apply suggestions individually or hit Apply All. Nothing changes without your approval.
Collection analysis runs once after the full batch finishes uploading, if AI classification is enabled. It sends a manifest of every file in the collection (names, periods, row counts, column names) to /collection-analyze, which asks the LLM to write a 1-2 sentence description of the collection, assign it a category, and generate a short summary. This description gets stored on the collection record and used in the library map.

The Library Map
This is a persistent understanding of the entire library that gets built and cached in KV at library:map:[dept]. It's rebuilt whenever files or collections change, and cached for 30 minutes between rebuilds.
The map profiles every collection: file count, context doc count, which years and states appear in the filenames, what file types are present, total row counts, and a sample of the actual file names. It then does two things automatically:
It detects cross-collection relationships — if a Legal/Compliance collection and a data collection share state coverage, it notes that the regulations govern the operational data. If two data collections share states and years, it notes they can be compared.
It detects gaps — if operational data exists for a state but no regulatory documents have been uploaded for that state, it flags it.
This map gets injected into CACI's system prompt at the start of every chat session. She doesn't rediscover the library on every query — she already knows what's there.

Query Processing — Reason Then Retrieve
When you send a message, the worker runs this sequence:
Step 1 — Intent analysis. The query is analyzed with regex patterns to classify what kind of question it is: comparative (vs, trend, year-over-year), aggregate (total, average, highest), filtered (mentions a state or location), or causal (why, what caused, driving). This shapes how retrieval will work — comparative queries get more files pulled, aggregate queries prioritize the stats block, causal queries look for correlated changes.
Step 2 — Retrieval planning. For non-trivial queries, before touching any documents, a fast LLM call generates a retrieval plan: which collections are most relevant and what kind of information is expected there. This plan gets injected into the system prompt alongside the retrieved content, so CACI knows why she's pulling what she's pulling.
Step 3 — Collection routing. If you're scoped to all documents, buildContextMultiCollection scores every collection by keyword relevance and pulls the top 3-4. If you're scoped to a specific collection, buildContextTwoPass runs on that collection. If you're on a single file, just that file's chunks are used.
Step 4 — File scoring (inside buildContextTwoPass). Every file in the selected collection gets scored against the query:

Date range match: +30 points if the file falls within an explicit date range in the query
Year match: +10-28 points depending on recency of the year mentioned
Month match: +8 points
Annual/full-year boost: +8 points for annual reports when the query asks for annual data
Quarter match: +8 points
Keyword match in filename: +2 per keyword
Category column boost: +5 if a known category value (like "Illinois" or "Recreational") matches a keyword
Recency boost: up to +3 for recently uploaded files
PDF preferred slightly over XLSX

Files are ranked by score. Up to 12 files are selected if there's a strong date signal, otherwise top 3 plus a spread of 7 more for broad coverage.
Step 5 — Chunk selection. For each selected file, chunks are scored against the query using scoreChunk — keyword proximity, numeric density for aggregate queries, multi-year presence for comparative queries. A total chunk budget is allocated (18-22 chunks depending on how many files are loaded), distributed across files with high-scoring files getting more chunks. Every file is guaranteed at least one chunk, and the FILE SUMMARY chunk from each file is always included regardless of score — so the AI always sees the statistical overview.
Step 6 — Reranking. The selected chunks go through rerankChunks for a second pass — bonus points for chunks where a number appears near a keyword (direct answer signal), numeric density bonus for aggregate queries, multi-year presence bonus for comparative queries, and a -20 penalty for near-duplicate chunks.
Step 7 — Context assembly. The system prompt is built in layers, in order: the library map, the retrieval plan, pre-computed stats summaries (labeled as authoritative for aggregate questions), context documents (regulatory/policy reference material attached to the collection), and finally the ranked document chunks. The model is told explicitly that the stats summaries are pre-computed from the full dataset and should be preferred for aggregate answers over scanning row chunks.
Step 8 — LLM call. The assembled system prompt plus the conversation history (last 20 turns) goes to whichever model is selected. The response comes back and gets rendered through the markdown renderer into tables, headers, bullets, and formatted text.

Context Documents
A special category of file that behaves differently from regular documents. Instead of being retrieved by the scoring system, context documents are loaded in full on every query to that collection — they're injected as background knowledge rather than searched. Regulatory documents, SOPs, policies, and reference material work best as context docs. The AI classify system automatically flags these and suggests converting them.

Voice
TTS is handled by two providers selectable in Config. xAI Grok (eve, ara, sal, leo, rex voices) is the primary and highest quality — CACI's text goes to the /tts worker endpoint which proxies to the xAI API and returns an audio blob that plays in the browser. Cloudflare Deepgram Aura 2 is the fallback. Voice input uses the browser's Web Speech API to transcribe speech to text before sending it to the chat. Auto-speak mode plays every AI response automatically.
