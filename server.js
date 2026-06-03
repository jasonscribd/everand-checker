const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const { Parser: Json2CsvParser } = require('json2csv');
const fetch = require('node-fetch');
const { Readable } = require('stream');
const path = require('path');

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(express.static(path.join(__dirname, 'public')));

// Store results per session (simple in-memory store keyed by upload id)
const sessions = new Map();

// Parse CSV buffer into array of {title, author, originalRow}, plus headers list
function parseCSV(buffer) {
  return new Promise((resolve, reject) => {
    const rows = [];
    let headers = null;
    const stream = Readable.from(buffer.toString());
    stream
      .pipe(csv())
      .on('headers', (hdrs) => { headers = hdrs; })
      .on('data', (row) => {
        // Normalize column names — handle various casings/spacings
        const normalized = {};
        for (const key of Object.keys(row)) {
          normalized[key.trim().toLowerCase()] = row[key].trim();
        }
        const title = normalized['title'] || '';
        let author = normalized['author'] || '';
        // Handle JSON array format like ["Author Name"] or ["Author1","Author2"]
        if (author.startsWith('[')) {
          try {
            const parsed = JSON.parse(author.replace(/\u201C|\u201D/g, '"'));
            author = Array.isArray(parsed) ? parsed.join(', ') : author;
          } catch {
            author = author.replace(/[\[\]"]+/g, '').trim();
          }
        }
        if (title) rows.push({ title, author, originalRow: row });
      })
      .on('end', () => resolve({ rows, headers: headers || [] }))
      .on('error', reject);
  });
}

const EVERAND_COOKIE = (process.env.EVERAND_COOKIE || '').replace(/[\r\n]+/g, ' ').trim();

// ---- Title/author matching helpers ----
// Catalog titles differ from how people write them: "&" vs "and", appended
// subtitles (": A Novel", ", Book 1"), curly apostrophes, reordered words.
// A naive substring check misses all of these, which is the main reason real
// titles came back as "none".
const MATCH_STOP = new Set(['the','a','an','of','and','to','in','is','for','on','with','at','by','novel','book','unabridged','audiobook']);
function normStr(s) {
  return (s || '').toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[‘’']/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ').trim();
}
function tokenize(s) {
  return normStr(s).split(' ').filter((w) => w && w.length > 1 && !MATCH_STOP.has(w));
}
// Knock-off products that reuse a book's title: third-party summaries,
// workbooks, study guides, etc. Reject these even though their title contains
// all the real title's words.
const IMPOSTER_RE = /\b(summary|workbook|study guide|analysis|companion|conversation starters|key takeaways|guide to)\b/i;
function matchScore(qTitle, qAuthor, doc) {
  const rawDocTitle = (doc.title || '').toLowerCase();
  const rawQueryTitle = (qTitle || '').toLowerCase();
  if (IMPOSTER_RE.test(rawDocTitle) && !IMPOSTER_RE.test(rawQueryTitle)) return 0;

  const qt = tokenize(qTitle);
  const dt = new Set(tokenize(doc.title));
  if (qt.length === 0 || dt.size === 0) return 0;
  const present = qt.filter((w) => dt.has(w)).length;
  const titleCoverage = present / qt.length;
  if (titleCoverage < 0.7) return 0;

  let authorScore = 0.5;
  const qa = tokenize(qAuthor);
  if (qa.length) {
    const da = new Set(tokenize((doc.author && doc.author.name) || ''));
    const last = qa[qa.length - 1];
    if (da.has(last)) authorScore = 1;
    else if (qa.some((w) => da.has(w))) authorScore = 0.8;
    else authorScore = 0;
  }
  if (authorScore === 0) return 0;
  return titleCoverage * 2 + authorScore;
}
function pickBest(qTitle, qAuthor, docs) {
  let best = null;
  let bestScore = 0;
  for (const doc of docs) {
    const s = matchScore(qTitle, qAuthor, doc);
    if (s > bestScore) { best = doc; bestScore = s; }
  }
  return best;
}

// fetch JSON with a timeout + one retry so a stalled request can't hang the run.
async function fetchEverandJSON(url, headers) {
  for (let attempt = 0; attempt <= 1; attempt++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 12000);
    try {
      const res = await fetch(url, { headers, signal: ctrl.signal });
      clearTimeout(timer);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const ct = res.headers.get('content-type') || '';
      const text = await res.text();
      if (!ct.includes('json') && text.trim()[0] !== '{') {
        throw new Error('non-JSON response (bot challenge / cookie missing or expired)');
      }
      return JSON.parse(text);
    } catch (err) {
      clearTimeout(timer);
      if (attempt === 1) throw err;
      await sleep(800);
    }
  }
}

// Search Everand's own API for a title+author match
async function searchEverand(title, author) {
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.everand.com/search',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'X-Requested-With': 'XMLHttpRequest',
  };
  if (EVERAND_COOKIE) headers['Cookie'] = EVERAND_COOKIE;

  const abResult = (doc) => ({
    link: doc.book_preview_url || `https://www.everand.com/audiobook/${doc.id}`,
    docId: String(doc.id), format: 'audiobook',
    title: doc.title || null, author: (doc.author && doc.author.name) || null,
  });
  const ebResult = (doc) => ({
    link: doc.book_preview_url || `https://www.everand.com/book/${doc.id}`,
    docId: String(doc.id), format: 'ebook',
    title: doc.title || null, author: (doc.author && doc.author.name) || null,
  });

  // Search by TITLE first. Adding the author to the query string hurts ranking
  // on titles where the author name pulls in summaries/knock-offs (e.g. the
  // real "Everything Is Tuberculosis" gets buried under "Summary of John
  // Green's..."). Fall back to title+author only if title-only finds nothing.
  const queries = author ? [title, `${title} ${author}`.trim()] : [title];

  try {
    for (const q of queries) {
      const apiUrl = `https://www.everand.com/search/query?query=${encodeURIComponent(q)}`;

      const data = await fetchEverandJSON(apiUrl, headers);
      const audiobooks = data.results?.audiobooks?.content?.documents || [];
      const abMatch = pickBest(title, author, audiobooks);
      if (abMatch) return abResult(abMatch);

      const booksData = await fetchEverandJSON(apiUrl + '&content_type=books', headers);
      const books = booksData.results?.books?.content?.documents || [];
      const ebMatch = pickBest(title, author, books);
      if (ebMatch) return ebResult(ebMatch);
    }
  } catch (err) {
    console.error(`  Error searching "${title}": ${err.message}`);
  }

  return { link: null, docId: null, format: null, title: null, author: null };
}

// Sleep helper
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Upload CSV endpoint — returns a session ID
app.post('/api/upload', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

  try {
    const { rows, headers } = await parseCSV(req.file.buffer);
    if (rows.length === 0) {
      return res.status(400).json({ error: 'CSV has no valid rows. Ensure columns "Title" and "Author" exist.' });
    }

    const sessionId = Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
    sessions.set(sessionId, {
      rows,
      headers,
      results: [],
      done: false,
    });

    // Kick off processing in background
    processSession(sessionId);

    res.json({ sessionId, total: rows.length });
  } catch (err) {
    console.error('Upload error:', err);
    res.status(500).json({ error: 'Failed to parse CSV' });
  }
});

// Background processing for a session
async function processSession(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return;

  for (let i = 0; i < session.rows.length; i++) {
    const { title, author, originalRow } = session.rows[i];
    console.log(`[${i + 1}/${session.rows.length}] Searching: "${title}" by ${author}`);

    let result;
    try {
      result = await searchEverand(title, author);
    } catch (err) {
      console.error(`  Error searching for "${title}":`, err.message);
      result = { link: null, docId: null, format: null };
    }

    // Start with all original columns preserved
    const resultRow = { ...originalRow };

    // Apply Everand's title/author spelling if a match was found
    const titleKey = Object.keys(originalRow).find(k => k.trim().toLowerCase() === 'title');
    const authorKey = Object.keys(originalRow).find(k => k.trim().toLowerCase() === 'author');
    if (titleKey) resultRow[titleKey] = result.title || title;
    if (authorKey) resultRow[authorKey] = result.author || author;

    // Also set canonical Title/Author for the frontend display
    resultRow.Title = result.title || title;
    resultRow.Author = result.author || author;

    // Append Everand columns
    resultRow.Everand_Link = result.link || '';
    resultRow.Doc_ID = result.docId || '';
    resultRow.Format = result.format || '';

    session.results.push(resultRow);

    // Delay between searches (skip after last)
    if (i < session.rows.length - 1) {
      const delay = 1000 + Math.random() * 1000; // 1-2s
      await sleep(delay);
    }
  }

  session.done = true;
  console.log(`Session ${sessionId} complete.`);
}

// SSE endpoint — stream results as they arrive
app.get('/api/results/:sessionId', (req, res) => {
  const sessionId = req.params.sessionId;
  const session = sessions.get(sessionId);

  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  let sent = 0;

  const interval = setInterval(() => {
    // Send any new results
    while (sent < session.results.length) {
      const row = session.results[sent];
      res.write(`data: ${JSON.stringify({ type: 'result', index: sent, row })}\n\n`);
      sent++;
    }

    // If processing is done, send completion event and close
    if (session.done && sent >= session.results.length) {
      res.write(`data: ${JSON.stringify({ type: 'done' })}\n\n`);
      clearInterval(interval);
      res.end();
    }
  }, 500);

  req.on('close', () => clearInterval(interval));
});

// Download CSV endpoint
app.get('/api/download/:sessionId', (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).json({ error: 'Session not found' });

  // Build field list: original CSV columns (in original order) + Everand columns appended
  const everandFields = ['Everand_Link', 'Doc_ID', 'Format'];
  const originalHeaders = (session.headers || []).filter(h => !everandFields.includes(h));
  // Deduplicate: if original CSV already had canonical Title/Author, don't double-add
  const seen = new Set(originalHeaders.map(h => h.trim().toLowerCase()));
  const extraCanonical = ['Title', 'Author'].filter(h => !seen.has(h.toLowerCase()));
  const fields = [...originalHeaders, ...extraCanonical, ...everandFields];

  const parser = new Json2CsvParser({ fields });
  const csvData = parser.parse(session.results);

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="everand_results.csv"');
  res.send(csvData);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Everand Checker running at http://localhost:${PORT}`);
});
