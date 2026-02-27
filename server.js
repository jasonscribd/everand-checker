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

// Parse CSV buffer into array of {Title, Author}
function parseCSV(buffer) {
  return new Promise((resolve, reject) => {
    const results = [];
    const stream = Readable.from(buffer.toString());
    stream
      .pipe(csv())
      .on('data', (row) => {
        // Normalize column names — handle various casings/spacings
        const normalized = {};
        for (const key of Object.keys(row)) {
          normalized[key.trim().toLowerCase()] = row[key].trim();
        }
        const title = normalized['title'] || '';
        const author = normalized['author'] || '';
        if (title) results.push({ Title: title, Author: author });
      })
      .on('end', () => resolve(results))
      .on('error', reject);
  });
}

// Search Everand's own API for a title+author match
async function searchEverand(title, author) {
  const query = `${title} ${author}`;
  const apiUrl = `https://www.everand.com/search/query?query=${encodeURIComponent(query)}`;

  const res = await fetch(apiUrl, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      'Accept': 'application/json',
    },
  });

  if (!res.ok) {
    console.error(`  Everand API returned ${res.status} for "${title}"`);
    return { link: null, docId: null, format: null };
  }

  const data = await res.json();
  const titleLower = title.toLowerCase();
  const authorLower = author.toLowerCase();

  // Helper: check if a document is a likely match by title/author
  function isMatch(doc) {
    const docTitle = (doc.title || '').toLowerCase();
    const docAuthor = (doc.author && doc.author.name || '').toLowerCase();
    // Check title similarity — one contains the other (handles subtitle differences)
    const titleMatch = docTitle.includes(titleLower) || titleLower.includes(docTitle);
    const authorMatch = docAuthor.includes(authorLower) || authorLower.includes(docAuthor);
    return titleMatch && authorMatch;
  }

  // Priority 1: audiobook results — look for a title+author match
  const audiobooks = data.results?.audiobooks?.content?.documents || [];
  for (const doc of audiobooks) {
    if (isMatch(doc)) {
      return {
        link: doc.book_preview_url || `https://www.everand.com/audiobook/${doc.id}`,
        docId: String(doc.id),
        format: 'audiobook',
      };
    }
  }

  // Priority 2: any audiobook result if only one came back (likely correct)
  if (audiobooks.length === 1) {
    const doc = audiobooks[0];
    return {
      link: doc.book_preview_url || `https://www.everand.com/audiobook/${doc.id}`,
      docId: String(doc.id),
      format: 'audiobook',
    };
  }

  // Priority 3: book/ebook results — fetch with content_type=books
  const booksRes = await fetch(
    `https://www.everand.com/search/query?query=${encodeURIComponent(query)}&content_type=books`,
    {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json',
      },
    }
  );

  if (booksRes.ok) {
    const booksData = await booksRes.json();
    const books = booksData.results?.books?.content?.documents || [];
    for (const doc of books) {
      if (isMatch(doc)) {
        return {
          link: doc.book_preview_url || `https://www.everand.com/book/${doc.id}`,
          docId: String(doc.id),
          format: 'ebook',
        };
      }
    }
    // Fallback: single book result
    if (books.length === 1) {
      const doc = books[0];
      return {
        link: doc.book_preview_url || `https://www.everand.com/book/${doc.id}`,
        docId: String(doc.id),
        format: 'ebook',
      };
    }
  }

  return { link: null, docId: null, format: null };
}

// Sleep helper
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Upload CSV endpoint — returns a session ID
app.post('/api/upload', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

  try {
    const rows = await parseCSV(req.file.buffer);
    if (rows.length === 0) {
      return res.status(400).json({ error: 'CSV has no valid rows. Ensure columns "Title" and "Author" exist.' });
    }

    const sessionId = Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
    sessions.set(sessionId, {
      rows,
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
    const { Title, Author } = session.rows[i];
    console.log(`[${i + 1}/${session.rows.length}] Searching: "${Title}" by ${Author}`);

    let result;
    try {
      result = await searchEverand(Title, Author);
    } catch (err) {
      console.error(`  Error searching for "${Title}":`, err.message);
      result = { link: null, docId: null, format: null };
    }

    session.results.push({
      Title,
      Author,
      Everand_Link: result.link,
      Doc_ID: result.docId,
      Format: result.format,
    });

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

  const fields = ['Title', 'Author', 'Everand_Link', 'Doc_ID', 'Format'];
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
