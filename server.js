// Chat ruangan tanpa dependensi. Jalankan: node server.js
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const MAX_MSGS = 200;
const rooms = new Map(); // code -> {msgs, clients:Set, last}
const hits = new Map();  // ip -> {n, t}

const clean = c => String(c || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
function room(code) {
  if (!rooms.has(code)) rooms.set(code, { msgs: [], clients: new Set(), last: Date.now() });
  return rooms.get(code);
}
function limited(ip) {
  const now = Date.now(), h = hits.get(ip) || { n: 0, t: now };
  if (now - h.t > 10000) { h.n = 0; h.t = now; }
  h.n++; hits.set(ip, h);
  return h.n > 20;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');

  if (req.method === 'GET' && url.pathname === '/events') {
    const code = clean(url.searchParams.get('room'));
    if (code.length < 4) { res.writeHead(400); return res.end(); }
    const r = room(code);
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no'
    });
    res.write('retry: 2000\n');
    res.write('event: init\ndata: ' + JSON.stringify(r.msgs) + '\n\n');
    r.clients.add(res);
    req.on('close', () => { r.clients.delete(res); r.last = Date.now(); });
    return;
  }

  if (req.method === 'POST' && url.pathname === '/send') {
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    if (limited(ip)) { res.writeHead(429); return res.end(); }
    let body = '';
    req.on('data', d => { body += d; if (body.length > 4096) req.destroy(); });
    req.on('end', () => {
      try {
        const d = JSON.parse(body);
        const code = clean(d.room);
        const text = String(d.text || '').trim().slice(0, 1000);
        const name = String(d.name || 'Anonim').trim().slice(0, 24) || 'Anonim';
        if (code.length < 4 || !text) { res.writeHead(400); return res.end(); }
        const r = room(code);
        const m = { id: crypto.randomUUID(), cid: String(d.cid || '').slice(0, 40), name, text, ts: Date.now() };
        r.msgs.push(m);
        if (r.msgs.length > MAX_MSGS) r.msgs.shift();
        r.last = Date.now();
        for (const c of r.clients) c.write('event: msg\ndata: ' + JSON.stringify(m) + '\n\n');
        res.writeHead(204); res.end();
      } catch (e) { res.writeHead(400); res.end(); }
    });
    return;
  }

  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  }
  res.writeHead(404); res.end('Tidak ditemukan');
});

// detak jantung agar koneksi tidak diputus proxy
setInterval(() => {
  for (const r of rooms.values()) for (const c of r.clients) c.write(': ping\n\n');
}, 25000);
// bersihkan ruangan kosong yang tidak aktif 24 jam
setInterval(() => {
  const now = Date.now();
  for (const [k, r] of rooms) if (!r.clients.size && now - r.last > 86400000) rooms.delete(k);
}, 3600000);

server.listen(PORT, () => console.log('Chat berjalan di http://localhost:' + PORT));
