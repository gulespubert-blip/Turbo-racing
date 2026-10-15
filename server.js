// Turbo Racing — serveur multijoueur (relais WebSocket + page du jeu).
// Aucune dépendance : `node server.js` suffit (Node 18+).
// Il sert index.html ET relaie les positions entre les joueurs d'un même salon.
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000, MAX_PER_ROOM = 12, MAX_MSG = 4096;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms = new Map(); // code -> Map(id -> client)

// ---------- HTTP : la page du jeu ----------
const server = http.createServer((req, res) => {
  const p = req.url.split('?')[0];
  if (p === '/ping') {
    res.writeHead(200, { 'content-type': 'text/plain', 'cache-control': 'no-store', 'access-control-allow-origin': '*' });
    return res.end('turbo-ok');
  }
  if (p === '/' || p === '/index.html') {
    return fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(500); return res.end('index.html introuvable'); }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      res.end(data);
    });
  }
  res.writeHead(404); res.end('Not found');
});

// ---------- WebSocket minimal (RFC 6455) ----------
function frame(op, payload) {
  const n = payload.length; let h;
  if (n < 126) h = Buffer.from([0x80 | op, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, payload]);
}
const closeFrame = code => { const b = Buffer.alloc(2); b.writeUInt16BE(code, 0); return frame(8, b); };
function write(c, buf, droppable) {
  const s = c.socket;
  if (!s.writable || s.destroyed) return;
  if (droppable && s.writableLength > 65536) return; // client trop lent : on saute les positions
  s.write(buf);
}
function sendTo(c, obj, droppable) { write(c, frame(1, Buffer.from(JSON.stringify(obj))), droppable); }
function broadcast(room, from, obj, droppable) {
  const buf = frame(1, Buffer.from(JSON.stringify(obj)));
  for (const c of room.values()) if (c !== from) write(c, buf, droppable);
}
function kick(c, code) { try { c.socket.write(closeFrame(code)); } catch (e) {} try { c.socket.end(); } catch (e) {} setTimeout(() => { try { c.socket.destroy(); } catch (e) {} }, 500); }

function leave(c) {
  if (c.gone) return; c.gone = true;
  const room = rooms.get(c.code);
  if (!room) return;
  room.delete(c.id);
  if (!room.size) rooms.delete(c.code); else broadcast(room, c, { t: 'l', id: c.id });
}

function clean(v) { // une valeur de présence doit rester simple
  if (v === null || typeof v === 'number' || typeof v === 'boolean') return v;
  if (typeof v === 'string') return v.slice(0, 40);
  return undefined;
}

function onMessage(c, txt) {
  let m; try { m = JSON.parse(txt); } catch (e) { return; }
  const room = rooms.get(c.code); if (!room || !m) return;
  if (m.t === 'p' && m.d && typeof m.d === 'object') {
    const d = {}, ks = Object.keys(m.d).slice(0, 40);
    for (const k of ks) {
      if (k.length > 12) continue;
      const v = clean(m.d[k]); if (v === undefined) continue;
      d[k] = v; if (v === null) delete c.pres[k]; else c.pres[k] = v;
    }
    broadcast(room, c, { t: 'p', id: c.id, d });
  } else if (m.t === 'c' && Array.isArray(m.d) && m.d.length <= 12 && m.d.every(x => typeof x === 'number')) {
    broadcast(room, c, { t: 'c', id: c.id, d: m.d }, true);
  } else if (m.t === 'm' && typeof m.d === 'string') {
    broadcast(room, c, { t: 'm', id: c.id, d: m.d.slice(0, 40) });
  } // 'k' = keepalive : rien à faire
}

function onData(c, chunk) {
  c.buf = Buffer.concat([c.buf, chunk]);
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1], fin = !!(b0 & 0x80), op = b0 & 0x0f, masked = !!(b1 & 0x80);
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_MSG) return kick(c, 1009);
    if (!masked) return kick(c, 1002);
    if (c.buf.length < off + 4 + len) return;
    const mask = c.buf.subarray(off, off + 4), payload = Buffer.from(c.buf.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
    c.buf = c.buf.subarray(off + 4 + len);
    c.alive = true;
    if (op === 8) { try { c.socket.write(closeFrame(1000)); } catch (e) {} c.socket.end(); return; }
    if (op === 9) { write(c, frame(10, payload)); continue; }
    if (op === 10) continue;
    if (op === 1 || op === 2) c.frag = [payload]; else if (op === 0 && c.frag) c.frag.push(payload); else continue;
    if (c.frag.reduce((a, b) => a + b.length, 0) > MAX_MSG) return kick(c, 1009);
    if (fin) {
      if (++c.rate > 200) return kick(c, 1008); // ~200 msg/s max
      const txt = Buffer.concat(c.frag).toString('utf8'); c.frag = null;
      onMessage(c, txt);
    }
  }
}

server.on('upgrade', (req, socket) => {
  const u = new URL(req.url, 'http://x');
  const key = req.headers['sec-websocket-key'];
  if (u.pathname !== '/ws' || !key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const code = (u.searchParams.get('room') || '').toLowerCase();
  const c = { socket, code, id: crypto.randomBytes(4).toString('hex'), pres: {}, buf: Buffer.alloc(0), frag: null, alive: true, rate: 0, gone: false };
  socket.on('data', d => { try { onData(c, d); } catch (e) { kick(c, 1011); } });
  socket.on('close', () => leave(c));
  socket.on('error', () => leave(c));
  if (!/^[a-z0-9]{3,8}$/.test(code)) return kick(c, 4002);
  let room = rooms.get(code); if (!room) { room = new Map(); rooms.set(code, room); }
  if (room.size >= MAX_PER_ROOM) return kick(c, 4001);
  const peers = {}; for (const [id, o] of room) peers[id] = o.pres;
  room.set(c.id, c);
  sendTo(c, { t: 'h', id: c.id, peers });
  broadcast(room, c, { t: 'j', id: c.id });
});

// Ping toutes les 25 s (garde la connexion ouverte derrière les proxys) + nettoyage des morts
setInterval(() => {
  for (const room of rooms.values()) for (const c of room.values()) {
    if (!c.alive) { c.socket.destroy(); continue; }
    c.alive = false; write(c, frame(9, Buffer.alloc(0)));
  }
}, 25000);
setInterval(() => { for (const room of rooms.values()) for (const c of room.values()) c.rate = 0; }, 1000);

server.listen(PORT, () => console.log('Turbo Racing en ligne sur le port ' + PORT));
