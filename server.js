'use strict';

/* ------------------------------------------------------------------ */
/*  Sayadaw Dhamma Preaching Live - cloud overlay server               */
/*  Standalone: connects straight to TikTok LIVE (no TikFinity).       */
/*  Multi-tenant: OBS URL = https://your-app.onrender.com/?username=XXX */
/* ------------------------------------------------------------------ */

const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');

const PORT = process.env.PORT || 3000;
const MAX_ROOMS = Number(process.env.MAX_ROOMS) || 50;      // max different streamers at once
const GRACE_MS = 30000;      // keep the TikTok connection this long after the last OBS client leaves
                             // (an OBS refresh does not cause a reconnect to TikTok)
const RETRY_BASE_MS = 15000; // reconnect backoff: 15s, 30s, 60s, 120s (max) - protects against IP bans
const RETRY_MAX_MS = 120000;

/* Idle bots + sound: sent to the overlay on connect (same as before) */
const BOT_ENABLED = true;
const BOT_MIN_MS = 10000;
const BOT_MAX_MS = 15000;
const SOUND_CONFIG = { dhammaVolume: 0.5 };

/* ------------------------------------------------------------------ */
/*  Hall memory (per streamer):  hall.json = { "username": [users] }   */
/*  NOTE: Render's disk is ephemeral - hall.json is reset on redeploy  */
/*  unless you attach a Render Disk and set HALL_DIR to its mount path.*/
/* ------------------------------------------------------------------ */
const HALL_FILE = path.join(process.env.HALL_DIR || __dirname, 'hall.json');
const HALL_MAX = 30;

let halls = {};
try {
  const saved = JSON.parse(fs.readFileSync(HALL_FILE, 'utf8'));
  if (saved && typeof saved === 'object' && !Array.isArray(saved)) halls = saved;
} catch (e) { /* first run */ }

let saveTimer = null;
function saveHall() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      fs.writeFile(HALL_FILE, JSON.stringify(halls, null, 1), (err) => {
        if (err) console.error('[HALL] could not save:', err.message);
      });
    } catch (e) { console.error('[HALL] save failed:', e.message); }
  }, 500);
}

function rememberUser(username, u) {
  let list = (halls[username] || []).filter((h) => h.userId !== u.userId);
  list.push({ userId: u.userId, nickname: u.nickname, profilePictureUrl: u.profilePictureUrl });
  if (list.length > HALL_MAX) list = list.slice(-HALL_MAX);
  halls[username] = list;
  saveHall();
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */
function safeStr(v, max, fallback) {
  try {
    if (Array.isArray(v)) v = v[0];
    if (v === undefined || v === null || typeof v === 'object') return fallback;
    const s = String(v).trim();
    return s ? Array.from(s).slice(0, max).join('') : fallback;
  } catch (e) { return fallback; }
}
function safeDecode(s) { try { return decodeURIComponent(s); } catch (e) { return String(s); } }

// "@Some.User " -> "some.user"; returns null when it is not a valid TikTok username
function normalizeUsername(v) {
  const s = safeStr(v, 40, '').replace(/^@+/, '').toLowerCase();
  return /^[a-z0-9._]{2,24}$/.test(s) ? s : null;
}

// tiktok-live-connector v1 delivers flat fields; nested "user" is handled too
function getUser(d) {
  const u = d.user || {};
  const nickname = d.nickname || u.nickname || d.uniqueId || u.uniqueId || 'Guest';
  const userId = String(d.userId || u.userId || d.uniqueId || u.uniqueId || nickname);
  const profilePictureUrl = d.profilePictureUrl ||
    (typeof u.profilePicture === 'string' ? u.profilePicture : '') ||
    (u.profilePicture && ((u.profilePicture.url && u.profilePicture.url[0]) ||
      (u.profilePicture.urls && u.profilePicture.urls[0]))) || '';
  return { userId, nickname, profilePictureUrl };
}
function getGift(d) {
  const det = d.giftDetails || {};
  const giftName = d.giftName || det.giftName || (d.gift && d.gift.name) || 'Gift';
  const giftType = d.giftType !== undefined ? d.giftType : det.giftType;
  const count = Math.max(1, Number(d.repeatCount) || 1);
  const giftPictureUrl = d.giftPictureUrl ||
    (det.giftImage && (det.giftImage.giftPictureUrl || (det.giftImage.url && det.giftImage.url[0]))) || '';
  return { giftName, giftType, count, giftPictureUrl };
}

/* ------------------------------------------------------------------ */
/*  Web server + Socket.IO                                             */
/* ------------------------------------------------------------------ */
const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e4, pingInterval: 20000, pingTimeout: 25000 });

const rooms = new Map();   // username -> room

function emitTo(room, event, payload) {
  try { io.to(room.id).emit(event, Object.assign({ type: event }, payload || {})); }
  catch (e) { console.error('[IO] emit failed:', e.message); }
}

let eventCounter = 0;
// ANY gift sends a devotee. Payload is identical to the old WebSocket message.
function sendGift(room, user, gift, persist) {
  eventCounter += 1;
  if (persist) rememberUser(room.name, user);
  emitTo(room, 'gift', {
    id: `gift-${Date.now()}-${eventCounter}`,
    userId: user.userId,
    nickname: user.nickname,
    profilePictureUrl: user.profilePictureUrl,
    giftName: gift.giftName,
    count: gift.count,
    giftPictureUrl: gift.giftPictureUrl,
  });
}

/* ------------------------------------------------------------------ */
/*  TikTok connection per streamer (shared by all their OBS clients)   */
/* ------------------------------------------------------------------ */
function getRoom(username) {
  let room = rooms.get(username);
  if (room) return room;
  if (rooms.size >= MAX_ROOMS) return null;
  room = {
    id: 'u:' + username, name: username,
    clients: 0, conn: null,
    graceTimer: null, retryTimer: null, retries: 0,
    lastGiftKey: '', lastGiftAt: 0, closed: false,
  };
  rooms.set(username, room);
  return room;
}

function handleTikTokGift(room, data) {
  const gift = getGift(data);
  // Streakable gifts fire many events while tapping: only the final one counts
  if (gift.giftType === 1 && data.repeatEnd === false) return;

  const user = getUser(data);
  const key = `${user.userId}|${gift.giftName}|${gift.count}|${data.msgId || data.giftId || ''}`;
  const now = Date.now();
  if (data.msgId && key === room.lastGiftKey && now - room.lastGiftAt < 1000) return;
  room.lastGiftKey = key;
  room.lastGiftAt = now;

  console.log(`[${room.name}] GIFT ${user.nickname} sent ${gift.giftName} x${gift.count}`);
  sendGift(room, user, gift, true);
}

function scheduleRetry(room) {
  if (room.closed || room.retryTimer || room.clients <= 0) return;
  const delay = Math.min(RETRY_MAX_MS, RETRY_BASE_MS * Math.pow(2, room.retries));
  room.retries += 1;
  console.log(`[${room.name}] retrying TikTok in ${delay / 1000}s`);
  room.retryTimer = setTimeout(() => { room.retryTimer = null; startTikTok(room); }, delay);
}

function startTikTok(room) {
  if (room.closed || room.conn || room.clients <= 0) return;
  const conn = new WebcastPushConnection(room.name, {
    processInitialData: false,        // do not replay old chat/gifts
    enableExtendedGiftInfo: true,     // gives giftPictureUrl for the banner
    requestPollingIntervalMs: 2000,
  });
  room.conn = conn;

  // The 5 event types requested. Only "gift" drives the current frontend;
  // the others are forwarded under their own names and simply ignored by it.
  conn.on('gift', (d) => { try { handleTikTokGift(room, d); } catch (e) { console.error('[gift]', e.message); } });
  conn.on('chat', (d) => emitTo(room, 'chat', { ...getUser(d), comment: safeStr(d.comment, 200, '') }));
  conn.on('follow', (d) => emitTo(room, 'follow', getUser(d)));
  conn.on('share', (d) => emitTo(room, 'share', getUser(d)));
  conn.on('member', (d) => emitTo(room, 'join', getUser(d)));

  conn.on('streamEnd', () => {
    console.log(`[${room.name}] live ended`);
    emitTo(room, 'status', { state: 'ended' });
  });
  conn.on('disconnected', () => {
    if (room.conn !== conn) return;
    room.conn = null;
    console.log(`[${room.name}] TikTok disconnected`);
    emitTo(room, 'status', { state: 'disconnected' });
    scheduleRetry(room);
  });
  conn.on('error', (e) => console.error(`[${room.name}] TikTok error:`, (e && (e.message || e.info)) || e));

  conn.connect().then((state) => {
    if (room.conn !== conn) { try { conn.disconnect(); } catch (e) { /* ignore */ } return; }
    room.retries = 0;
    console.log(`[${room.name}] connected to LIVE room ${state && state.roomId}`);
    emitTo(room, 'status', { state: 'connected' });
  }).catch((err) => {
    if (room.conn === conn) room.conn = null;
    console.error(`[${room.name}] connect failed:`, (err && err.message) || err);
    emitTo(room, 'status', { state: 'offline' });   // not live yet / blocked: keep trying with backoff
    scheduleRetry(room);
  });
}

function stopTikTok(room) {
  if (room.retryTimer) { clearTimeout(room.retryTimer); room.retryTimer = null; }
  const conn = room.conn;
  room.conn = null;
  if (conn) {
    try { conn.removeAllListeners(); conn.on('error', () => {}); conn.disconnect(); }
    catch (e) { console.error(`[${room.name}] disconnect failed:`, e.message); }
  }
}

function closeRoom(room) {
  room.closed = true;
  if (room.graceTimer) { clearTimeout(room.graceTimer); room.graceTimer = null; }
  stopTikTok(room);
  rooms.delete(room.name);
  console.log(`[${room.name}] room closed (no OBS clients) - TikTok connection released`);
}

io.on('connection', (socket) => {
  const username = normalizeUsername(socket.handshake.query && socket.handshake.query.username);
  if (!username) {
    // No / invalid ?username= : stay connected but empty, tell the overlay why
    socket.emit('status', { type: 'status', state: 'no-username' });
    socket.emit('sync', { type: 'sync', users: [], config: Object.assign({ botEnabled: BOT_ENABLED, botMinMs: BOT_MIN_MS, botMaxMs: BOT_MAX_MS }, SOUND_CONFIG) });
    return;
  }
  const room = getRoom(username);
  if (!room) {
    socket.emit('status', { type: 'status', state: 'server-full' });
    socket.disconnect(true);
    return;
  }

  socket.join(room.id);
  room.clients += 1;
  if (room.graceTimer) { clearTimeout(room.graceTimer); room.graceTimer = null; }
  console.log(`[${username}] overlay connected (${room.clients} client(s))`);

  socket.emit('sync', {
    type: 'sync',
    users: halls[username] || [],
    config: Object.assign({ botEnabled: BOT_ENABLED, botMinMs: BOT_MIN_MS, botMaxMs: BOT_MAX_MS }, SOUND_CONFIG),
  });

  startTikTok(room);   // no-op if already connected / retrying

  socket.on('disconnect', () => {
    room.clients = Math.max(0, room.clients - 1);
    console.log(`[${username}] overlay left (${room.clients} client(s))`);
    if (room.clients === 0 && !room.graceTimer) {
      room.graceTimer = setTimeout(() => { room.graceTimer = null; if (room.clients === 0) closeRoom(room); }, GRACE_MS);
    }
  });
  socket.on('error', (e) => console.error(`[${username}] socket error:`, (e && e.message) || e));
});

/* ------------------------------------------------------------------ */
/*  HTTP routes (all need ?username=)                                  */
/*    /test?username=abc&name=Hung&gift=Rose&count=3                   */
/*    /test?username=abc&name=Guest&n=20                               */
/*    /reset?username=abc      /bot?username=abc                       */
/* ------------------------------------------------------------------ */
app.get('/healthz', (req, res) => res.status(200).send('ok'));   // Render health check

app.get('/test', (req, res) => {
  res.status(200).send('OK');
  try {
    const q = req.query || {};
    const room = rooms.get(normalizeUsername(q.username));
    if (!room) return console.log('[TEST] no open overlay for that username');
    const name = safeStr(q.name, 40, 'Test User');
    const giftName = safeStr(q.gift, 30, 'Rose');
    const count = Math.max(1, parseInt(safeStr(q.count, 10, '1'), 10) || 1);
    const pic = safeStr(q.pic, 500, '');
    const n = Math.min(30, Math.max(1, parseInt(safeStr(q.n, 4, '1'), 10) || 1));
    for (let i = 0; i < n; i++) {
      setTimeout(() => {
        try {
          const nick = n > 1 ? `${name} ${i + 1}` : name;
          sendGift(room, { userId: `test-${nick}`, nickname: nick, profilePictureUrl: pic },
            { giftName, count, giftPictureUrl: '' }, false);
        } catch (e) { console.error('[TEST] gift failed:', e && e.message); }
      }, i * 800);
    }
  } catch (e) { console.error('[TEST] bad request ignored:', e && e.message); }
});

app.get('/reset', (req, res) => {
  res.status(200).send('Hall reset. Overlay will reload.');
  try {
    const username = normalizeUsername(req.query && req.query.username);
    if (!username) return;
    delete halls[username];
    saveHall();
    const room = rooms.get(username);
    if (room) emitTo(room, 'reset');
  } catch (e) { console.error('[RESET] failed:', e && e.message); }
});

app.get('/bot', (req, res) => {
  res.status(200).send('OK');
  try {
    const room = rooms.get(normalizeUsername(req.query && req.query.username));
    if (room) emitTo(room, 'bot');
  } catch (e) { console.error('[BOT] failed:', e && e.message); }
});

/* ------------------------------------------------------------------ */
/*  Static files from the project root (allow-list; code/data private) */
/* ------------------------------------------------------------------ */
const SERVED_EXT = new Set([
  '.html', '.css', '.png', '.jpg', '.jpeg', '.webp', '.gif', '.svg',
  '.mp4', '.webm', '.mp3', '.ogg', '.wav', '.m4a', '.woff', '.woff2', '.ttf', '.ico',
]);
const PRIVATE_FILES = new Set(['server.js', 'hall.json', 'package.json', 'package-lock.json']);
const staticFiles = express.static(__dirname, { index: false, dotfiles: 'ignore', fallthrough: true });

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.use((req, res, next) => {
  if (req.method !== 'GET' && req.method !== 'HEAD') return next();
  if (!/^\/[^/]+$/.test(req.path)) return next();
  const name = safeDecode(req.path.slice(1)).toLowerCase();
  if (name.includes('..') || name.includes('\\') || name.startsWith('.') ||
      PRIVATE_FILES.has(name) || !SERVED_EXT.has(path.extname(name))) return next();
  try { staticFiles(req, res, next); } catch (e) { next(); }
});

app.use((err, req, res, next) => {
  console.error('[HTTP] request error:', err && err.message);
  if (res.headersSent) return;
  try { res.status(200).send('OK'); } catch (e) { /* socket gone */ }
});

server.on('error', (err) => console.error('[SERVER] error:', (err && err.message) || err));
server.on('clientError', (err, socket) => { try { socket.end('HTTP/1.1 400 Bad Request\r\n\r\n'); } catch (e) { /* ignore */ } });

server.listen(PORT, () => {
  console.log(`Sayadaw Dhamma Preaching Live overlay listening on port ${PORT}`);
  console.log(`OBS URL:  /?username=TIKTOK_USERNAME`);
  console.log(`Test:     /test?username=TIKTOK_USERNAME&name=Guest&n=20`);
});

/* ------------------------------------------------------------------ */
/*  Shutdown + crash protection                                        */
/* ------------------------------------------------------------------ */
function shutdown() {
  console.log('Shutting down - releasing TikTok connections');
  for (const room of Array.from(rooms.values())) { try { closeRoom(room); } catch (e) { /* ignore */ } }
  try { fs.writeFileSync(HALL_FILE, JSON.stringify(halls, null, 1)); } catch (e) { /* ignore */ }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
process.on('uncaughtException', (err) => console.error('Caught exception:', err));
process.on('unhandledRejection', (reason) => console.error('Unhandled Rejection:', reason));
