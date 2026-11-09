'use strict';

/* ------------------------------------------------------------------ */
/*  Sayadaw Dhamma Preaching Live  -  overlay server (PORT 5000)       */
/* ------------------------------------------------------------------ */

const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');

/* TikFinity local WebSocket (TikFinity Desktop App must be running and connected to your LIVE). */
const TIKFINITY_WS_URL = process.env.TIKFINITY_WS || 'ws://localhost:21213/';

const PORT = 5000;                 // Sayadaw Dhamma Preaching Live
const RECONNECT_DELAY_MS = 5000;

/* ------------------------------------------------------------------ */
/*  Idle loop: bot devotees keep the screen moving while nobody sends  */
/*  a gift. Sent to the overlay on connect, so you only edit it here.  */
/* ------------------------------------------------------------------ */
const BOT_ENABLED = true;
const BOT_MIN_MS = 10000;   // 10 seconds
const BOT_MAX_MS = 15000;   // 15 seconds

/* ------------------------------------------------------------------ */
/*  Sound settings (sent to the overlay on connect)                    */
/*  Optional background chanting loop: put  dhamma.mp3  in this folder.*/
/*  No dhamma.mp3?  The overlay stays silent (bowing is always silent).*/
/* ------------------------------------------------------------------ */
const SOUND_CONFIG = {
  dhammaVolume: 0.5,     // background loop (dhamma.mp3), 0.0 - 1.0
};

/* ------------------------------------------------------------------ */
/*  Hall memory                                                        */
/*  The server remembers the most recent gift senders (hall.json) so   */
/*  the seated devotees come back after you refresh OBS or restart.    */
/* ------------------------------------------------------------------ */
const HALL_FILE = path.join(__dirname, 'hall.json');
const HALL_MAX = 30;   // must be >= number of seats in index.html (20 seats: 10 left + 10 right)

let hall = [];
try {
  const saved = JSON.parse(fs.readFileSync(HALL_FILE, 'utf8'));
  if (Array.isArray(saved)) hall = saved.slice(-HALL_MAX);
} catch (e) { /* first run: no file yet */ }

// Debounced: a burst of gifts writes hall.json once, never several overlapping writes
let saveTimer = null;
function saveHall() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      fs.writeFile(HALL_FILE, JSON.stringify(hall, null, 1), (err) => {
        if (err) console.error('[HALL] could not save hall.json:', err.message);
      });
    } catch (e) {
      console.error('[HALL] save failed:', e.message);
    }
  }, 500);
}

// decodeURIComponent that never throws on emojis / malformed % sequences
function safeDecode(s) {
  try { return decodeURIComponent(s); } catch (e) { return String(s); }
}
// Any query value (string, array, object, emoji, garbage) -> short safe string
function safeStr(v, max, fallback) {
  try {
    if (Array.isArray(v)) v = v[0];
    if (v === undefined || v === null || typeof v === 'object') return fallback;
    const s = String(v).trim();
    return s ? Array.from(s).slice(0, max).join('') : fallback;   // Array.from = never cuts an emoji in half
  } catch (e) { return fallback; }
}

function rememberUser(u) {
  hall = hall.filter((h) => h.userId !== u.userId);   // same viewer moves to "newest"
  hall.push({
    userId: u.userId,
    nickname: u.nickname,
    profilePictureUrl: u.profilePictureUrl,
  });
  if (hall.length > HALL_MAX) hall = hall.slice(-HALL_MAX);
  saveHall();
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

// Works with flat fields (TikFinity / v1) and nested "user" (v2) shapes
function getUser(d) {
  const u = d.user || {};
  const nickname = d.nickname || u.nickname || d.uniqueId || u.uniqueId || 'Guest';
  const userId = String(d.userId || u.userId || d.uniqueId || u.uniqueId || nickname);
  const profilePictureUrl =
    d.profilePictureUrl ||
    (typeof u.profilePicture === 'string' ? u.profilePicture : '') ||
    (u.profilePicture && ((u.profilePicture.url && u.profilePicture.url[0]) ||
      (u.profilePicture.urls && u.profilePicture.urls[0]))) ||
    '';
  return { userId, nickname, profilePictureUrl };
}

function getGift(d) {
  const det = d.giftDetails || {};
  const giftName = d.giftName || det.giftName || (d.gift && d.gift.name) || 'Gift';
  const giftType = d.giftType !== undefined ? d.giftType : det.giftType;
  const count = Math.max(1, Number(d.repeatCount) || 1);
  const giftPictureUrl =
    d.giftPictureUrl ||
    (det.giftImage && (det.giftImage.giftPictureUrl || (det.giftImage.url && det.giftImage.url[0]))) ||
    '';
  return { giftName, giftType, count, giftPictureUrl };
}

/* ------------------------------------------------------------------ */
/*  Web server + WebSocket (for the overlay in OBS)                    */
/* ------------------------------------------------------------------ */
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Static files: every asset in this folder is served (html, css, png, mp4, mp3, ...),
// so new images / audio never 404. Code and data files stay private.
const SERVED_EXT = new Set([
  '.html', '.css', '.png', '.jpg', '.jpeg', '.webp', '.gif', '.svg',
  '.mp4', '.webm', '.mp3', '.ogg', '.wav', '.m4a', '.woff', '.woff2', '.ttf',
]);
const PRIVATE_FILES = new Set(['server.js', 'hall.json', 'package.json', 'package-lock.json']);
const staticFiles = express.static(__dirname, {
  index: false,
  dotfiles: 'ignore',
  fallthrough: true,   // static also handles HTTP Range requests (needed for video)
});

function broadcast(payload) {
  let msg;
  try { msg = JSON.stringify(payload); } catch (e) { return; }
  for (const client of wss.clients) {
    try {
      if (client.readyState === WebSocket.OPEN) client.send(msg);
    } catch (e) {
      console.error('[WS] send failed:', e.message);
    }
  }
}

let eventCounter = 0;

// ANY gift sends a devotee: walk up -> bow 3x -> walk to a seat -> listen to the sermon
function sendGift(user, gift, persist) {
  eventCounter += 1;
  if (persist) rememberUser(user);
  broadcast({
    type: 'gift',
    id: `gift-${Date.now()}-${eventCounter}`,
    userId: user.userId,
    nickname: user.nickname,
    profilePictureUrl: user.profilePictureUrl,
    giftName: gift.giftName,
    count: gift.count,
    giftPictureUrl: gift.giftPictureUrl,
  });
}

// Test without TikTok (test gifts are NOT saved to hall.json):
//   http://localhost:5000/test?name=Hung&gift=Rose&count=3
//   http://localhost:5000/test?name=Guest&n=20     (20 different viewers, 0.8s apart)
// The reply goes out FIRST (instant "OK"), the gift is processed afterwards, so TikFinity never times out.
app.get('/test', (req, res) => {
  res.status(200).send('OK');   // answer immediately, before any processing

  try {
    const q = req.query || {};
    const name = safeStr(q.name, 40, 'Test User');
    const giftName = safeStr(q.gift, 30, 'Rose');
    const count = Math.max(1, parseInt(safeStr(q.count, 10, '1'), 10) || 1);
    const pic = safeStr(q.pic, 500, '');
    const n = Math.min(30, Math.max(1, parseInt(safeStr(q.n, 4, '1'), 10) || 1));
    for (let i = 0; i < n; i++) {
      setTimeout(() => {
        try {
          const nick = n > 1 ? `${name} ${i + 1}` : name;
          sendGift(
            { userId: `test-${nick}`, nickname: nick, profilePictureUrl: pic },
            { giftName, count, giftPictureUrl: '' },
            false
          );
        } catch (e) {
          console.error('[TEST] gift failed:', e && e.message);
        }
      }, i * 800);
    }
    console.log(`[TEST] ${n} gift(s): ${name} sent ${giftName} x${count}`);
  } catch (e) {
    console.error('[TEST] bad request ignored:', e && e.message);
  }
});

// Clear the remembered hall (all real devotees go away, bots fill the seats again)
app.get('/reset', (req, res) => {
  res.status(200).send('Hall reset. Overlay will reload.');
  try {
    hall = [];
    saveHall();
    broadcast({ type: 'reset' });
  } catch (e) {
    console.error('[RESET] failed:', e && e.message);
  }
});

// Spawn one bot right now (to preview the idle loop):  http://localhost:5000/bot
app.get('/bot', (req, res) => {
  res.status(200).send('OK');
  try { broadcast({ type: 'bot' }); } catch (e) { console.error('[BOT] failed:', e && e.message); }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
// Top-level files only (no sub-folders, no dotfiles), allowed extensions, never server code / hall.json
app.use((req, res, next) => {
  if (req.method !== 'GET' && req.method !== 'HEAD') return next();
  if (!/^\/[^/]+$/.test(req.path)) return next();
  const name = safeDecode(req.path.slice(1)).toLowerCase();   // never throws on malformed % sequences
  if (name.includes('..') || name.includes('\\') || name.startsWith('.') || PRIVATE_FILES.has(name) || !SERVED_EXT.has(path.extname(name))) return next();
  try { staticFiles(req, res, next); } catch (e) { next(); }
});

// Last-resort Express error handler: a bad request gets a plain reply, never a crash
app.use((err, req, res, next) => {
  console.error('[HTTP] request error:', err && err.message);
  if (res.headersSent) return;
  try { res.status(200).send('OK'); } catch (e) { /* socket already gone */ }
});

wss.on('error', (err) => console.error('[WSS] error:', (err && err.message) || err));
server.on('error', (err) => {
  console.error('[SERVER] error:', (err && err.message) || err,
    err && err.code === 'EADDRINUSE' ? `(port ${PORT} is already used by another program)` : '');
});
server.on('clientError', (err, socket) => {
  try { socket.end('HTTP/1.1 400 Bad Request\r\n\r\n'); } catch (e) { /* ignore */ }
});

wss.on('connection', (ws) => {
  console.log(`[WS] overlay connected (${wss.clients.size} client(s))`);
  ws.on('error', (err) => console.error('[WS] client error:', (err && err.message) || err));
  // Tell the overlay who is already sitting in the hall
  try {
    ws.send(JSON.stringify({
      type: 'sync',
      users: hall,
      config: Object.assign(
        { botEnabled: BOT_ENABLED, botMinMs: BOT_MIN_MS, botMaxMs: BOT_MAX_MS },
        SOUND_CONFIG
      ),
    }));
  } catch (e) {
    console.error('[WS] sync failed:', e && e.message);
  }
});

/* ------------------------------------------------------------------ */
/*  TikFinity connection - listens ONLY to the "gift" event            */
/*  TikFinity sends JSON like:  { "event": "gift", "data": { ... } }   */
/* ------------------------------------------------------------------ */
let tikfinityWs = null;
let reconnectTimer = null;
let lastGiftKey = '';
let lastGiftAt = 0;

function scheduleReconnect() {
  if (reconnectTimer) return;
  console.log(`[TikFinity] retrying in ${RECONNECT_DELAY_MS / 1000}s...`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectTikFinity();
  }, RECONNECT_DELAY_MS);
}

function handleTikFinityMessage(raw) {
  let msg;
  try {
    msg = JSON.parse(raw.toString());
  } catch (e) {
    return; // ignore non-JSON frames
  }

  // Be tolerant about the envelope: { event, data } or { type, data } or a bare event
  const eventName = msg.event || msg.type || msg.eventName;
  if (eventName !== 'gift') return;

  const data = msg.data || msg.payload || msg;
  const gift = getGift(data);

  // Streakable gifts (e.g. Rose) can fire many events while tapping.
  // If TikFinity marks the streak as still running, wait for the final one.
  if (gift.giftType === 1 && data.repeatEnd === false) return;

  const user = getUser(data);

  // Safety net against the same gift being delivered twice in a row
  const key = `${user.userId}|${gift.giftName}|${gift.count}|${data.msgId || data.giftId || ''}`;
  const now = Date.now();
  if (data.msgId && key === lastGiftKey && now - lastGiftAt < 1000) return;
  lastGiftKey = key;
  lastGiftAt = now;

  console.log(`[GIFT] ${user.nickname} sent ${gift.giftName} x${gift.count}`);
  sendGift(user, gift, true);
}

function connectTikFinity() {
  console.log(`[TikFinity] connecting to ${TIKFINITY_WS_URL} ...`);
  const ws = new WebSocket(TIKFINITY_WS_URL);
  tikfinityWs = ws;

  ws.on('open', () => {
    console.log('[TikFinity] connected - waiting for gifts');
  });

  ws.on('message', (raw) => {
    try { handleTikFinityMessage(raw); } catch (e) {
      console.error('[TikFinity] message ignored:', e && e.message);
    }
  });

  ws.on('close', () => {
    console.log('[TikFinity] disconnected');
    if (tikfinityWs === ws) scheduleReconnect();
  });

  ws.on('error', (err) => {
    // 'close' fires right after 'error', which schedules the reconnect
    console.error('[TikFinity] error:', (err && err.message) || err,
      '(is TikFinity Desktop running?)');
  });
}

server.listen(PORT, () => {
  const hasVideo = fs.existsSync(path.join(__dirname, 'altar.mp4'));
  console.log(`Sayadaw Dhamma Preaching Live overlay:  http://localhost:${PORT}`);
  console.log(`Background: ${hasVideo ? 'altar.mp4 (looping video)' : 'altar.png (no altar.mp4 found)'}`);
  const hasDhamma = fs.existsSync(path.join(__dirname, 'dhamma.mp3'));
  const SPRITES = [
    'boy_walk_back.png', 'boy_bow_back.png', 'boy_pray_front.png', 'boy_sit_back.png',
    'girl_walk_back.png', 'girl_bow_back.png', 'girl_pray_front.png', 'girl_sit_back.png', 'altar.png',
  ];
  const missing = SPRITES.filter((f) => !fs.existsSync(path.join(__dirname, f)));
  console.log(missing.length
    ? `WARNING:  missing image(s) in ${__dirname}: ${missing.join(', ')}`
    : 'Sprites:  all 8 character sprites + altar.png found');
  console.log(`Chant:    ${hasDhamma ? 'dhamma.mp3 (looping)' : 'no dhamma.mp3 (silent - use OBS Desktop Audio if you like)'}`);
  console.log(`Test:     http://localhost:${PORT}/test?name=Guest&n=20`);
  console.log(`Offerings: /test?gift=Rose (flowers)  /test?gift=Candle (candlelight)  /test?gift=Galaxy (stardust)`);
  console.log(`Tuning:   http://localhost:${PORT}/?debug=1`);
  console.log(`Reset:    http://localhost:${PORT}/reset`);
  console.log(`Bot now:  http://localhost:${PORT}/bot`);
  connectTikFinity();
});

/* ------------------------------------------------------------------ */
/*  Crash protection: log the problem, keep the overlay running        */
/* ------------------------------------------------------------------ */
process.on('uncaughtException', (err) => {
  console.error('Caught exception:', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
});
