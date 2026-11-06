'use strict';

// RP RUSH real-time multiplayer relay (WebSocket)
// Node.js 20+ | No PlayFab Secret Key is required here.
const http = require('node:http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const MAX_PLAYERS = Math.max(2, Math.min(500, Number(process.env.MAX_PLAYERS || 100)));
const UPDATE_INTERVAL_MS = 100;
const players = new Map();
const socketsById = new Map();

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ game: 'RP RUSH', status: 'online', players: players.size, maxPlayers: MAX_PLAYERS }));
    return;
  }
  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocketServer({ server, maxPayload: 4096, perMessageDeflate: false });

function cleanText(value, max = 24) {
  return String(value ?? '').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, max) || 'بازیکن';
}
function validId(value) {
  const id = String(value ?? '');
  return /^[a-zA-Z0-9@._:-]{3,100}$/.test(id) ? id : null;
}
function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function snapshot(exceptId = null) {
  return [...players.values()].filter(p => p.id !== exceptId).map(p => ({
    id: p.id, name: p.name, x: p.x, z: p.z, ry: p.ry, skin: p.skin
  }));
}
function broadcastSnapshot() {
  const list = [...players.values()].map(p => ({ id:p.id, name:p.name, x:p.x, z:p.z, ry:p.ry, skin:p.skin }));
  for (const ws of wss.clients) if (ws.readyState === WebSocket.OPEN) send(ws, { type: 'snapshot', players: list });
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.playerId = null;
  ws.lastUpdate = 0;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg !== 'object') return;

    if (msg.type === 'join') {
      const id = validId(msg.id);
      if (!id) { send(ws, { type:'error', message:'شناسه بازیکن نامعتبر است' }); ws.close(1008, 'invalid id'); return; }
      if (!players.has(id) && players.size >= MAX_PLAYERS) {
        send(ws, { type:'error', message:'ظرفیت سرور تکمیل است' }); ws.close(1013, 'server full'); return;
      }
      const previous = socketsById.get(id);
      if (previous && previous !== ws) {
        previous.playerId = null;
        previous.close(4001, 'signed in elsewhere');
      }
      ws.playerId = id;
      socketsById.set(id, ws);
      const old = players.get(id);
      players.set(id, {
        id,
        name: cleanText(msg.name, 24),
        x: old?.x ?? 0, z: old?.z ?? 0, ry: old?.ry ?? 0,
        skin: Number.isInteger(msg.skin) ? Math.max(0, Math.min(50, msg.skin)) : 0,
        lastSeen: Date.now()
      });
      send(ws, { type:'snapshot', players:snapshot(id) });
      broadcastSnapshot();
      return;
    }

    if (msg.type === 'move') {
      if (!ws.playerId || msg.id !== ws.playerId) return;
      const now = Date.now();
      if (now - ws.lastUpdate < UPDATE_INTERVAL_MS) return;
      ws.lastUpdate = now;
      const p = players.get(ws.playerId);
      if (!p) return;
      const x = Number(msg.x), z = Number(msg.z), ry = Number(msg.ry);
      if (!Number.isFinite(x) || !Number.isFinite(z) || !Number.isFinite(ry)) return;
      p.x = Math.max(-120, Math.min(120, x));
      p.z = Math.max(-120, Math.min(120, z));
      p.ry = ((ry % (Math.PI * 2)) + Math.PI * 2) % (Math.PI * 2);
      p.name = cleanText(msg.name, 24);
      p.skin = Number.isInteger(msg.skin) ? Math.max(0, Math.min(50, msg.skin)) : p.skin;
      p.lastSeen = now;
      // Send only to other connected players; their clients render this player's avatar.
      const update = { type:'update', player:{ id:p.id, name:p.name, x:p.x, z:p.z, ry:p.ry, skin:p.skin } };
      for (const client of wss.clients) {
        if (client !== ws && client.readyState === WebSocket.OPEN) send(client, update);
      }
    }
  });

  ws.on('close', () => {
    const id = ws.playerId;
    if (id && socketsById.get(id) === ws) {
      socketsById.delete(id);
      players.delete(id);
      broadcastSnapshot();
    }
  });
  ws.on('error', () => {});
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`RP RUSH server listening on ${PORT}; max players=${MAX_PLAYERS}`);
});
