// ============================================================
//  CLOSER · relay server
//  A tiny WebSocket relay. It holds no game logic — it only
//  passes each player's position, chat and emotes to the other.
//  Rooms are keyed by an id the client sends in 'join' (?room= /
//  ROOM_ID) — each room only ever hears its own pair. A client that
//  omits room (an older build, or a bad param) falls back to
//  'default', so nothing breaks if only one side is updated.
//  Deploy target: Render free tier (or any Node host).
// ============================================================

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3001;

// --- plain HTTP endpoint so hosting health checks pass -------
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Closer server awake · HYD ✦ 2,430 km ✦ DXB\n');
});

const wss = new WebSocketServer({ server });

// id -> { ws, room, color, name, d (last state) } — room is null until 'join'
const players = new Map();
let nextId = 1;

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

// only reaches players in the same room as the sender
function broadcast(msg, exceptId, room) {
  if (!room) return;
  const raw = JSON.stringify(msg);
  for (const [id, p] of players) {
    if (id !== exceptId && p.room === room && p.ws.readyState === p.ws.OPEN) p.ws.send(raw);
  }
}

wss.on('connection', (ws) => {
  const id = String(nextId++);
  players.set(id, { ws, room: null, color: null, name: null, char: null, d: null });
  ws.isAlive = true;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const me = players.get(id);
    if (!me) return;

    switch (msg.t) {
      case 'join': {
        me.color = /^#[0-9a-fA-F]{6}$/.test(msg.color || '') ? msg.color : '#e8b84b';
        me.name = String(msg.name || '').slice(0, 20) || 'Someone';
        me.char = String(msg.char || '').slice(0, 20) || 'pip';
        me.room = String(msg.room || '').slice(0, 40) || 'default';

        // reconnect, not a third party: the room is always exactly two named
        // people, so if the same name rejoins the same room while their old
        // socket hasn't been reaped yet (refresh, dropped wifi, a
        // backgrounded mobile tab resuming), the room would otherwise briefly
        // hold both — and the OTHER client's partnerId() can lock onto the
        // stale one until the heartbeat catches up (~5-10s). Evict the old
        // socket immediately instead of waiting on the heartbeat backstop.
        const myNameLower = me.name.toLowerCase();
        for (const [pid, p] of players) {
          if (pid !== id && p.room === me.room && p.color && p.ws.readyState === p.ws.OPEN &&
              p.name && p.name.toLowerCase() === myNameLower) {
            broadcast({ t: 'left', id: pid }, pid, p.room);
            p.ws.terminate();
            players.delete(pid);
          }
        }

        // roster is only knowable once we know which room to scope it to —
        // welcome waits for 'join' rather than firing on raw connection.
        // Only list sockets that are OPEN, joined, and in the SAME room —
        // skips half-dead "ghost" connections and every other room's pair.
        const roster = [];
        for (const [pid, p] of players) {
          if (pid !== id && p.room === me.room && p.color && p.ws.readyState === p.ws.OPEN) {
            roster.push({ id: pid, color: p.color, name: p.name, char: p.char, d: p.d });
          }
        }
        send(ws, { t: 'welcome', id, players: roster });

        broadcast({ t: 'joined', id, color: me.color, name: me.name, char: me.char }, id, me.room);
        break;
      }

      case 'state': // { d: {x,y,z,ry,mv,sit} } ~12 times/sec
        me.d = msg.d;
        broadcast({ t: 'state', id, d: msg.d }, id, me.room);
        break;

      case 'chat':
        broadcast({ t: 'chat', id, text: String(msg.text || '').slice(0, 140) }, id, me.room);
        break;

      case 'emote': // wave | action | hug
        broadcast({ t: 'emote', id, e: String(msg.e || '').slice(0, 12) }, id, me.room);
        break;

      case 'fx': // shared effects, e.g. fireworks
        broadcast({ t: 'fx', kind: String(msg.kind || '').slice(0, 12), x: +msg.x || 0, z: +msg.z || 0 }, id, me.room);
        break;
    }
  });

  ws.on('close', () => {
    const me = players.get(id);
    players.delete(id);
    if (me) broadcast({ t: 'left', id }, id, me.room);   // no-ops if they never joined (room still null)
  });
});

// Keepalive — drop dead sockets fast so "ghost" players clear quickly,
// keep live ones warm. (Was 30s; tightened so presence stays accurate.)
const heartbeat = setInterval(() => {
  for (const [id, p] of players) {
    if (!p.ws.isAlive) {
      p.ws.terminate();
      players.delete(id);
      broadcast({ t: 'left', id }, id, p.room);
      continue;
    }
    p.ws.isAlive = false;
    p.ws.ping();
  }
}, 5000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`Closer relay listening on :${PORT}`);
});
