// ============================================================
//  CLOSER · relay server
//  A tiny WebSocket relay. It holds no game logic — it only
//  passes each player's position, chat and emotes to the other.
//  Deploy target: Render free tier (or any Node host).
// ============================================================

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3001;

// --- plain HTTP endpoint so hosting health checks pass -------
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Closer server awake · HYD \u2726 2,430 km \u2726 DXB\n');
});

const wss = new WebSocketServer({ server });

// id -> { ws, role, name, d (last state) }
const players = new Map();
let nextId = 1;

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function broadcast(msg, exceptId = null) {
  const raw = JSON.stringify(msg);
  for (const [id, p] of players) {
    if (id !== exceptId && p.ws.readyState === p.ws.OPEN) p.ws.send(raw);
  }
}

wss.on('connection', (ws) => {
  const id = String(nextId++);
  players.set(id, { ws, color: null, name: null, d: null });
  ws.isAlive = true;

  // Tell the newcomer who they are and who is already here
  const roster = [];
  for (const [pid, p] of players) {
    if (pid !== id && p.color) roster.push({ id: pid, color: p.color, name: p.name, d: p.d });
  }
  send(ws, { t: 'welcome', id, players: roster });

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const me = players.get(id);
    if (!me) return;

    switch (msg.t) {
      case 'join':
        me.color = /^#[0-9a-fA-F]{6}$/.test(msg.color || '') ? msg.color : '#e8b84b';
        me.name = String(msg.name || '').slice(0, 20) || 'Someone';
        broadcast({ t: 'joined', id, color: me.color, name: me.name }, id);
        break;

      case 'state': // { d: {x,y,z,ry,mv,sit} } ~12 times/sec
        me.d = msg.d;
        broadcast({ t: 'state', id, d: msg.d }, id);
        break;

      case 'chat':
        broadcast({ t: 'chat', id, text: String(msg.text || '').slice(0, 140) }, id);
        break;

      case 'emote': // wave | heart | dance | sit | hug
        broadcast({ t: 'emote', id, e: String(msg.e || '').slice(0, 12) }, id);
        break;

      case 'fx': // shared effects, e.g. fireworks
        broadcast({ t: 'fx', kind: String(msg.kind || '').slice(0, 12), x: +msg.x || 0, z: +msg.z || 0 }, id);
        break;
    }
  });

  ws.on('close', () => {
    players.delete(id);
    broadcast({ t: 'left', id });
  });
});

// Keepalive — drop dead sockets, keep live ones warm
const heartbeat = setInterval(() => {
  for (const [id, p] of players) {
    if (!p.ws.isAlive) { p.ws.terminate(); players.delete(id); broadcast({ t: 'left', id }); continue; }
    p.ws.isAlive = false;
    p.ws.ping();
  }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`Closer relay listening on :${PORT}`);
});
