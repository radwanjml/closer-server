// ============================================================
//  CLOSER · relay server
//  A tiny WebSocket relay. It holds no game logic — it only
//  passes each player's position, chat and emotes to the other.
//  Rooms are keyed by an id the client sends in 'join' (?room= /
//  ROOM_ID) — each room only ever hears its own pair. A client that
//  omits room (an older build, or a bad param) falls back to
//  'default', so nothing breaks if only one side is updated.
//
//  v0.3.0 adds THE CAMPFIRE to the activity sessions this file
//  already held. Reported live 2026-09-03: two people stood at
//  one fire and each had a private copy of it. Fuel, burn
//  colour, log count, wind and stick pickup were all
//  client-local, so feeding the fire changed nothing on the
//  other screen and neither player could see the other's
//  driftwood. See the CAMPFIRE section below.
//
//  Deploy target: Render free tier (or any Node host).
// ============================================================

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3001;

// --- activity sessions -----------------------------------------------
// First server-side game state in a file that was otherwise a pure relay.
// A session exists the moment anyone joins an activity and is destroyed
// only when the last participant leaves — there is no host, so nobody's
// disconnect can end it for the others. Keyed room -> activityId -> session.
// Seat/anchor assignment on the client is driven entirely by a
// participant's INDEX in `participants`, so that array's order is load-
// bearing: never sort it, only push (join) and splice (leave/expire).
const sessions = new Map();   // room -> Map(activityId -> session)

// a refresh (or a dropped wifi / backgrounded tab) must never cost a
// player their seat — their slot (and its index) is held for this long
// before it's actually freed
const SESSION_GRACE_MS = 20000;

// the only activity-specific knowledge this file has: which shared-view
// keys are mutually exclusive for a given activity. Everything else about
// sessions is generic.
const SF_ACTION_COOLDOWN_MS = 2000;
const SHARED_RULES = {
  starfall: { exclusive: ['trailsOn', 'constOn', 'fwShow'] },
  // When Embers becomes a seated session too (Tier 1.1, light painting),
  // it lands here as one more row. Nothing else in the session code is
  // starfall-specific: sessionJoin takes any activityId, getRoomSessions
  // holds a Map per room, and the seat grace is generic — so a second
  // activity is a table entry and a corner panel on the client, not a
  // protocol change. Deliberately not stubbed until it exists; an empty
  // rule here would be dead config that reads as a promise.
};

function getRoomSessions(room) {
  let m = sessions.get(room);
  if (!m) { m = new Map(); sessions.set(room, m); }
  return m;
}
// the wire shape of a session — strips the server-internal heldUntil and
// stamps the server's own clock so clients can correct for their own skew
// against startedAt (see cove.html's late-join math)
function publicSession(s) {
  return {
    activityId: s.activityId,
    startedAt: s.startedAt,
    now: Date.now(),
    participants: s.participants.map((p) => ({ id: p.id, name: p.name, connected: p.connected })),
    shared: s.shared,
  };
}


/* ================================================================
   THE CAMPFIRE

   Server-owned: `fuel`, `wind` and `claim` (stickId -> playerId) —
   exactly the three named in the 2026-08-06 scope decision — plus the
   two that fall out of them: `burnType` (what a claimed stick becomes
   when it is fed) and `logs` (pile height, a pure function of how many
   have gone in). Everything else about the fire stays client-local,
   because it is all DERIVED from those: particles, shaders, the light
   pool, the darkening, the beat ring, audio.

   Two things are deliberately NOT streamed:

   - FUEL travels as a sample plus a timestamp. Both clients extrapolate
     the same line from the same origin, so they agree between snapshots,
     and a late packet corrects instead of jumping.
   - WIND travels as the GUST — {at, dir, mag, ramp, hold, decay} — not
     as a strength. One small message every 20-40s replaces a stream, and
     a player arriving mid-gust picks it up at the right point of its arc
     for free.

   Both follow the discipline the client already had for `tl`: a derived
   value, never a wall-clock accumulator.

   Claims are held across a disconnect on the same terms as session seats
   (SESSION_GRACE_MS): a refresh should not tip your armful of driftwood
   onto the sand.
   ================================================================ */

// every one of these mirrors a constant in cove.html and must not drift
// halved from 0.005 on 2026-09-03: a full fire died in 200s while a stick
// round trip measures 40-90s, so the campfire spent most of its life dead.
// ~6.7 minutes now. Mirrors embersCfg.fire.decayRate in cove.html.
const EMBERS_DECAY_RATE = 0.0025;   // fuel per second, while anyone is present
// 0.17 -> 0.20 so RING_CAP (5) sticks is exactly one full fire
const EMBERS_FEED_PER_LOG = 0.20;
const EMBERS_STICK_TARGET = 7;
const EMBERS_RING_CAP = 5;          // sticks one player may carry
const EMBERS_MAX_LOGS = 8;
const EMBERS_BURN_MS = 30000;       // embersCfg.color.burnDur, in ms
const EMBERS_SPAWN_COOLDOWN_MS = 6000;
const EMBERS_RESYNC_MS = 3000;
// Fallback only. Clients report their real SPAWN_POINTS.length in 'join'
// (that array comes off a rejection sampler, so its length is deterministic
// but not self-evident), and the room uses the smallest any client claims —
// a spec can then never name a patch of sand somebody doesn't have.
const EMBERS_SPAWN_POINTS_DEFAULT = 26;

const FUEL_KEYS = ['common', 'salted', 'copper', 'sodium', 'resinous', 'crimson', 'violet', 'verdigris', 'rose'];
const FUEL_WEIGHTS = [0.40, 0.09, 0.08, 0.08, 0.07, 0.09, 0.08, 0.06, 0.05];
function rollFuelType() {
  let r = Math.random(), acc = 0;
  for (let i = 0; i < FUEL_WEIGHTS.length; i++) { acc += FUEL_WEIGHTS[i]; if (r <= acc) return FUEL_KEYS[i]; }
  return 'common';
}

const embers = new Map();   // room -> campfire state

function roomPopulation(room) {
  let n = 0;
  for (const [, p] of players) if (p.room === room && p.color && p.ws.readyState === p.ws.OPEN) n++;
  return n;
}

function newGust(now) {
  const ramp = 1 + Math.random();
  const hold = 2 + Math.random() * 2;
  const decay = 2 + Math.random();
  return {
    at: now, dir: Math.random() * Math.PI * 2, mag: 0.4 + Math.random() * 0.6,
    ramp, hold, decay,
    // the whole arc, plus the calm interval before the next one starts
    endsAt: now + (ramp + hold + decay) * 1000 + (15 + Math.random() * 30) * 1000,
  };
}

function getEmbers(room) {
  let em = embers.get(room);
  if (em) return em;
  const now = Date.now();
  em = {
    fuel: 1, fuelAt: now,
    burnType: 'common', burnUntil: 0,
    logs: EMBERS_MAX_LOGS,
    sticks: [], cooldowns: new Map(), nextStickId: 1,
    spawnPoints: EMBERS_SPAWN_POINTS_DEFAULT,
    gust: newGust(now),
    lastBroadcast: 0,
  };
  embers.set(room, em);
  for (let i = 0; i < EMBERS_STICK_TARGET; i++) spawnStick(em, now);
  return em;
}

// fuel is stored as a sample; this is its value right now. Decay is gated on
// PRESENCE, not on any client's tab visibility — the old client-side rule
// stopped the fire whenever your own tab went to the background, which is
// one of the ways the two fires drifted apart.
function currentFuel(em, now, present) {
  if (present <= 0) return em.fuel;
  return Math.max(0, em.fuel - EMBERS_DECAY_RATE * (now - em.fuelAt) / 1000);
}
// bank the decay that has already happened before changing fuel, or feeding
// the fire would silently erase it
function settleFuel(em, now, present) {
  em.fuel = currentFuel(em, now, present);
  em.fuelAt = now;
}

function spawnStick(em, now) {
  const used = new Set(em.sticks.map((x) => x.spi));
  const free = [];
  for (let i = 0; i < em.spawnPoints; i++) {
    if (used.has(i)) continue;
    if ((em.cooldowns.get(i) || 0) > now) continue;
    free.push(i);
  }
  if (!free.length) return null;
  const s = {
    id: 'S' + (em.nextStickId++),
    spi: free[Math.floor(Math.random() * free.length)],
    type: rollFuelType(),
    // the client seeds its own mulberry32 from this, so these four numbers
    // build a byte-identical stick on every device — which is the whole
    // reason driftwood can be described instead of shipped
    seed: 1 + Math.floor(Math.random() * 0xfffffff),
    by: null, byName: null, heldUntil: null,
  };
  em.sticks.push(s);
  return s;
}

function embersPayload(em, room) {
  const now = Date.now();
  const present = roomPopulation(room);
  return {
    t: 'embers',
    now,
    fuel: currentFuel(em, now, present),
    fuelAt: now,
    decayRate: EMBERS_DECAY_RATE,
    present,
    burnType: em.burnType,
    logs: em.logs,
    gust: { at: em.gust.at, dir: em.gust.dir, mag: em.gust.mag,
            ramp: em.gust.ramp, hold: em.gust.hold, decay: em.gust.decay },
    sticks: em.sticks.map((x) => ({ id: x.id, spi: x.spi, type: x.type, seed: x.seed, by: x.by })),
  };
}

function broadcastEmbers(room) {
  const em = embers.get(room);
  if (!em) return;
  em.lastBroadcast = Date.now();
  broadcast(embersPayload(em, room), null, room);
}

// a disconnect is not a departure — same rule as session seats. The claim
// stays with the name for SESSION_GRACE_MS so a refresh keeps your armful.
function holdClaims(room, playerId) {
  const em = embers.get(room);
  if (!em) return false;
  let changed = false;
  for (const x of em.sticks) {
    if (x.by === playerId && x.heldUntil === null) { x.heldUntil = Date.now() + SESSION_GRACE_MS; changed = true; }
  }
  return changed;
}
// a same-name reconnect adopts its own driftwood back, exactly as it adopts
// its seat — matched on the OLD player record, since the name is all the two
// sockets share
function adoptClaims(room, oldId, newId) {
  const em = embers.get(room);
  if (!em) return false;
  let changed = false;
  for (const x of em.sticks) {
    if (x.by === oldId) { x.by = newId; x.heldUntil = null; changed = true; }
  }
  return changed;
}
/* Adoption by NAME, and it is the path that actually fires most of the time.
   The id-based version above only works when the old socket is still in
   `players` — a fast reconnect that beats its own 'close' event. A normal
   refresh does not: 'close' lands first and deletes the player record, so by
   the time the new socket says hello there is no old id left to match on.
   Exactly the asynchrony the session-seat adoption comment describes, hit
   for real in testing (claims survived the disconnect, then failed to come
   back). Sticks therefore carry the claimant's NAME as well as their id —
   the name is the only thing the two sockets share. */
function adoptClaimsByName(room, nameLower, newId) {
  const em = embers.get(room);
  if (!em) return false;
  let changed = false;
  for (const x of em.sticks) {
    if (x.heldUntil !== null && x.byName && x.byName.toLowerCase() === nameLower) {
      x.by = newId; x.heldUntil = null; changed = true;
    }
  }
  return changed;
}

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
// sessions broadcast to the WHOLE room, including the sender — every
// client (seated or roaming) is a single source of truth from the server
function broadcastSession(room, s) {
  broadcast({ t: 'sessionState', ...publicSession(s) }, null, room);
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
            // driftwood follows the seat: adopt this name's claims onto the
            // new socket before the old one is dropped, or an armful of
            // sticks would sit orphaned until the grace timer swept them
            adoptClaims(me.room, pid, id);
            p.ws.terminate();
            players.delete(pid);
          }
        }

        // session continuity, extending the reconnect logic above rather
        // than duplicating it: if this name already holds a seat in one of
        // this room's activity sessions, adopt that seat under the new id
        // instead of losing its index (the seat) — matched on name
        // regardless of whether that seat is already marked disconnected,
        // because the OLD socket's 'close' event (which is what marks a
        // seat disconnected) fires asynchronously and may not have landed
        // yet when this fast a reconnect arrives. The 'close' handler below
        // only marks a seat held if its id still matches the closing
        // socket's — once adopted here, that id no longer matches, so a
        // later 'close' for the old socket can't re-orphan it.
        const roomSessions = sessions.get(me.room);
        if (roomSessions) {
          for (const [, s] of roomSessions) {
            const held = s.participants.find(
              (p) => p.id !== id && p.name && p.name.toLowerCase() === myNameLower
            );
            if (held) {
              held.id = id;
              held.connected = true;
              held.heldUntil = null;
              broadcastSession(me.room, s);
            }
          }
        }
        // driftwood comes back with the seat, on the same terms and for the
        // same reason — a refresh is not a departure, and it should not tip
        // your armful onto the sand
        adoptClaimsByName(me.room, myNameLower, id);

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
        // a freshly connected client gets whatever activity sessions already
        // exist in this room in the SAME payload it learns about other
        // players in — it can decide what to render before its first frame,
        // rather than popping in a beat later once sessionState arrives
        const sessionsOut = roomSessions ? Array.from(roomSessions.values()).map(publicSession) : [];
        // the campfire rides in the same payload for the same reason the
        // sessions do: a late joiner should walk into the fire the other
        // person has been feeding, not pop into it a beat later
        const em = getEmbers(me.room);
        // clients report their own SPAWN_POINTS.length; the room takes the
        // smallest, so no spec can name sand somebody hasn't got
        const claimed = Number(msg.spawnPoints);
        if (Number.isFinite(claimed) && claimed > 0 && Math.floor(claimed) < em.spawnPoints) {
          em.spawnPoints = Math.floor(claimed);
          /* The room's driftwood was laid out before this client turned up
             and said it had fewer patches of sand, so anything now out of
             range has to be re-laid — otherwise it would be invisible to
             the client that just arrived while the other one could see it,
             which is the whole class of bug this pass exists to remove.
             Claimed sticks are left alone: they are in somebody's hands,
             where their spawn point no longer means anything. */
          const stale = em.sticks.filter((k) => k.by === null && k.spi >= em.spawnPoints);
          if (stale.length) {
            em.sticks = em.sticks.filter((k) => !stale.includes(k));
            const now2 = Date.now();
            while (em.sticks.length < EMBERS_STICK_TARGET && spawnStick(em, now2)) { /* refill */ }
          }
        }
        send(ws, { t: 'welcome', id, players: roster, sessions: sessionsOut, now: Date.now(),
                   embers: embersPayload(em, me.room) });

        broadcast({ t: 'joined', id, color: me.color, name: me.name, char: me.char }, id, me.room);
        // presence changed, and presence is the decay gate — everyone needs
        // to re-anchor their fuel line on the same instant
        broadcastEmbers(me.room);
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

      // create-or-join is one synchronous check-then-act with no await
      // between the check and the write, so two 'sessionJoin's arriving in
      // the same tick (Node is single-threaded) can never race into two
      // sessions — the second one always sees the first one's session.
      case 'sessionJoin': {
        if (!me.room) break;
        const activityId = String(msg.activityId || '').slice(0, 30);
        if (!activityId) break;
        const roomSessions = getRoomSessions(me.room);
        let s = roomSessions.get(activityId);
        if (!s) {
          s = { activityId, startedAt: Date.now(), participants: [], shared: {} };
          roomSessions.set(activityId, s);
        }
        if (!s.participants.some((p) => p.id === id)) {
          s.participants.push({ id, name: me.name, connected: true, heldUntil: null });
        }
        broadcastSession(me.room, s);
        break;
      }

      // a participant may be in more than one of this room's activity
      // sessions in principle, so this scans all of them rather than
      // trusting the client to say which — the session that survives is
      // whichever one still has someone left in it
      case 'sessionLeave': {
        if (!me.room) break;
        const roomSessions = sessions.get(me.room);
        if (!roomSessions) break;
        for (const [activityId, s] of roomSessions) {
          const idx = s.participants.findIndex((p) => p.id === id);
          if (idx === -1) continue;
          s.participants.splice(idx, 1);
          broadcastSession(me.room, s);   // let the room see it end/shrink before it's gone
          if (s.participants.length === 0) roomSessions.delete(activityId);
        }
        break;
      }

      // shared-view actions only: only a current participant may write into
      // `shared`, last write wins, and any activity's declared mutual-
      // exclusivity is enforced here so the two clients can't disagree
      case 'sessionAction': {
        if (!me.room) break;
        const activityId = String(msg.activityId || '').slice(0, 30);
        const roomSessions = sessions.get(me.room);
        const s = roomSessions && roomSessions.get(activityId);
        if (!s || !s.participants.some((p) => p.id === id)) break;
        const key = String(msg.key || '').slice(0, 20);
        if (!key) break;
        // thrash guard — a client-side cooldown already throttles clicks,
        // but the server is the one that actually has to agree on it
        if (s.shared.lastAt && Date.now() - s.shared.lastAt < SF_ACTION_COOLDOWN_MS) break;
        const rule = SHARED_RULES[activityId];
        if (rule && rule.exclusive.includes(key) && msg.value) {
          for (const k of rule.exclusive) if (k !== key) s.shared[k] = false;
        }
        s.shared[key] = msg.value;
        s.shared.lastBy = me.name;
        s.shared.lastAt = Date.now();
        broadcastSession(me.room, s);
        break;
      }

      // ---- the campfire ------------------------------------------------
      // A pickup is a REQUEST. The server owns the claim, so two people
      // reaching for the same piece of driftwood resolve to one winner
      // instead of both walking away with a copy of it — which is exactly
      // what used to happen, invisibly, on two separate beaches.
      case 'stickGrab': {
        if (!me.room) break;
        const em = getEmbers(me.room);
        const x = em.sticks.find((k) => k.id === msg.id);
        // already claimed — possibly by the asker, whose grant went missing.
        // Silence is right either way: the next snapshot tells them the truth.
        if (!x || x.by !== null) break;
        let held = 0;
        for (const k of em.sticks) if (k.by === id) held++;
        if (held >= EMBERS_RING_CAP) break;
        x.by = id;
        x.byName = me.name;   // the only handle a reconnect can be matched on
        x.heldUntil = null;
        // don't respawn onto the patch of sand someone just cleared
        em.cooldowns.set(x.spi, Date.now() + EMBERS_SPAWN_COOLDOWN_MS);
        broadcastEmbers(me.room);
        break;
      }

      // one message per stick, sent in the client's own 0.15s deposit
      // rhythm, so the other player watches the fire climb log by log
      // rather than jump once at the end
      case 'embersFeed': {
        if (!me.room) break;
        const em = getEmbers(me.room);
        const now = Date.now();
        const present = roomPopulation(me.room);
        const i = em.sticks.findIndex((k) => k.id === msg.id && k.by === id);
        if (i === -1) break;                    // not yours, or already burnt
        settleFuel(em, now, present);
        if (em.fuel >= 0.995) break;            // full: the stick stays in hand
        const x = em.sticks[i];
        em.sticks.splice(i, 1);
        em.fuel = Math.min(1, em.fuel + EMBERS_FEED_PER_LOG);
        if (em.logs < EMBERS_MAX_LOGS) em.logs++;
        if (x.type !== 'common') { em.burnType = x.type; em.burnUntil = now + EMBERS_BURN_MS; }
        broadcastEmbers(me.room);
        break;
      }
    }
  });

  ws.on('close', () => {
    const me = players.get(id);
    players.delete(id);
    if (!me) return;
    broadcast({ t: 'left', id }, id, me.room);   // no-ops if they never joined (room still null)

    // hold this socket's session seat(s) rather than dropping them — a
    // disconnect is not a departure. Only marks a seat held if it still
    // carries THIS id: if a fast reconnect already adopted it (see the
    // 'join' handler above), this find() simply comes up empty and does
    // nothing, so an already-adopted seat can never be re-orphaned.
    if (me.room) {
      const roomSessions = sessions.get(me.room);
      if (roomSessions) {
        for (const [, s] of roomSessions) {
          const p = s.participants.find((pp) => pp.id === id);
          if (p) {
            p.connected = false;
            p.heldUntil = Date.now() + SESSION_GRACE_MS;
            broadcastSession(me.room, s);
          }
        }
      }
      // their driftwood is held on the same terms, and the room's fuel line
      // has to be re-anchored either way because presence just changed
      holdClaims(me.room, id);
      broadcastEmbers(me.room);
    }
  });
});

// Keepalive — drop dead sockets fast so "ghost" players clear quickly,
// keep live ones warm. (Was 30s; tightened so presence stays accurate.)
// The same 5s tick also sweeps expired held session seats — no new timer.
const heartbeat = setInterval(() => {
  for (const [id, p] of players) {
    if (!p.ws.isAlive) {
      p.ws.terminate();
      players.delete(id);
      broadcast({ t: 'left', id }, id, p.room);
      // a reaped socket never fires 'close', so hold its claims here too —
      // otherwise a dropped phone loses its armful with no grace at all
      if (p.room) { holdClaims(p.room, id); broadcastEmbers(p.room); }
      continue;
    }
    p.ws.isAlive = false;
    p.ws.ping();
  }

  const now = Date.now();
  for (const [room, roomSessions] of sessions) {
    for (const [activityId, s] of roomSessions) {
      let changed = false;
      for (let i = s.participants.length - 1; i >= 0; i--) {
        const p = s.participants[i];
        if (!p.connected && p.heldUntil !== null && now >= p.heldUntil) {
          s.participants.splice(i, 1);
          changed = true;
        }
      }
      if (!changed) continue;
      broadcastSession(room, s);   // let the room see it shrink/end before it's gone
      if (s.participants.length === 0) roomSessions.delete(activityId);
    }
    if (roomSessions.size === 0) sessions.delete(room);
  }
}, 5000);

/* The campfire tick. Slow on purpose: nothing here streams. It only notices
   the things that change on their own — a coloured burn running out, a gust
   ending, an expired claim falling back to the sand, the beach needing
   another piece of driftwood — plus the periodic resync that keeps both
   clients' clocks and fuel lines pinned to the server's. */
const emberTick = setInterval(() => {
  const now = Date.now();
  for (const [room, em] of embers) {
    const present = roomPopulation(room);
    if (present === 0) {
      // nobody here: freeze the fire rather than burning an empty cove down
      // to nothing (locked rule — fuel decays on active time only)
      em.fuel = currentFuel(em, now, 0);
      em.fuelAt = now;
      continue;
    }
    let changed = false;
    if (em.burnType !== 'common' && em.burnUntil > 0 && now > em.burnUntil) {
      em.burnType = 'common'; em.burnUntil = 0; changed = true;
    }
    if (now > em.gust.endsAt) { em.gust = newGust(now); changed = true; }
    // a claim whose grace has run out goes back on the sand where it lay
    for (const x of em.sticks) {
      if (x.by !== null && x.heldUntil !== null && now >= x.heldUntil) {
        x.by = null; x.heldUntil = null; changed = true;
      }
    }
    if (em.sticks.length < EMBERS_STICK_TARGET && spawnStick(em, now)) changed = true;
    if (changed || now - em.lastBroadcast > EMBERS_RESYNC_MS) broadcastEmbers(room);
  }
}, 1000);

wss.on('close', () => { clearInterval(heartbeat); clearInterval(emberTick); });

server.listen(PORT, () => {
  console.log(`Closer relay listening on :${PORT}`);
});
