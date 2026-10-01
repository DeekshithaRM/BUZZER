const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const HOST_KEY = process.env.HOST_KEY || 'apti';
const PORT = process.env.PORT || 3000;

app.use(express.static(__dirname + '/public'));

let order = [];  // [{ team, ts }] in server-arrival order (server clock only)
let teams = {};  // n -> { token, sid }

function snapshot() {
  const status = {};
  for (let n = 1; n <= 10; n++) {
    const t = teams[n];
    status[n] = !t ? 'free' : t.sid ? 'on' : 'off';
  }
  return { order, status };
}
const broadcast = () => io.emit('order_update', snapshot());

io.on('connection', (socket) => {
  socket.emit('order_update', snapshot());

  socket.on('join', ({ team, token } = {}, ack) => {
    if (typeof ack !== 'function') return;
    team = parseInt(team, 10);
    if (!(team >= 1 && team <= 10) || !token) return ack({ ok: false, msg: 'Invalid team' });
    const t = teams[team];
    if (t && t.token !== token) return ack({ ok: false, msg: `Team ${team} is already taken` });
    teams[team] = { token, sid: socket.id };
    socket.team = team;
    ack({ ok: true, team });
    broadcast();
  });

  socket.on('buzz', () => {
    const n = socket.team;
    if (!n || order.some((o) => o.team === n)) return;
    order.push({ team: n, ts: Date.now() });
    broadcast();
  });

  socket.on('host_auth', (key, ack) => typeof ack === 'function' && ack(key === HOST_KEY));

  socket.on('reset', (key) => {
    if (key !== HOST_KEY) return;
    order = [];
    broadcast();
  });

  socket.on('kick', ({ team, key } = {}) => {
    if (key !== HOST_KEY || !teams[team]) return;
    if (teams[team].sid) io.to(teams[team].sid).emit('kicked');
    delete teams[team];
    order = order.filter((o) => o.team !== team);
    broadcast();
  });

  socket.on('disconnect', () => {
    const n = socket.team;
    if (n && teams[n] && teams[n].sid === socket.id) {
      teams[n].sid = null; // keep reservation so the same phone can reconnect
      broadcast();
    }
  });
});

server.listen(PORT, () => console.log(`BUZZER running on port ${PORT}`));