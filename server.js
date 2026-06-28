// Upstox live feed backend
// -------------------------------------------------------------
// What this does:
// 1. You paste today's Upstox access token into a simple web form (or POST it).
// 2. Server opens an authenticated WebSocket to Upstox's market data feed.
// 3. Live ticks (LTP) for NIFTY / BANKNIFTY are broadcast to your frontend
//    terminal over Socket.IO, so the "Live (Upstox API)" toggle gets real data.
//
// You do NOT need to touch this file daily — only paste a new token each
// morning via the /connect endpoint (a tiny HTML form is served at "/").
// -------------------------------------------------------------

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const protobuf = require('protobufjs');
const axios = require('axios');
const path = require('path');

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Allow the frontend artifact (running on a different origin) to call this API
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// ---- Instrument keys (Upstox format) ----
const INSTRUMENT_KEYS = {
  NIFTY: 'NSE_INDEX|Nifty 50',
  BANKNIFTY: 'NSE_INDEX|Nifty Bank',
};

let upstoxSocket = null;
let currentToken = null;
let protoRoot = null;
let reconnectTimer = null;
let latestTicks = { NIFTY: null, BANKNIFTY: null };

async function loadProto() {
  if (protoRoot) return protoRoot;
  const protoPath = path.join(__dirname, 'MarketDataFeed.proto');
  protoRoot = await protobuf.load(protoPath);
  return protoRoot;
}

function decodeFeed(buffer) {
  try {
    const root = protoRoot;
    const FeedResponse = root.lookupType('com.upstox.marketdatafeeder.rpc.proto.FeedResponse');
    const decoded = FeedResponse.decode(buffer);
    return FeedResponse.toObject(decoded);
  } catch (err) {
    console.error('Decode error:', err.message);
    return null;
  }
}

async function connectToUpstox(accessToken) {
  await loadProto();

  const authRes = await axios.get(
    'https://api.upstox.com/v3/feed/market-data-feed/authorize',
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Accept: 'application/json',
      },
    }
  );

  const wsUrl = authRes.data?.data?.authorized_redirect_uri;
  if (!wsUrl) throw new Error('Could not get authorized WebSocket URL from Upstox');

  if (upstoxSocket) {
    try { upstoxSocket.close(); } catch (_) {}
  }

  upstoxSocket = new WebSocket(wsUrl);

  upstoxSocket.on('open', () => {
    console.log('[upstox] WebSocket connected');
    const subscribePayload = {
      guid: 'someguid',
      method: 'sub',
      data: {
        mode: 'ltpc',
        instrumentKeys: Object.values(INSTRUMENT_KEYS),
      },
    };
    upstoxSocket.send(Buffer.from(JSON.stringify(subscribePayload)));
    io.emit('upstox_status', { connected: true });
  });

  upstoxSocket.on('message', (data) => {
    const decoded = decodeFeed(data);
    if (!decoded || !decoded.feeds) return;

    for (const [key, feed] of Object.entries(decoded.feeds)) {
      const symbol = Object.entries(INSTRUMENT_KEYS).find(([, v]) => v === key)?.[0];
      if (!symbol) continue;

      const ltpc = feed.ltpc || feed.ff?.ltpc;
      if (ltpc) {
        latestTicks[symbol] = { price: ltpc.ltp, ts: Date.now() };
        io.emit('tick', {
          symbol,
          price: ltpc.ltp,
          ts: Date.now(),
        });
      }
    }
  });

  upstoxSocket.on('close', () => {
    console.log('[upstox] WebSocket closed');
    io.emit('upstox_status', { connected: false });
  });

  upstoxSocket.on('error', (err) => {
    console.error('[upstox] WebSocket error:', err.message);
    io.emit('upstox_status', { connected: false, error: err.message });
  });

  currentToken = accessToken;
}

app.get('/', (req, res) => {
  res.send(`
    <html>
      <body style="font-family: monospace; background:#0a0a0a; color:#eee; padding:24px;">
        <h2>Upstox Feed — Daily Token</h2>
        <form method="POST" action="/connect">
          <input name="token" placeholder="Paste today's access token"
                 style="width:320px; padding:8px;" />
          <button type="submit" style="padding:8px 16px;">Connect</button>
        </form>
        <p style="color:#888; font-size:13px;">Status: ${upstoxSocket && upstoxSocket.readyState === 1 ? 'Connected ✅' : 'Not connected'}</p>
      </body>
    </html>
  `);
});

app.post('/connect', async (req, res) => {
  const token = req.body.token;
  if (!token) return res.status(400).json({ error: 'token required' });
  try {
    await connectToUpstox(token);
    res.send('Connected. You can close this tab and open your terminal.');
  } catch (err) {
    console.error(err.message);
    res.status(500).send('Failed to connect: ' + err.message);
  }
});

app.post('/api/connect', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'token required' });
  try {
    await connectToUpstox(token);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/status', (req, res) => {
  res.json({ connected: !!(upstoxSocket && upstoxSocket.readyState === 1) });
});

app.get('/api/latest', (req, res) => {
  res.json({
    connected: !!(upstoxSocket && upstoxSocket.readyState === 1),
    ticks: latestTicks,
  });
});

io.on('connection', (socket) => {
  console.log('[frontend] client connected', socket.id);
  socket.emit('upstox_status', { connected: !!(upstoxSocket && upstoxSocket.readyState === 1) });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`Backend running on port ${PORT}`);
});
