import http from 'node:http';
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import {
  TikTokLiveConnection,
  WebcastEvent,
  ControlEvent,
  UserOfflineError
} from 'tiktok-live-connector';

const PORT = Number(process.env.PORT) || 3000;
const HEARTBEAT_INTERVAL_MS = 30_000;
const USERNAME_REGEX = /^[a-zA-Z0-9._]{2,24}$/;
const startedAt = Date.now();

/**
 * Structured logger with ISO timestamp and channel tag.
 * @param {'HTTP' | 'WS' | 'TIKTOK' | 'SYS'} channel
 * @param {string} message
 */
function log(channel, message) {
  const ts = new Date().toISOString();
  console.log(`${ts} [${channel}] ${message}`);
}

/**
 * Sanitizes and validates a TikTok username from query params.
 * @param {string | null} rawUsername
 * @returns {string | null}
 */
function normalizeUsername(rawUsername) {
  if (!rawUsername || typeof rawUsername !== 'string') {
    return null;
  }
  const cleaned = rawUsername.trim().replace(/^@+/, '').toLowerCase();
  if (!USERNAME_REGEX.test(cleaned)) {
    return null;
  }
  return cleaned;
}

/**
 * Safely sends a JSON payload over an open WebSocket.
 * @param {WebSocket} ws
 * @param {Record<string, unknown>} payload
 */
function sendJson(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

/**
 * Extracts normalized user identity fields from TikTok proto messages.
 * @param {any} data
 * @returns {{ username: string, nickname: string }}
 */
function extractUser(data) {
  const username = data?.user?.uniqueId || data?.uniqueId || 'unknown';
  const nickname = data?.user?.nickname || data?.nickname || username;
  return { username, nickname };
}

/**
 * Determines whether a connection error indicates the streamer is offline.
 * @param {unknown} err
 * @returns {boolean}
 */
function isOfflineError(err) {
  if (err instanceof UserOfflineError) {
    return true;
  }
  const msg = String(err?.info || err?.message || err || '').toLowerCase();
  return (
    msg.includes('offline') ||
    msg.includes('not live') ||
    msg.includes('room_id') ||
    msg.includes('failed to retrieve room id') ||
    msg.includes('isn\'t online') ||
    msg.includes('not found')
  );
}

/**
 * Active sessions tracked by socket id for status reporting and cleanup.
 * @type {Map<WebSocket, { username: string, connectedAt: number, roomId: string | null, tiktok: TikTokLiveConnection | null }>}
 */
const activeSessions = new Map();

const app = express();
app.disable('x-powered-by');

app.use((req, res, next) => {
  res.on('finish', () => {
    if (req.path !== '/health') {
      log('HTTP', `${req.method} ${req.path} -> ${res.statusCode}`);
    }
  });
  next();
});

app.get('/health', (_req, res) => {
  res.status(200).json({
    status: 'ok',
    service: 'apex-pk-bridge',
    uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
    timestamp: Date.now()
  });
});

app.get('/status', (_req, res) => {
  const sessions = [];
  for (const session of activeSessions.values()) {
    sessions.push({
      username: session.username,
      roomId: session.roomId,
      connectedAt: session.connectedAt
    });
  }

  const mem = process.memoryUsage();
  res.status(200).json({
    status: 'ok',
    service: 'apex-pk-bridge',
    activeSockets: activeSessions.size,
    sessions,
    uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
    memoryRssMb: Math.round((mem.rss / 1024 / 1024) * 10) / 10,
    timestamp: Date.now()
  });
});

app.get('/', (req, res) => {
  const host = req.headers.host || `localhost:${PORT}`;
  const proto = req.headers['x-forwarded-proto'] === 'https' ? 'wss' : 'ws';
  const uptimeSeconds = Math.floor((Date.now() - startedAt) / 1000);
  const wsExample = `${proto}://${host}/live?username=ejemplo_usuario`;

  res.status(200).type('html').send(`<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>APEX PK BRIDGE — Estado Operativo</title>
  <style>
    :root {
      --bg: #0E1116;
      --surface: #151A22;
      --border: #252C38;
      --text: #E6EDF3;
      --muted: #8B949E;
      --accent: #3FB950;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: var(--sans);
      line-height: 1.5;
      padding: 40px 20px;
      display: flex;
      justify-content: center;
    }
    main {
      width: 100%;
      max-width: 680px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 28px;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid var(--border);
      padding-bottom: 16px;
      margin-bottom: 24px;
    }
    h1 {
      font-size: 16px;
      font-weight: 600;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-family: var(--mono);
      font-size: 12px;
      color: var(--accent);
      background: rgba(63, 185, 80, 0.1);
      border: 1px solid rgba(63, 185, 80, 0.3);
      padding: 4px 10px;
      border-radius: 999px;
    }
    .dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--accent);
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
      margin-bottom: 24px;
    }
    .metric {
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 14px;
    }
    .metric-label {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .metric-value {
      font-family: var(--mono);
      font-size: 20px;
      font-weight: 600;
    }
    .section-title {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted);
      margin-bottom: 8px;
    }
    pre {
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 14px;
      font-family: var(--mono);
      font-size: 13px;
      color: var(--text);
      overflow-x: auto;
      margin-bottom: 18px;
    }
    .endpoints {
      font-family: var(--mono);
      font-size: 12px;
      color: var(--muted);
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
    }
    @media (max-width: 540px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>APEX PK BRIDGE</h1>
      <span class="badge"><span class="dot"></span>OPERATIVO</span>
    </header>
    <section class="grid">
      <div class="metric">
        <div class="metric-label">Sockets Activos</div>
        <div class="metric-value">${activeSessions.size}</div>
      </div>
      <div class="metric">
        <div class="metric-label">Uptime</div>
        <div class="metric-value">${uptimeSeconds}s</div>
      </div>
      <div class="metric">
        <div class="metric-label">Canal WS</div>
        <div class="metric-value">/live</div>
      </div>
    </section>
    <div class="section-title">Endpoint WebSocket</div>
    <pre>${wsExample}</pre>
    <div class="endpoints">
      <span>GET /health</span>
      <span>GET /status</span>
      <span>WS /live?username=&lt;tiktok_username&gt;</span>
    </div>
  </main>
</body>
</html>`);
});

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  try {
    const url = new URL(request.url || '/', `http://${request.headers.host || 'localhost'}`);
    if (url.pathname !== '/live') {
      socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request, url);
    });
  } catch (err) {
    log('WS', `Error en upgrade HTTP->WS: ${err?.message || err}`);
    socket.destroy();
  }
});

/**
 * Cleans up a WebSocket session and disconnects its TikTokLiveConnection instance.
 * @param {WebSocket} ws
 */
function cleanupSession(ws) {
  const session = activeSessions.get(ws);
  if (!session) {
    return;
  }

  activeSessions.delete(ws);

  if (session.tiktok) {
    try {
      session.tiktok.removeAllListeners();
      session.tiktok.disconnect();
    } catch (err) {
      log('TIKTOK', `Error al limpiar sesion de @${session.username}: ${err?.message || err}`);
    }
    session.tiktok = null;
  }

  log('WS', `Sesion cerrada para @${session.username} (activos: ${activeSessions.size})`);
}

wss.on('connection', (ws, _request, url) => {
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  const rawUsername = url.searchParams.get('username');
  const username = normalizeUsername(rawUsername);

  if (!username) {
    log('WS', `Conexion rechazada por username invalido: "${rawUsername || ''}"`);
    sendJson(ws, {
      type: 'error',
      username: rawUsername || '',
      reason: 'Parametro "username" invalido o ausente. Usa un usuario publico valido de TikTok.',
      recoverable: false,
      timestamp: Date.now()
    });
    ws.close(1008, 'Invalid username');
    return;
  }

  const session = {
    username,
    connectedAt: Date.now(),
    roomId: null,
    isConnecting: true,
    tiktok: null
  };
  activeSessions.set(ws, session);

  log('WS', `Cliente conectado para @${username} (activos: ${activeSessions.size})`);

  sendJson(ws, {
    type: 'status',
    state: 'connecting',
    username,
    timestamp: Date.now()
  });

  const tiktok = new TikTokLiveConnection(username, {
    processInitialData: false,
    enableExtendedGiftInfo: true
  });
  session.tiktok = tiktok;

  tiktok.on(ControlEvent.CONNECTED, (state) => {
    session.isConnecting = false;
    const roomId = state?.roomId ? String(state.roomId) : null;
    session.roomId = roomId;
    log('TIKTOK', `Enlazado a @${username} (roomId: ${roomId || 'n/a'})`);
    sendJson(ws, {
      type: 'connected',
      username,
      roomId,
      timestamp: Date.now()
    });
  });

  tiktok.on(ControlEvent.DISCONNECTED, () => {
    log('TIKTOK', `Desconectado de @${username}`);
    sendJson(ws, {
      type: 'disconnected',
      username,
      reason: 'Conexion con la sala de TikTok LIVE cerrada.',
      recoverable: true,
      timestamp: Date.now()
    });
  });

  tiktok.on(ControlEvent.ERROR, (err) => {
    if (session.isConnecting) {
      return;
    }
    const reason = err?.info || err?.message || 'Error inesperado en la conexion con TikTok LIVE.';
    log('TIKTOK', `Error en @${username}: ${reason}`);
    sendJson(ws, {
      type: 'error',
      username,
      reason: String(reason),
      recoverable: true,
      timestamp: Date.now()
    });
  });

  tiktok.on(WebcastEvent.STREAM_END, () => {
    log('TIKTOK', `Transmision finalizada para @${username}`);
    sendJson(ws, {
      type: 'streamEnd',
      username,
      timestamp: Date.now()
    });
  });

  tiktok.on(WebcastEvent.MEMBER, (data) => {
    const user = extractUser(data);
    sendJson(ws, {
      type: 'member',
      username: user.username,
      nickname: user.nickname,
      timestamp: Date.now()
    });
  });

  tiktok.on(WebcastEvent.CHAT, (data) => {
    const user = extractUser(data);
    sendJson(ws, {
      type: 'chat',
      username: user.username,
      nickname: user.nickname,
      comment: data?.comment || '',
      timestamp: Date.now()
    });
  });

  tiktok.on(WebcastEvent.GIFT, (data) => {
    const user = extractUser(data);
    const giftType = data?.giftDetails?.giftType ?? data?.gift?.giftType ?? 0;
    const giftName =
      data?.giftDetails?.giftName ||
      data?.extendedGiftInfo?.name ||
      data?.giftName ||
      `Gift #${data?.giftId || 0}`;
    const diamondCount = Number(
      data?.giftDetails?.diamondCount ??
      data?.extendedGiftInfo?.diamond_count ??
      data?.diamondCount ??
      0
    );
    const repeatCount = Number(data?.repeatCount ?? 1);
    const repeatEnd = Boolean(data?.repeatEnd);
    const isStreakActive = giftType === 1 && !repeatEnd;

    sendJson(ws, {
      type: 'gift',
      username: user.username,
      nickname: user.nickname,
      giftId: Number(data?.giftId ?? 0),
      giftName,
      diamondCount,
      repeatCount,
      repeatEnd,
      isStreakActive,
      timestamp: Date.now()
    });
  });

  tiktok.on(WebcastEvent.LIKE, (data) => {
    const user = extractUser(data);
    sendJson(ws, {
      type: 'like',
      username: user.username,
      nickname: user.nickname,
      likeCount: Number(data?.likeCount ?? 1),
      totalLikeCount: Number(data?.totalLikeCount ?? 0),
      timestamp: Date.now()
    });
  });

  tiktok.on(WebcastEvent.FOLLOW, (data) => {
    const user = extractUser(data);
    sendJson(ws, {
      type: 'follow',
      username: user.username,
      nickname: user.nickname,
      timestamp: Date.now()
    });
  });

  ws.on('close', () => {
    cleanupSession(ws);
  });

  ws.on('error', (err) => {
    log('WS', `Error de socket en @${username}: ${err?.message || err}`);
    cleanupSession(ws);
  });

  tiktok.connect().catch((err) => {
    session.isConnecting = false;
    if (!activeSessions.has(ws)) {
      return;
    }

    if (isOfflineError(err)) {
      log('TIKTOK', `Sala no encontrada o sin LIVE activo para @${username}`);
      sendJson(ws, {
        type: 'liveNotFound',
        username,
        message: `El usuario @${username} no tiene una transmision en vivo activa en este momento.`,
        timestamp: Date.now()
      });
    } else {
      const reason = err?.message || 'No se pudo conectar con TikTok LIVE.';
      log('TIKTOK', `Fallo al conectar con @${username}: ${reason}`);
      sendJson(ws, {
        type: 'error',
        username,
        reason: String(reason),
        recoverable: true,
        timestamp: Date.now()
      });
    }

    cleanupSession(ws);
    if (ws.readyState === WebSocket.OPEN) {
      ws.close(1000, 'TikTok session ended');
    }
  });
});

const heartbeatTimer = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      cleanupSession(ws);
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_INTERVAL_MS);

heartbeatTimer.unref();

process.on('unhandledRejection', (reason) => {
  log('SYS', `Promesa no capturada: ${reason?.stack || reason}`);
});

process.on('uncaughtException', (err) => {
  log('SYS', `Excepcion no capturada: ${err?.stack || err}`);
});

function shutdown(signal) {
  log('SYS', `Senal ${signal} recibida. Cerrando sesiones activas...`);
  clearInterval(heartbeatTimer);

  for (const ws of Array.from(activeSessions.keys())) {
    cleanupSession(ws);
    if (ws.readyState === WebSocket.OPEN) {
      ws.close(1001, 'Server shutting down');
    }
  }

  server.close(() => {
    log('SYS', 'Servidor HTTP detenido correctamente.');
    process.exit(0);
  });
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

server.listen(PORT, () => {
  log('HTTP', `APEX PK BRIDGE escuchando en puerto ${PORT}`);
});
