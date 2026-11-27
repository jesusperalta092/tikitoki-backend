# APEX PK BRIDGE

Micro-servidor puente en Node.js dedicado exclusivamente a conectar un `@usuario` público de TikTok LIVE (sin contraseñas) con la aplicación móvil **APEX PK** mediante WebSockets en tiempo real.

## Endpoints

- `GET /` — Panel de estado operativo en tiempo real.
- `GET /health` — Verificación rápida de salud para Render (`200 OK`).
- `GET /status` — Métricas de sockets activos, sesiones, estado de Gemini AI y memoria.
- `POST /ai/analyze` — Análisis táctico PK bajo demanda con **Gemini 2.5 Flash** (`GEMINI_API_KEY`).
- `WS /live?username=<tiktok_username>` — Canal WebSocket de eventos normalizados (soporta mensajes entrantes `{"type":"ai_analyze"}`).

## Despliegue en Render (4 pasos)

### 1. Subir el proyecto a GitHub
Crea un repositorio en GitHub y sube el contenido de la carpeta `tiktok-live-bridge/`:

```bash
git init
git add .
git commit -m "feat: initial apex pk bridge"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/apex-pk-bridge.git
git push -u origin main
```

### 2. Crear el Web Service en Render
1. Entra a [dashboard.render.com](https://dashboard.render.com) y selecciona **New + → Web Service** (o **Blueprint** para usar `render.yaml` automáticamente).
2. Conecta tu repositorio de GitHub.
3. Si lo configuras manualmente, verifica estos valores:
   - **Runtime:** `Node`
   - **Build Command:** `npm install`
   - **Start Command:** `npm start`
   - **Health Check Path:** `/health`
   - **Environment Variables:** agrega `GEMINI_API_KEY` con tu clave de Google AI Studio (opcional: `GEMINI_MODEL`, por defecto `gemini-2.5-flash`).

### 3. Obtener la URL `wss://`
Una vez que el despliegue marque **Live**, Render te asignará una URL HTTPS pública, por ejemplo:

```text
https://apex-pk-bridge.onrender.com
```

Tu endpoint WebSocket para pegar en la app Android **APEX PK** será:

```text
wss://apex-pk-bridge.onrender.com/live?username=USUARIO_TIKTOK
```

### 4. Probar la conexión en vivo
Verifica primero el estado HTTP y luego prueba el WebSocket con `wscat` (o desde APEX PK):

```bash
curl https://apex-pk-bridge.onrender.com/health
npx wscat -c "wss://apex-pk-bridge.onrender.com/live?username=USUARIO_EN_VIVO"
```

Si el usuario está transmitiendo, recibirás `status` (`connecting`), `connected` con el `roomId` y los eventos reales (`member`, `chat`, `gift`, `like`, `follow`, `streamEnd`). Si no está al aire, recibirás `liveNotFound`.

