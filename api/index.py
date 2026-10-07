import asyncio
import json
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from upstash_redis.asyncio import Redis

CHANNEL = "board:events"
SNAPSHOT_KEY = "board:strokes"
MAX_STROKES = 10000

redis = Redis.from_env()
local_clients: dict[WebSocket, str] = {}
_listener_task: asyncio.Task | None = None


async def _redis_listener():
    """Один слушатель Redis на весь инстанс функции.
    Получает события из канала и рассылает их локальным клиентам."""
    while True:
        try:
            pubsub = redis.pubsub()
            await pubsub.subscribe(CHANNEL)
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode()
                try:
                    msg = json.loads(data)
                except Exception:
                    continue
                sender = msg.pop("_sender", None)
                payload = json.dumps(msg)
                for ws, sid in list(local_clients.items()):
                    if sid == sender:
                        continue  # автору уже отрисовано локально
                    try:
                        await ws.send_text(payload)
                    except Exception:
                        local_clients.pop(ws, None)
        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"[redis listener] {e}")
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _listener_task
    _listener_task = asyncio.create_task(_redis_listener())
    yield
    if _listener_task:
        _listener_task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return HTMLResponse(INDEX_HTML)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = str(uuid.uuid4())
    local_clients[websocket] = session_id

    try:
        # 1) отправляем снимок текущей доски
        raw = await redis.lrange(SNAPSHOT_KEY, 0, -1) or []
        strokes = []
        for item in raw:
            if isinstance(item, bytes):
                item = item.decode()
            try:
                strokes.append(json.loads(item))
            except Exception:
                pass
        await websocket.send_text(json.dumps({"type": "snapshot", "strokes": strokes}))

        # 2) основной цикл приёма сообщений от клиента
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
            except Exception:
                continue
            msg["_sender"] = session_id
            mtype = msg.get("type")

            if mtype == "stroke":
                stroke = msg.get("stroke")
                if stroke:
                    await redis.rpush(SNAPSHOT_KEY, json.dumps(stroke))
                    await redis.ltrim(SNAPSHOT_KEY, -MAX_STROKES, -1)
            elif mtype == "clear":
                await redis.delete(SNAPSHOT_KEY)

            await redis.publish(CHANNEL, json.dumps(msg))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[ws] {e}")
    finally:
        local_clients.pop(websocket, None)


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>Совместная доска</title>
<style>
  html,body{margin:0;height:100%;overflow:hidden;font-family:system-ui,sans-serif;background:#fafafa}
  canvas{display:block;cursor:crosshair;touch-action:none}
  #bar{position:fixed;top:12px;left:12px;background:#fff;padding:10px 12px;border-radius:10px;
       box-shadow:0 2px 10px rgba(0,0,0,.08);display:flex;gap:10px;align-items:center;z-index:10}
  #bar input[type=range]{width:120px}
  #bar button{padding:6px 12px;border:1px solid #ddd;border-radius:6px;background:#fff;cursor:pointer}
  #bar button:hover{background:#f2f2f2}
  #status{position:fixed;top:12px;right:12px;padding:6px 12px;border-radius:10px;font-size:13px;
          background:#fff;box-shadow:0 2px 10px rgba(0,0,0,.08);z-index:10}
  #status.on{color:#0a0}
  #status.off{color:#c00}
</style>
</head>
<body>
<div id="bar">
  <input type="color" id="color" value="#111111">
  <input type="range" id="width" min="1" max="40" value="4">
  <button id="clear">Очистить</button>
</div>
<div id="status" class="off">Подключение…</div>
<canvas id="c"></canvas>

<script>
const canvas = document.getElementById('c');
const ctx = canvas.getContext('2d');
const colorEl = document.getElementById('color');
const widthEl = document.getElementById('width');
const clearBtn = document.getElementById('clear');
const statusEl = document.getElementById('status');

let ws = null;
let connected = false;
let strokes = [];
let pending = [];          // события, пришедшие до снимка
let gotSnapshot = false;
let drawing = false;
let last = null;

function resize() {
  canvas.width = window.innerWidth;
  canvas.height = window.innerHeight;
  redraw();
}
window.addEventListener('resize', resize);

function drawStroke(s) {
  ctx.strokeStyle = s.color;
  ctx.lineWidth = s.width;
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  ctx.beginPath();
  ctx.moveTo(s.x0, s.y0);
  ctx.lineTo(s.x1, s.y1);
  ctx.stroke();
}

function redraw() {
  ctx.fillStyle = '#fff';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  for (const s of strokes) drawStroke(s);
}

function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(proto + '//' + location.host + '/ws');

  ws.onopen = () => {
    connected = true;
    statusEl.textContent = 'Онлайн';
    statusEl.className = 'on';
  };

  ws.onmessage = (ev) => {
    let msg;
    try { msg = JSON.parse(ev.data); } catch { return; }

    if (msg.type === 'snapshot') {
      strokes = msg.strokes || [];
      gotSnapshot = true;
      for (const s of pending) strokes.push(s);
      pending = [];
      redraw();
    } else if (msg.type === 'stroke') {
      if (!gotSnapshot) {
        pending.push(msg.stroke);
      } else {
        strokes.push(msg.stroke);
        drawStroke(msg.stroke);
      }
    } else if (msg.type === 'clear') {
      strokes = [];
      redraw();
    }
  };

  ws.onclose = () => {
    connected = false;
    gotSnapshot = false;
    pending = [];
    statusEl.textContent = 'Переподключение…';
    statusEl.className = 'off';
    setTimeout(connect, 800);
  };

  ws.onerror = () => ws.close();
}

function send(obj) {
  if (connected && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function startStroke(x, y) { drawing = true; last = { x, y }; }
function endStroke() { drawing = false; last = null; }

function moveStroke(x, y) {
  if (!drawing || !last) return;
  const s = {
    x0: last.x, y0: last.y, x1: x, y1: y,
    color: colorEl.value,
    width: parseInt(widthEl.value, 10) || 4
  };
  strokes.push(s);
  drawStroke(s);
  send({ type: 'stroke', stroke: s });
  last = { x, y };
}

// Мышь
canvas.addEventListener('mousedown', e => startStroke(e.clientX, e.clientY));
canvas.addEventListener('mousemove', e => moveStroke(e.clientX, e.clientY));
window.addEventListener('mouseup', endStroke);
canvas.addEventListener('mouseleave', endStroke);

// Тач
canvas.addEventListener('touchstart', e => {
  e.preventDefault();
  const t = e.touches[0];
  startStroke(t.clientX, t.clientY);
}, { passive: false });
canvas.addEventListener('touchmove', e => {
  e.preventDefault();
  const t = e.touches[0];
  moveStroke(t.clientX, t.clientY);
}, { passive: false });
canvas.addEventListener('touchend', e => { e.preventDefault(); endStroke(); }, { passive: false });

clearBtn.addEventListener('click', () => {
  strokes = [];
  redraw();
  send({ type: 'clear' });
});

resize();
connect();
</script>
</body>
</html>
"""