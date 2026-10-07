import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from upstash_redis.asyncio import Redis

CHANNEL = "board:events"
SNAPSHOT_KEY = "board:strokes"
MAX_STROKES = 10000

redis = Redis.from_env()
local_clients: dict = {}
_listener_task = None


async def _redis_listener():
    """Фоновый слушатель Redis: получает события из канала и рассылает локальным клиентам."""
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
                        continue
                    try:
                        await ws.send_text(payload)
                    except Exception:
                        local_clients.pop(ws, None)
        except asyncio.CancelledError:
            return
        except Exception as e:
            print("[redis listener]", e)
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
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()
    return HTMLResponse(html)


@app.websocket("/api/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = str(uuid.uuid4())
    local_clients[websocket] = session_id

    try:
        # 1) снимок текущей доски
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

        # 2) основной цикл
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
        print("[ws]", e)
    finally:
        local_clients.pop(websocket, None)


# Для локального запуска
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)