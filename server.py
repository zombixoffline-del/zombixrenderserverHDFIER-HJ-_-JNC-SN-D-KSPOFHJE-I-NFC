import eventlet
eventlet.monkey_patch()

from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
import time
import os
import threading

app = Flask(__name__)
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="eventlet",
    ping_timeout=60,
    ping_interval=25,
    max_http_buffer_size=10 * 1024 * 1024,
)

players = {}
players_lock = threading.Lock()

# Игрок считается отключённым, если не присылал state > STALE_TIMEOUT секунд
STALE_TIMEOUT = 60


def public_player(p: dict) -> dict:
    """Убираем служебные поля перед отправкой клиентам."""
    return {k: v for k, v in p.items() if k != "last_seen"}


def cleanup_loop():
    """Фоновый поток: выкидывает игроков, которые давно не слали state."""
    while True:
        eventlet.sleep(15)
        now = time.time()
        stale = []
        with players_lock:
            for sid, p in list(players.items()):
                if now - p.get("last_seen", now) > STALE_TIMEOUT:
                    stale.append(sid)
            for sid in stale:
                players.pop(sid, None)
        for sid in stale:
            socketio.emit("player_left", {"sid": sid})
        if stale:
            print(f"[cleanup] removed {len(stale)} stale players | total: {len(players)}")


@socketio.on("connect")
def on_connect():
    print(f"[+] connect: {request.sid} | total: {len(players)}")


@socketio.on("register")
def handle_register(data):
    name = str(data.get("name", "Player"))[:32]
    with players_lock:
        players[request.sid] = {
            "x": 0, "y": 0, "dir": "down", "anim": "idle",
            "loc": "home", "char": "fighter", "bike": False,
            "rolling": False, "pet": None,
            "name": name,
            "last_seen": time.time(),
        }
        new_data = public_player(players[request.sid])
        others = {sid: public_player(p) for sid, p in players.items() if sid != request.sid}

    # Новому игроку — список уже подключённых
    emit("all_players", others)
    # Остальным — сообщаем о новом игроке
    emit("player_update", {"sid": request.sid, **new_data}, broadcast=True, include_self=False)
    print(f"[reg] {name} | total: {len(players)}")


@socketio.on("state")
def handle_state(data):
    with players_lock:
        if request.sid not in players:
            return
        p = players[request.sid]
        p["x"] = int(data.get("x", 0))
        p["y"] = int(data.get("y", 0))
        p["dir"] = data.get("dir", "down")
        p["anim"] = data.get("anim", "idle")
        p["loc"] = data.get("loc", "home")
        p["char"] = data.get("char", "fighter")
        p["bike"] = bool(data.get("bike", False))
        p["rolling"] = bool(data.get("rolling", False))
        p["pet"] = data.get("pet", None)
        p["last_seen"] = time.time()
        out = {"sid": request.sid, **public_player(p)}

    emit("player_update", out, broadcast=True, include_self=False)


@socketio.on("chat")
def handle_chat(data):
    with players_lock:
        if request.sid not in players:
            return
        name = players[request.sid].get("name", "Player")

    msg = str(data.get("text", ""))[:200]
    if not msg.strip():
        return

    payload = {
        "sid": request.sid,
        "name": name,
        "text": msg,
        "ts": time.time(),
    }
    emit("chat", payload, broadcast=True)


@socketio.on("disconnect")
def on_disconnect():
    with players_lock:
        players.pop(request.sid, None)
        total = len(players)
    emit("player_left", {"sid": request.sid}, broadcast=True)
    print(f"[-] disconnect: {request.sid} | total: {total}")


@socketio.on("ping_alive")
def handle_ping():
    with players_lock:
        if request.sid in players:
            players[request.sid]["last_seen"] = time.time()


@app.route("/")
def index():
    return jsonify({
        "status": "ok",
        "service": "Zombix Server",
        "players_online": len(players),
    })


@app.route("/health")
def health():
    return jsonify({"ok": True, "players": len(players)})


# Запускаем фоновую уборку «мёртвых» игроков
threading.Thread(target=cleanup_loop, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"Server starting on port {port}")
    socketio.run(app, host="0.0.0.0", port=port)