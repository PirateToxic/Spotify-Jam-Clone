import time
import uuid
import re
import urllib.parse
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp

app = FastAPI(title="YouTube Music Jam API", version="10.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- PROXY ENDPOINT FOR YOUTUBE AUDIO (BYPASS 403 IP BLOCK) ---
@app.get("/api/stream")
async def proxy_stream(request: Request, url: str):
    target_url = urllib.parse.unquote(url)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    if "range" in request.headers:
        headers["Range"] = request.headers["range"]

    client = httpx.AsyncClient()
    req = client.build_request("GET", target_url, headers=headers)
    res = await client.send(req, stream=True)

    async def stream_generator():
        async for chunk in res.aiter_bytes(8192):
            yield chunk
        await client.aclose()

    response = StreamingResponse(stream_generator(), status_code=res.status_code)
    for k, v in res.headers.items():
        if k.lower() in ["content-type", "content-length", "content-range", "accept-ranges"]:
            response.headers[k] = v
    return response

async def resolve_audio_stream(title: str, artist: str):
    clean_title = re.sub(r'\(.*?\)|\[.*?\]', '', title).strip() 
    clean_artist = artist.split(',')[0].strip()
    
    try:
        # Search specifically on YouTube Music for official audio tracks
        ydl_opts = {'format': 'bestaudio/best', 'noplaylist': True, 'quiet': True, 'no_warnings': True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"ytmsearch1:{clean_title} {clean_artist}", download=False)
            if 'entries' in info and len(info['entries']) > 0:
                entry = info['entries'][0]
                raw_url = entry.get('url')
                if raw_url:
                    proxy_url = f"{room.base_url}/api/stream?url={urllib.parse.quote(raw_url)}"
                    return {
                        "stream_url": proxy_url,
                        "cover": entry.get('thumbnail'),
                        "duration": int(entry.get('duration', 0))
                    }
    except Exception as e:
        print(f"[YTM Resolver Error] {e}")

    return None

def fetch_youtube_metadata(yt_url: str):
    ydl_opts = {'extract_flat': 'in_playlist', 'skip_download': True, 'quiet': True}
    tracks = []
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(yt_url, download=False)
            if 'entries' in info:
                for entry in info['entries']:
                    if entry:
                        tracks.append({
                            "title": entry.get('title', 'Unknown Title'),
                            "artist": entry.get('uploader') or entry.get('channel') or 'YouTube',
                            "cover": entry.get('thumbnail') or ''
                        })
            else:
                tracks.append({
                    "title": info.get('title', 'Unknown Title'),
                    "artist": info.get('uploader') or info.get('channel') or 'YouTube',
                    "cover": info.get('thumbnail', '')
                })
    except Exception as e:
        print(f"[YouTube Parser Error] {e}")
    return tracks

class JamRoom:
    def __init__(self):
        self.clients = []
        self.queue = []
        self.current_track = None
        self.is_playing = False
        self.seek_position = 0.0
        self.last_updated = time.time()
        self.chat_history = []
        self.base_url = "http://127.0.0.1:8000"

    def get_current_time(self) -> float:
        if self.is_playing:
            return self.seek_position + (time.time() - self.last_updated)
        return self.seek_position

    async def broadcast(self, message: dict):
        dead_clients = []
        for client in self.clients:
            try: await client.send_json(message)
            except: dead_clients.append(client)
        for d in dead_clients:
            if d in self.clients: self.clients.remove(d)

room = JamRoom()

def get_room_state():
    return {
        "current_track": room.current_track,
        "queue": room.queue,
        "is_playing": room.is_playing,
        "current_time": room.get_current_time(),
        "listeners": len(room.clients),
        "chat_history": room.chat_history[-40:] 
    }

async def play_next_in_queue():
    while room.queue:
        next_track = room.queue.pop(0)
        # Dynamically resolve stream URL so links don't expire
        stream_info = await resolve_audio_stream(next_track["title"], next_track["artist"])
        if stream_info and stream_info.get("stream_url"):
            next_track["stream_url"] = stream_info["stream_url"]
            if stream_info.get("cover") and not next_track.get("cover"):
                next_track["cover"] = stream_info["cover"]
            
            room.current_track = next_track
            room.seek_position = 0.0
            room.last_updated = time.time()
            room.is_playing = True
            return
    room.current_track = None
    room.is_playing = False
    room.seek_position = 0.0

@app.get("/")
async def serve_ui():
    return FileResponse("index.html")

@app.post("/api/import-music")
async def import_music(request: Request, payload: dict):
    room.base_url = str(request.base_url).rstrip('/')
    query_or_url = payload.get("url", "").strip()
    added_by = payload.get("added_by", "Guest").strip() or "Guest"
    
    if not query_or_url:
        return {"status": "error"}

    if "youtube.com" in query_or_url or "youtu.be" in query_or_url:
        tracks = fetch_youtube_metadata(query_or_url)
    else:
        # Just search the exact text
        tracks = [{"title": query_or_url, "artist": "Search", "cover": ""}]

    for t in tracks:
        t["id"] = uuid.uuid4().hex
        t["added_by"] = added_by
        room.queue.append(t)

    if not room.current_track and room.queue:
        await play_next_in_queue()

    await room.broadcast({"type": "STATE_UPDATE", "state": get_room_state()})
    return {"status": "ok", "added": len(tracks)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    room.clients.append(websocket)
    await websocket.send_json({"type": "STATE_UPDATE", "state": get_room_state()})
    await room.broadcast({"type": "STATE_UPDATE", "state": get_room_state()})

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "PLAY":
                room.is_playing = True
                room.seek_position = float(data.get("time", room.get_current_time()))
                room.last_updated = time.time()
            elif action == "PAUSE":
                room.is_playing = False
                room.seek_position = float(data.get("time", room.get_current_time()))
            elif action == "SEEK":
                room.seek_position = float(data.get("time", 0.0))
                room.last_updated = time.time()
            elif action == "NEXT":
                await play_next_in_queue()
            elif action == "REMOVE":
                track_id = data.get("track_id")
                room.queue = [t for t in room.queue if t.get("id") != track_id]
            elif action == "REACTION":
                await room.broadcast({"type": "REACTION", "emoji": data.get("emoji", "❤️")})
                continue
            elif action == "CHAT":
                msg = {"user": data.get("user", "Anonymous"), "text": data.get("text", "")}
                room.chat_history.append(msg)
                await room.broadcast({"type": "CHAT", "message": msg})
                continue
            elif action == "PING":
                continue

            await room.broadcast({"type": "STATE_UPDATE", "state": get_room_state()})
    except WebSocketDisconnect:
        if websocket in room.clients:
            room.clients.remove(websocket)
        await room.broadcast({"type": "STATE_UPDATE", "state": get_room_state()})
