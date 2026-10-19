import json
import time
import uuid
import re
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp

app = FastAPI(title="Spotify Jam Clone API", version="9.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def fetch_spotify_metadata(spotify_url: str):
    clean_url = spotify_url.split("?")[0]
    try:
        ydl_opts = {'extract_flat': 'in_playlist', 'quiet': True, 'no_warnings': True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(clean_url, download=False)
            tracks = []
            if 'entries' in info:
                for entry in info['entries']:
                    if entry:
                        tracks.append({
                            "title": entry.get('title', 'Unknown Track'),
                            "artist": entry.get('track_artist') or entry.get('artist') or entry.get('uploader') or '',
                            "cover": entry.get('thumbnail') or '',
                            "spotify_url": entry.get('url') or clean_url
                        })
                if tracks: return tracks
            elif info.get('title'):
                return [{
                    "title": info.get('title', 'Unknown Track'),
                    "artist": info.get('track_artist') or info.get('artist') or info.get('uploader') or '',
                    "cover": info.get('thumbnail') or '',
                    "spotify_url": info.get('url') or clean_url
                }]
    except Exception as e:
        print(f"[yt-dlp Spotify Error] {e}")

    # Fallback OpenGraph
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            res = await client.get(clean_url, headers=headers)
        soup = BeautifulSoup(res.text, "html.parser")
        title_tag = soup.find("meta", property="og:title")
        desc_tag = soup.find("meta", property="og:description")
        image_tag = soup.find("meta", property="og:image")
        
        if title_tag and desc_tag:
            title = title_tag["content"]
            desc = desc_tag["content"]
            cover = image_tag["content"] if image_tag else ""
            artist = desc.split(" · ")[0] if " · " in desc else "Spotify"
            return [{"title": title, "artist": artist, "cover": cover, "spotify_url": clean_url}]
    except Exception as e:
        print(f"[Spotify OG Scraper Error] {e}")
        
    return []

class JamRoom:
    def __init__(self):
        self.clients = []
        self.queue = []
        self.current_track = None
        self.is_playing = False
        self.seek_position = 0.0
        self.last_updated = time.time()
        self.chat_history = []

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
    if room.queue:
        room.current_track = room.queue.pop(0)
        room.seek_position = 0.0
        room.last_updated = time.time()
        room.is_playing = True
    else:
        room.current_track = None
        room.is_playing = False
        room.seek_position = 0.0

@app.get("/")
async def serve_ui():
    return FileResponse("index.html")

@app.post("/api/import-spotify")
async def import_spotify(payload: dict):
    query_or_url = payload.get("url", "").strip()
    added_by = payload.get("added_by", "Guest").strip() or "Guest"
    if not query_or_url:
        return {"status": "error"}

    if "open.spotify.com" in query_or_url:
        tracks = await fetch_spotify_metadata(query_or_url)
    else:
        # If it's not a Spotify link, we can't use the official Spotify Embed Player.
        return {"status": "error", "message": "Only valid Spotify links are supported in Version 9."}

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
