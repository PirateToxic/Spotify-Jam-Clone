import base64
import json
import time
import uuid
import re
import httpx
from bs4 import BeautifulSoup
from Crypto.Cipher import DES
from Crypto.Util.Padding import unpad
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp

app = FastAPI(title="Spotify Jam Clone API", version="5.0")

# --- PRODUCTION CORS SETUP ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- DECRYPT 320KBPS JIOSAAVN AUDIO STREAM ---
DES_KEY = b"38346591"

def decrypt_saavn_url(encrypted_url: str) -> str:
    try:
        cipher = DES.new(DES_KEY, DES.MODE_ECB)
        enc_bytes = base64.b64decode(encrypted_url.strip())
        dec_bytes = unpad(cipher.decrypt(enc_bytes), DES.block_size)
        url = dec_bytes.decode("utf-8")
        return url.replace("_96.mp4", "_320.mp4").replace("_160.mp4", "_320.mp4")
    except Exception:
        return ""

async def resolve_audio_stream(title: str, artist: str):
    # Try JioSaavn first for crisp 320kbps CDN stream
    query = f"{title} {artist}".strip()
    url = "https://www.jiosaavn.com/api.php"
    params = {
        "__call": "search.getResults",
        "_format": "json",
        "_marker": "0",
        "ctx": "web6dot0",
        "api_version": "4",
        "q": query,
        "n": "1",
        "p": "1"
    }
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            res = await client.get(url, params=params, headers=headers)
            data = res.json()
            results = data.get("results", [])
            if results:
                song = results[0]
                enc_url = song.get("more_info", {}).get("encrypted_media_url", "")
                stream_url = decrypt_saavn_url(enc_url)
                cover = song.get("image", "").replace("150x150", "500x500")
                if stream_url:
                    return {
                        "stream_url": stream_url,
                        "cover": cover,
                        "duration": int(song.get("more_info", {}).get("duration", 0))
                    }
    except Exception as e:
        print(f"[Saavn Resolver Error] {e}")

    # Fallback to yt-dlp search if JioSaavn misses
    try:
        ydl_opts = {'format': 'bestaudio/best', 'noplaylist': True, 'quiet': True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"ytsearch1:{title} {artist}", download=False)
            if 'entries' in info and len(info['entries']) > 0:
                entry = info['entries'][0]
                return {
                    "stream_url": entry.get('url'),
                    "cover": entry.get('thumbnail'),
                    "duration": int(entry.get('duration', 0))
                }
    except Exception as e:
        print(f"[Fallback ytdl Error] {e}")

    return None

# --- PARSE YOUTUBE / YOUTUBE MUSIC URLS ---
def fetch_youtube_metadata(yt_url: str):
    ydl_opts = {
        'extract_flat': 'in_playlist',
        'skip_download': True,
        'quiet': True,
        'no_warnings': True
    }
    tracks = []
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(yt_url, download=False)
            if 'entries' in info:
                for entry in info['entries']:
                    if entry:
                        tracks.append({
                            "title": entry.get('title', 'Unknown Title'),
                            "artist": entry.get('uploader') or entry.get('channel') or 'YouTube Music',
                            "cover": entry.get('thumbnail') or (entry.get('thumbnails', [{}])[-1].get('url', '') if entry.get('thumbnails') else ''),
                            "stream_url": entry.get('url') if 'googlevideo.com' in entry.get('url', '') else ''
                        })
            else:
                stream_url = ""
                formats = info.get('formats', [])
                audio_formats = [f['url'] for f in formats if f.get('acodec') != 'none' and 'url' in f]
                if audio_formats:
                    stream_url = audio_formats[-1]
                elif info.get('url'):
                    stream_url = info.get('url')

                tracks.append({
                    "title": info.get('title', 'Unknown Title'),
                    "artist": info.get('uploader') or info.get('channel') or 'YouTube',
                    "cover": info.get('thumbnail', ''),
                    "stream_url": stream_url
                })
    except Exception as e:
        print(f"[YouTube Parser Error] {e}")
    return tracks

# --- SCRAPE SPOTIFY METADATA ---
async def fetch_spotify_metadata(spotify_url: str):
    clean_url = spotify_url.split("?")[0]
    
    # 1. Safest Approach: Use yt-dlp to extract Spotify Metadata (Handles Playlists & Tracks)
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
                            "cover": entry.get('thumbnail') or ''
                        })
                if tracks:
                    return tracks
            elif info.get('title'):
                return [{
                    "title": info.get('title', 'Unknown Track'),
                    "artist": info.get('track_artist') or info.get('artist') or info.get('uploader') or '',
                    "cover": info.get('thumbnail') or ''
                }]
    except Exception as e:
        print(f"[yt-dlp Spotify Error] {e}")

    # 2. Ultimate Fallback: OpenGraph tags for Single Tracks (100% block-proof)
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
            
            # Description format: "ArtistName · Song · 2019"
            artist = desc.split(" · ")[0] if " · " in desc else "Spotify"
            return [{"title": title, "artist": artist, "cover": cover}]
    except Exception as e:
        print(f"[Spotify OG Scraper Error] {e}")
        
    return []

# --- REAL-TIME JAM ROOM ---
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
            try:
                await client.send_json(message)
            except Exception:
                dead_clients.append(client)
        for d in dead_clients:
            if d in self.clients:
                self.clients.remove(d)

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
        if not next_track.get("stream_url"):
            stream_info = await resolve_audio_stream(next_track["title"], next_track["artist"])
            if stream_info and stream_info.get("stream_url"):
                next_track["stream_url"] = stream_info["stream_url"]
                if stream_info.get("cover") and not next_track.get("cover"):
                    next_track["cover"] = stream_info["cover"]
        
        if next_track.get("stream_url"):
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

@app.post("/api/import-spotify")
async def import_spotify(payload: dict):
    query_or_url = payload.get("url", "").strip()
    added_by = payload.get("added_by", "Guest").strip() or "Guest"
    if not query_or_url:
        return {"status": "error", "message": "No input provided"}

    # 1. Spotify Links
    if "open.spotify.com" in query_or_url:
        tracks = await fetch_spotify_metadata(query_or_url)
    # 2. YouTube & YouTube Music Links
    elif "youtube.com" in query_or_url or "youtu.be" in query_or_url or "music.youtube.com" in query_or_url:
        tracks = fetch_youtube_metadata(query_or_url)
    # 3. Direct Song Name Search / Text
    else:
        tracks = [{"title": query_or_url, "artist": "Search Query", "cover": ""}]

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
    except Exception as e:
        print(f"[WS Error] {e}")
        if websocket in room.clients:
            room.clients.remove(websocket)
