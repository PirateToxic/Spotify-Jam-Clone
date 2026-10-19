import base64
import json
import time
import uuid
import re
import urllib.parse
import httpx
from bs4 import BeautifulSoup
from Crypto.Cipher import DES
from Crypto.Util.Padding import unpad
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="YouTube Music Jam API", version="13.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DES_KEY = b"38346591"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def decrypt_saavn_url(encrypted_url: str) -> str:
    try:
        cipher = DES.new(DES_KEY, DES.MODE_ECB)
        enc_bytes = base64.b64decode(encrypted_url.strip())
        dec_bytes = unpad(cipher.decrypt(enc_bytes), DES.block_size)
        url = dec_bytes.decode("utf-8")
        return url.replace("_96.mp4", "_320.mp4").replace("_160.mp4", "_320.mp4").replace("http://", "https://")
    except Exception:
        return ""

def extract_youtube_id(url: str) -> str | None:
    patterns = [
        r"(?:v=|/v/|youtu\.be/|/embed/|/shorts/)([a-zA-Z0-9_-]{11})",
        r"^([a-zA-Z0-9_-]{11})$"
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None

def clean_metadata(raw_title: str, raw_artist: str) -> tuple[str, str]:
    # Clean artist channel suffixes
    artist = re.sub(r"(?i)\s*-\s*topic$", "", raw_artist or "").strip()
    artist = re.sub(r"(?i)vevo$", "", artist).strip()
    artist = re.sub(r"(?i)official$", "", artist).strip()

    # Clean noise from video titles
    title = raw_title or ""
    noise_patterns = [
        r"(?i)\(official\s*(music\s*)?(video|audio|lyric\s*video|visualizer)\)",
        r"(?i)\[official\s*(music\s*)?(video|audio|lyric\s*video|visualizer)\]",
        r"(?i)\(lyrics?\)",
        r"(?i)\[lyrics?\]",
        r"(?i)\(audio\)",
        r"(?i)\[audio\]",
        r"(?i)\(visualizer\)",
        r"(?i)\(full\s*video\)",
        r"(?i)\|.*$",
    ]
    for pat in noise_patterns:
        title = re.sub(pat, "", title).strip()

    # If title is formatted as "Artist - Song Title", split it cleanly
    if " - " in title:
        parts = title.split(" - ", 1)
        left, right = parts[0].strip(), parts[1].strip()
        if not artist or artist.lower() in left.lower() or left.lower() in artist.lower():
            artist = left
            title = right

    return title.strip(), artist.strip()

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

async def search_youtube_id(query: str) -> dict | None:
    try:
        search_url = f"https://www.youtube.com/results?search_query={urllib.parse.quote(query)}"
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            res = await client.get(search_url, headers=HEADERS)
            m = re.search(r'"videoId":"([a-zA-Z0-9_-]{11})"', res.text)
            if m:
                vid = m.group(1)
                oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={vid}&format=json"
                ores = await client.get(oembed_url, headers=HEADERS)
                if ores.status_code == 200:
                    odata = ores.json()
                    c_title, c_artist = clean_metadata(odata.get("title", query), odata.get("author_name", ""))
                    return {
                        "yt_id": vid,
                        "title": c_title or query,
                        "artist": c_artist or odata.get("author_name", "YouTube Music"),
                        "cover": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
                    }
                return {
                    "yt_id": vid,
                    "title": query,
                    "artist": "YouTube Music",
                    "cover": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
                }
    except Exception as e:
        print(f"[YT Search Error] {e}")
    return None

async def fetch_youtube_metadata(yt_url: str) -> list[dict]:
    vid = extract_youtube_id(yt_url)
    if vid:
        oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={vid}&format=json"
        try:
            async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
                res = await client.get(oembed_url, headers=HEADERS)
                if res.status_code == 200:
                    data = res.json()
                    raw_title = data.get("title", "YouTube Track")
                    raw_author = data.get("author_name", "YouTube Music")
                    c_title, c_artist = clean_metadata(raw_title, raw_author)
                    return [{
                        "title": c_title or raw_title,
                        "artist": c_artist or raw_author,
                        "raw_title": raw_title,
                        "cover": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
                        "yt_id": vid
                    }]
        except Exception as e:
            print(f"[oEmbed Error] {e}")

        # Even if oEmbed is unreachable, never fail when we have a valid video ID!
        return [{
            "title": "YouTube Track",
            "artist": "YouTube Music",
            "cover": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
            "yt_id": vid
        }]

    # Fallback: search YouTube if it's a playlist/custom link without direct v=
    found = await search_youtube_id(yt_url)
    if found:
        return [found]
    return []

async def fetch_spotify_metadata(spotify_url: str) -> list[dict]:
    clean_url = spotify_url.split("?")[0]
    try:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            # 1. Try Spotify's public oEmbed API (Never blocks cloud IPs)
            oembed = await client.get(f"https://open.spotify.com/oembed?url={clean_url}", headers=HEADERS)
            title = ""
            cover = ""
            if oembed.status_code == 200:
                odata = oembed.json()
                title = odata.get("title", "")
                cover = odata.get("thumbnail_url", "")

            # 2. Try OpenGraph meta tags for artist name
            page = await client.get(clean_url, headers={"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"})
            soup = BeautifulSoup(page.text, "html.parser")
            og_title = soup.find("meta", property="og:title")
            og_desc = soup.find("meta", property="og:description")
            og_img = soup.find("meta", property="og:image")

            if og_title and not title:
                title = og_title.get("content", "")
            if og_img and not cover:
                cover = og_img.get("content", "")
            artist = "Spotify"
            if og_desc:
                desc = og_desc.get("content", "")
                if " · " in desc:
                    artist = desc.split(" · ")[0].strip()

            if title:
                return [{"title": title, "artist": artist, "cover": cover}]
    except Exception as e:
        print(f"[Spotify Metadata Error] {e}")
    return []

async def resolve_audio_stream(track_data: dict) -> dict:
    title = track_data.get("title", "").strip()
    artist = track_data.get("artist", "").strip()
    yt_id = track_data.get("yt_id")

    # If we don't have a yt_id yet (e.g., text search or Spotify link), find its YouTube ID first
    if not yt_id and title:
        q = f"{title} {artist}".strip() if artist not in ["Search", "YouTube Music", "Spotify"] else title
        yt_info = await search_youtube_id(q)
        if yt_info:
            yt_id = yt_info.get("yt_id")
            track_data["yt_id"] = yt_id
            if title == track_data.get("raw_query") or artist == "Search":
                title = yt_info.get("title", title)
                artist = yt_info.get("artist", artist)
                track_data["title"] = title
                track_data["artist"] = artist
            if not track_data.get("cover") or "default-cover" in track_data.get("cover", ""):
                track_data["cover"] = yt_info.get("cover", "")

    # 1. Try JioSaavn Smart 320kbps Search (Top 10 results with strict scoring)
    search_q = f"{title} {artist}".strip() if artist not in ["Search", "YouTube Music", "Spotify"] else title
    saavn_url = "https://www.jiosaavn.com/api.php"
    params = {
        "__call": "search.getResults",
        "_format": "json",
        "_marker": "0",
        "ctx": "web6dot0",
        "api_version": "4",
        "q": search_q,
        "n": "10",
        "p": "1"
    }
    try:
        async with httpx.AsyncClient(timeout=7.0) as client:
            res = await client.get(saavn_url, params=params, headers=HEADERS)
            data = res.json()
            results = data.get("results", [])

            exp_t = norm(title)
            exp_a = norm(artist.split(",")[0]) if artist not in ["Search", "YouTube Music", "Spotify"] else ""
            bad_words = ["remix", "lofi", "slowed", "reverb", "karaoke", "instrumental", "cover", "8d", "mashup"]
            user_wants_bad = any(bw in (title + " " + artist).lower() for bw in bad_words)

            best_song = None
            best_score = -1

            for song in results:
                s_title_raw = BeautifulSoup(song.get("title", ""), "html.parser").text
                s_sub_raw = BeautifulSoup(song.get("subtitle", ""), "html.parser").text
                s_title = norm(s_title_raw)
                s_sub = norm(s_sub_raw)

                if not s_title:
                    continue

                # Skip remixes/covers/lofi unless user explicitly searched for them
                if not user_wants_bad and any(bw in s_title_raw.lower() for bw in bad_words):
                    continue

                score = 0
                if exp_t and (exp_t == s_title):
                    score += 60
                elif exp_t and (exp_t in s_title or s_title in exp_t):
                    score += 35

                if exp_a and len(exp_a) > 2:
                    if exp_a in s_sub or exp_a in s_title:
                        score += 40

                if score > best_score:
                    best_score = score
                    best_song = song

            # Require a strong match if we also have a YouTube ID fallback, or accept best match for text search
            min_threshold = 35 if yt_id else 20
            if best_song and best_score >= min_threshold:
                enc_url = best_song.get("more_info", {}).get("encrypted_media_url", "")
                stream_url = decrypt_saavn_url(enc_url)
                if stream_url:
                    cover = best_song.get("image", "").replace("150x150", "500x500")
                    return {
                        "stream_url": stream_url,
                        "yt_id": yt_id,
                        "cover": cover or track_data.get("cover"),
                        "title": BeautifulSoup(best_song.get("title", title), "html.parser").text if artist == "Search" else title,
                        "artist": BeautifulSoup(best_song.get("subtitle", artist), "html.parser").text.split("-")[0].strip() if artist == "Search" else artist
                    }
    except Exception as e:
        print(f"[Saavn Smart Matcher Error] {e}")

    # 2. If JioSaavn didn't have an exact match, use YouTube IFrame Player ID directly!
    if yt_id:
        return {
            "stream_url": f"yt:{yt_id}",
            "yt_id": yt_id,
            "cover": track_data.get("cover") or f"https://i.ytimg.com/vi/{yt_id}/hqdefault.jpg",
            "title": title,
            "artist": artist
        }

    return {}

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
        stream_info = await resolve_audio_stream(next_track)
        if stream_info and stream_info.get("stream_url"):
            next_track.update(stream_info)
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
@app.post("/api/import-spotify")
async def import_music(request: Request, payload: dict):
    query_or_url = payload.get("url", "").strip()
    added_by = payload.get("added_by", "Guest").strip() or "Guest"

    if not query_or_url:
        return {"status": "error", "message": "Please enter a link or song name."}

    if "youtube.com" in query_or_url or "youtu.be" in query_or_url:
        tracks = await fetch_youtube_metadata(query_or_url)
    elif "open.spotify.com" in query_or_url:
        tracks = await fetch_spotify_metadata(query_or_url)
    else:
        tracks = [{
            "title": query_or_url,
            "raw_query": query_or_url,
            "artist": "Search",
            "cover": "https://i.postimg.cc/mZhG7G7z/default-cover.png"
        }]

    if not tracks:
        return {"status": "error", "message": "Could not find any tracks for that input."}

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
