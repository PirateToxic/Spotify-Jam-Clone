import json
import time
import uuid
import re
import urllib.parse
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="YouTube Music Jam API", version="14.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

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

async def get_oembed_info(vid: str) -> dict:
    oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={vid}&format=json"
    try:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            res = await client.get(oembed_url, headers=HEADERS)
            if res.status_code == 200:
                data = res.json()
                author = re.sub(r"(?i)\s*-\s*topic$", "", data.get("author_name", "YouTube Music")).strip()
                return {
                    "yt_id": vid,
                    "stream_url": f"yt:{vid}",
                    "title": data.get("title", "YouTube Track"),
                    "artist": author or "YouTube Music",
                    "cover": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
                }
    except Exception as e:
        print(f"[oEmbed Error] {e}")

    return {
        "yt_id": vid,
        "stream_url": f"yt:{vid}",
        "title": "YouTube Track",
        "artist": "YouTube Music",
        "cover": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
    }

async def search_youtube_video(query: str, exclude_ids: list[str] | None = None) -> dict | None:
    exclude_ids = exclude_ids or []
    try:
        search_url = f"https://www.youtube.com/results?search_query={urllib.parse.quote(query)}"
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            res = await client.get(search_url, headers=HEADERS)
            all_ids = re.findall(r'"videoId":"([a-zA-Z0-9_-]{11})"', res.text)
            seen = set()
            candidates = []
            for vid in all_ids:
                if vid not in seen and vid not in exclude_ids:
                    seen.add(vid)
                    candidates.append(vid)

            # Check candidates via oEmbed (oEmbed only returns 200 OK for embeddable public videos!)
            for vid in candidates[:5]:
                oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={vid}&format=json"
                ores = await client.get(oembed_url, headers=HEADERS)
                if ores.status_code == 200:
                    odata = ores.json()
                    author = re.sub(r"(?i)\s*-\s*topic$", "", odata.get("author_name", "YouTube Music")).strip()
                    return {
                        "yt_id": vid,
                        "stream_url": f"yt:{vid}",
                        "title": odata.get("title", query),
                        "artist": author or "YouTube Music",
                        "cover": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
                    }

            if candidates:
                return await get_oembed_info(candidates[0])
    except Exception as e:
        print(f"[YT Search Error] {e}")
    return None

async def fetch_youtube_metadata(yt_url: str) -> list[dict]:
    # 1. If the link has a direct video ID (watch?v=..., music.youtube.com/watch?v=..., youtu.be/...),
    # use that EXACT video ID without re-searching!
    vid = extract_youtube_id(yt_url)
    if vid:
        info = await get_oembed_info(vid)
        return [info]

    # 2. If it's a playlist link without v=, extract video IDs from the playlist page
    if "list=" in yt_url:
        try:
            clean_list_url = yt_url.replace("music.youtube.com", "www.youtube.com")
            async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
                res = await client.get(clean_list_url, headers=HEADERS)
                vids = []
                for m in re.findall(r'"playlistVideoRenderer":\{"videoId":"([a-zA-Z0-9_-]{11})"', res.text):
                    if m not in vids:
                        vids.append(m)
                if not vids:
                    for m in re.findall(r'"videoId":"([a-zA-Z0-9_-]{11})"', res.text):
                        if m not in vids:
                            vids.append(m)
                tracks = []
                for v in vids[:25]:
                    tracks.append(await get_oembed_info(v))
                if tracks:
                    return tracks
        except Exception as e:
            print(f"[YT Playlist Error] {e}")

    found = await search_youtube_video(yt_url)
    return [found] if found else []

async def fetch_spotify_metadata(spotify_url: str) -> list[dict]:
    clean_url = spotify_url.split("?")[0]
    embed_url = clean_url.replace("open.spotify.com/", "open.spotify.com/embed/")
    tracks = []

    try:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            # 1. Try Spotify Embed page JSON (works great for tracks, albums, and playlists)
            er = await client.get(embed_url, headers=HEADERS)
            if er.status_code == 200:
                esoup = BeautifulSoup(er.text, "html.parser")
                ndata = esoup.find("script", {"id": "__NEXT_DATA__"})
                if ndata and ndata.string:
                    jdata = json.loads(ndata.string)
                    entity = jdata.get("props", {}).get("pageProps", {}).get("state", {}).get("data", {}).get("entity", {})
                    if entity.get("type") == "track":
                        t_name = entity.get("name") or entity.get("title")
                        artists = ", ".join([a.get("name", "") for a in entity.get("artists", []) if a.get("name")])
                        if t_name:
                            tracks.append({"title": t_name, "artist": artists})
                    elif entity.get("trackList"):
                        for item in entity.get("trackList", [])[:25]:
                            t_name = item.get("title")
                            t_art = item.get("subtitle", "")
                            if t_name:
                                tracks.append({"title": t_name, "artist": t_art})

            # 2. Fallback to Spotify oEmbed + OpenGraph if embed JSON wasn't present
            if not tracks:
                title = ""
                artist = ""
                oembed = await client.get(f"https://open.spotify.com/oembed?url={clean_url}", headers=HEADERS)
                if oembed.status_code == 200:
                    odata = oembed.json()
                    title = odata.get("title", "")

                page = await client.get(clean_url, headers={"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"})
                soup = BeautifulSoup(page.text, "html.parser")
                og_title = soup.find("meta", property="og:title")
                og_desc = soup.find("meta", property="og:description")
                page_title = soup.find("title")

                if og_title and not title:
                    title = og_title.get("content", "")
                if og_desc:
                    desc = og_desc.get("content", "")
                    if " · " in desc:
                        artist = desc.split(" · ")[0].replace("Listen to " + title + " on Spotify.", "").strip()
                if not artist and page_title and " by " in page_title.text:
                    artist = page_title.text.split(" by ")[-1].split("|")[0].strip()

                if title:
                    tracks.append({"title": title, "artist": artist})
    except Exception as e:
        print(f"[Spotify Metadata Error] {e}")

    return tracks

async def resolve_youtube_track(track_data: dict) -> dict:
    # If it already has a direct YouTube video ID, return it immediately (100% exact video!)
    if track_data.get("yt_id"):
        vid = track_data["yt_id"]
        if track_data.get("title") in [None, "", "YouTube Track"]:
            info = await get_oembed_info(vid)
            track_data.update(info)
        else:
            track_data["stream_url"] = f"yt:{vid}"
        return track_data

    # Otherwise, it came from a Spotify link or text search: find the exact official YouTube video
    title = track_data.get("title", "").strip()
    artist = track_data.get("artist", "").strip()
    search_q = f"{title} {artist}".strip() if artist and artist not in ["Search", "Spotify"] else title
    yt_info = await search_youtube_video(search_q)
    if yt_info:
        track_data.update(yt_info)
        return track_data
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
        resolved = await resolve_youtube_track(next_track)
        if resolved and resolved.get("yt_id"):
            room.current_track = resolved
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
            "artist": "Search",
            "cover": "https://i.postimg.cc/mZhG7G7z/default-cover.png"
        }]

    if not tracks:
        return {"status": "error", "message": "Could not find any tracks for that link."}

    # Resolve the very first track immediately if it's a single track so queue shows real title
    if len(tracks) == 1 and not tracks[0].get("yt_id"):
        resolved = await resolve_youtube_track(tracks[0])
        if resolved:
            tracks[0] = resolved

    for t in tracks:
        t["id"] = uuid.uuid4().hex
        t["added_by"] = added_by
        room.queue.append(t)

    if not room.current_track and room.queue:
        await play_next_in_queue()

    await room.broadcast({"type": "STATE_UPDATE", "state": get_room_state()})
    return {"status": "ok", "added": len(tracks)}

@app.post("/api/fallback-video")
async def fallback_video(payload: dict):
    # Called automatically if a record label blocks embedding on a specific video (Error 101/150)
    if not room.current_track:
        return {"status": "none"}
    bad_id = payload.get("bad_id", "")
    curr = room.current_track
    if curr.get("yt_id") != bad_id:
        return {"status": "already_updated"}

    excludes = curr.get("tried_ids", []) + [bad_id]
    curr["tried_ids"] = excludes
    query = f"{curr.get('title', '')} {curr.get('artist', '')} audio"
    alt = await search_youtube_video(query, exclude_ids=excludes)
    if alt and alt.get("yt_id"):
        curr["yt_id"] = alt["yt_id"]
        curr["stream_url"] = f"yt:{alt['yt_id']}"
        curr["cover"] = alt.get("cover", curr.get("cover"))
        room.seek_position = 0.0
        room.last_updated = time.time()
        room.is_playing = True
        await room.broadcast({"type": "STATE_UPDATE", "state": get_room_state()})
        return {"status": "ok", "new_id": alt["yt_id"]}
    return {"status": "error"}

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
