"""
Поиск и скачивание треков с SoundCloud.
"""
import asyncio, logging, re, os
import aiohttp
from typing import Optional

logger = logging.getLogger(__name__)

SC_CLIENT_ID = "iZIs9mchVcX5lhVRyQNGogYH2BTG5W3F"

# Общая сессия для переиспользования
_session: Optional[aiohttp.ClientSession] = None

def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        _session = aiohttp.ClientSession(connector=connector)
    return _session


async def _refresh_client_id() -> Optional[str]:
    try:
        session = _get_session()
        async with session.get("https://soundcloud.com",
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            html = await r.text()
        scripts = re.findall(r'src="(https://a-v2\.sndcdn\.com/assets/[^"]+\.js)"', html)
        for script_url in reversed(scripts[-5:]):
            async with session.get(script_url,
                                   timeout=aiohttp.ClientTimeout(total=8)) as r:
                js = await r.text()
            m = re.search(r'client_id:"([a-zA-Z0-9]+)"', js)
            if m:
                return m.group(1)
    except Exception as e:
        logger.warning(f"Could not refresh client_id: {e}")
    return None


async def search_soundcloud(query: str, limit: int = 8) -> list:
    global SC_CLIENT_ID
    url    = "https://api-v2.soundcloud.com/search/tracks"
    params = {"q": query, "limit": limit, "client_id": SC_CLIENT_ID}
    session = _get_session()

    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 401:
                new_id = await _refresh_client_id()
                if new_id:
                    SC_CLIENT_ID = new_id
                    params["client_id"] = SC_CLIENT_ID
                    async with session.get(url, params=params,
                                           timeout=aiohttp.ClientTimeout(total=6)) as r2:
                        data = await r2.json()
                else:
                    return []
            elif resp.status != 200:
                return []
            else:
                data = await resp.json()
    except Exception as e:
        logger.error(f"SoundCloud search error: {e}")
        return []

    results = []
    for item in data.get("collection", []):
        duration_ms  = item.get("duration", 0)
        duration_sec = duration_ms // 1000
        duration_fmt = f"{duration_sec//60}:{duration_sec%60:02d}" if duration_sec else ""
        track_id     = str(item["id"])
        results.append({
            "track_id":     track_id,
            "title":        item.get("title", "Unknown"),
            "artist":       item.get("user", {}).get("username", "Unknown"),
            "url":          item.get("permalink_url", ""),
            "duration_sec": duration_sec,
            "duration_fmt": duration_fmt,
            "artwork_url":  item.get("artwork_url", ""),
            "_sc_id":       item["id"],
        })
    return results


async def _get_stream_url(sc_id: int) -> Optional[str]:
    url     = f"https://api-v2.soundcloud.com/tracks/{sc_id}/streams"
    params  = {"client_id": SC_CLIENT_ID}
    session = _get_session()
    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
        return (
            data.get("http_mp3_128_url") or
            data.get("hls_mp3_128_url") or
            data.get("preview_mp3_128_url")
        )
    except Exception as e:
        logger.error(f"Stream URL error: {e}")
        return None


async def download_track(track: dict) -> Optional[bytes]:
    sc_id = track.get("_sc_id") or track.get("sc_id")
    if not sc_id:
        # Ищем в БД
        data_dir = os.getenv("DATA_DIR", "/app/data")
        db_path  = os.path.join(data_dir, "library.db")
        try:
            from database import Database
            saved = Database(db_path).get_track(track.get("track_id",""))
            if saved:
                sc_id = saved.get("_sc_id") or saved.get("sc_id")
        except Exception:
            pass

    # Способ 1: SoundCloud Streams API
    if sc_id:
        stream_url = await _get_stream_url(int(sc_id))
        if stream_url:
            data = await _download_bytes(stream_url)
            if data:
                return data

    # Способ 2: yt-dlp
    if track.get("url"):
        return await _ytdlp_download(track["url"])

    return None


async def _download_bytes(url: str) -> Optional[bytes]:
    session = _get_session()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=90)) as resp:
            if resp.status == 200:
                return await resp.read()
    except Exception as e:
        logger.error(f"Download error: {e}")
    return None


async def _ytdlp_download(url: str) -> Optional[bytes]:
    try:
        import yt_dlp, tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            out  = os.path.join(tmpdir, "track.%(ext)s")
            opts = {
                "format": "bestaudio/best",
                "outtmpl": out,
                "quiet": True,
                "no_warnings": True,
                "postprocessors": [{"key": "FFmpegExtractAudio",
                                    "preferredcodec": "mp3",
                                    "preferredquality": "128"}],
            }
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: yt_dlp.YoutubeDL(opts).__enter__().download([url])
            )
            for fname in os.listdir(tmpdir):
                with open(os.path.join(tmpdir, fname), "rb") as f:
                    return f.read()
    except ImportError:
        logger.warning("yt-dlp not installed: pip install yt-dlp")
    except Exception as e:
        logger.error(f"yt-dlp error: {e}")
    return None
