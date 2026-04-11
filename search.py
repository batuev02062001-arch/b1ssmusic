"""
Поиск и скачивание треков с SoundCloud.
"""
import asyncio
import logging
import re
import aiohttp
from typing import Optional

logger = logging.getLogger(__name__)

SC_CLIENT_ID = "iZIs9mchVcX5lhVRyQNGogYH2BTG5W3F"


async def _refresh_client_id() -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://soundcloud.com",
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                html = await r.text()
            scripts = re.findall(
                r'src="(https://a-v2\.sndcdn\.com/assets/[^"]+\.js)"', html
            )
            for script_url in reversed(scripts[-5:]):
                async with session.get(script_url,
                                       timeout=aiohttp.ClientTimeout(total=10)) as r:
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

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 401:
                    new_id = await _refresh_client_id()
                    if new_id:
                        SC_CLIENT_ID = new_id
                        params["client_id"] = SC_CLIENT_ID
                        async with session.get(url, params=params,
                                               timeout=aiohttp.ClientTimeout(total=10)) as r2:
                            data = await r2.json()
                    else:
                        return []
                else:
                    data = await resp.json()
        except Exception as e:
            logger.error(f"SoundCloud search error: {e}")
            return []

    from database import Database
    db = Database("library.db")

    results = []
    for item in data.get("collection", []):
        duration_ms  = item.get("duration", 0)
        duration_sec = duration_ms // 1000
        duration_fmt = f"{duration_sec//60}:{duration_sec%60:02d}" if duration_sec else ""
        track_id     = str(item["id"])
        track = {
            "track_id":     track_id,
            "title":        item.get("title", "Unknown"),
            "artist":       item.get("user", {}).get("username", "Unknown"),
            "url":          item.get("permalink_url", ""),
            "duration_sec": duration_sec,
            "duration_fmt": duration_fmt,
            "artwork_url":  item.get("artwork_url", ""),
            "_sc_id":       item["id"],
        }
        db.save_track(track)
        results.append(track)

    return results


async def _get_stream_url(sc_id: int) -> Optional[str]:
    params = {"client_id": SC_CLIENT_ID}
    url    = f"https://api-v2.soundcloud.com/tracks/{sc_id}/streams"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
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
        from database import Database
        saved = Database("library.db").get_track(track.get("track_id",""))
        if saved:
            sc_id = saved.get("_sc_id") or saved.get("sc_id")

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
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 200:
                    return await resp.read()
    except Exception as e:
        logger.error(f"Download error: {e}")
    return None


async def _ytdlp_download(url: str) -> Optional[bytes]:
    try:
        import yt_dlp, tempfile, os
        with tempfile.TemporaryDirectory() as tmpdir:
            out  = os.path.join(tmpdir, "track.%(ext)s")
            opts = {
                "format": "bestaudio/best",
                "outtmpl": out,
                "quiet": True,
                "no_warnings": True,
                "postprocessors": [{"key":"FFmpegExtractAudio",
                                    "preferredcodec":"mp3",
                                    "preferredquality":"128"}],
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
