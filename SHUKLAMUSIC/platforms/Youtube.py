import asyncio
import os
import re
import json
import glob
import random
import logging
from typing import Union

import yt_dlp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
from youtubesearchpython.__future__ import VideosSearch  # (நீங்கள் தேவை என்றால் வைத்திருக்கலாம்)

from SHUKLAMUSIC.utils.database import is_on_off
from SHUKLAMUSIC.utils.formatters import time_to_seconds  # (இங்கே நேரடியாக பயன்படுத்தல; இருந்தாலும் வைத்திருக்கலாம்)

# -------------------------
# Cookie helper (unchanged)
# -------------------------
def cookie_txt_file():
    folder_path = f"{os.getcwd()}/cookies"
    filename = f"{os.getcwd()}/cookies/logs.csv"
    txt_files = glob.glob(os.path.join(folder_path, "*.txt"))
    if not txt_files:
        raise FileNotFoundError("No .txt files found in the specified folder.")
    cookie_txt = random.choice(txt_files)
    with open(filename, "a") as file:
        file.write(f"Choosen File : {cookie_txt}\n")
    return f"""cookies/{str(cookie_txt).split("/")[-1]}"""


# -------------------------
# URL normalize helpers
# -------------------------
YTB_REGEX_ID = re.compile(
    r"""
    (?:youtu\.be/|
       youtube(?:-nocookie)?\.com/
       (?:watch\?.*?v=|embed/|shorts/|live/|v/|.+?\#(?:.*?&)?v=)
    )
    ([A-Za-z0-9_-]{11})
    """,
    re.IGNORECASE | re.VERBOSE,
)

def extract_video_id(url: str) -> Union[str, None]:
    """
    Any YouTube URL -> return 11-char video id, else None
    """
    if not url:
        return None
    url = url.strip().strip("<>")
    m = YTB_REGEX_ID.search(url)
    if m:
        return m.group(1)
    # Fallback: query param v=...
    try:
        from urllib.parse import urlparse, parse_qs
        u = urlparse(url)
        qs = parse_qs(u.query or "")
        if "v" in qs and qs["v"]:
            vid = qs["v"][0]
            if len(vid) == 11 and re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
                return vid
    except Exception:
        pass
    return None

def normalize_watch_url(url_or_id: str) -> Union[str, None]:
    """
    Accepts any youtube url or raw id. Returns canonical watch URL.
    """
    if not url_or_id:
        return None
    candidate = url_or_id.strip().strip("<>")
    # Raw id?
    if len(candidate) == 11 and re.fullmatch(r"[A-Za-z0-9_-]{11}", candidate):
        return f"https://www.youtube.com/watch?v={candidate}"
    vid = extract_video_id(candidate)
    if vid:
        return f"https://www.youtube.com/watch?v={vid}"
    # Already a watch/youtu.be url → keep as-is
    if "youtube.com/watch" in candidate or "youtu.be/" in candidate:
        return candidate
    return None

def is_playlist_url(url: str) -> bool:
    try:
        from urllib.parse import urlparse, parse_qs
        u = urlparse(url or "")
        qs = parse_qs(u.query or "")
        return bool(qs.get("list"))
    except Exception:
        return "playlist?list=" in (url or "")


# -------------------------
# Utility: check size via yt-dlp -J
# -------------------------
async def check_file_size(link):
    async def get_format_info(url):
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "--cookies", cookie_txt_file(),
            "-J",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            print(f"Error:\n{stderr.decode()}")
            return None
        return json.loads(stdout.decode())

    def parse_size(formats):
        total_size = 0
        for fmt in formats:
            if "filesize" in fmt:
                total_size += fmt["filesize"]
        return total_size

    info = await get_format_info(link)
    if info is None:
        return None
    formats = info.get("formats", [])
    if not formats:
        print("No formats found.")
        return None
    total_size = parse_size(formats)
    return total_size


# -------------------------
# Shell helper (unchanged)
# -------------------------
async def shell_cmd(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, errorz = await proc.communicate()
    if errorz:
        if "unavailable videos are hidden" in (errorz.decode("utf-8")).lower():
            return out.decode("utf-8")
        else:
            return errorz.decode("utf-8")
    return out.decode("utf-8")


# -------------------------
# YouTube API wrapper
# -------------------------
class YouTubeAPI:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.status = "https://www.youtube.com/oembed?url="
        self.listbase = "https://youtube.com/playlist?list="
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        return bool(re.search(self.regex, link))

    async def url(self, message_1: Message) -> Union[str, None]:
        """
        Message or its reply → first URL/TEXT_LINK
        """
        messages = [message_1]
        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)
        text = ""
        offset = None
        length = None
        for msg in messages:
            if offset:
                break
            if msg.entities:
                for entity in msg.entities:
                    if entity.type == MessageEntityType.URL:
                        text = msg.text or msg.caption
                        offset, length = entity.offset, entity.length
                        break
            elif msg.caption_entities:
                for entity in msg.caption_entities:
                    if entity.type == MessageEntityType.TEXT_LINK:
                        return entity.url
        if offset in (None,):
            return None
        return text[offset: offset + length]

    # ---------- Metadata via yt_dlp ----------
    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        norm = normalize_watch_url(link)
        if not norm:
            return None, None, 0, None, None

        ydl_opts = {
            "quiet": True,
            "nocheckcertificate": True,
            "geo_bypass": True,
            "cookiefile": cookie_txt_file(),
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(norm, download=False)

        title = info.get("title")
        duration_sec = info.get("duration") or 0  # livestreams → None
        # make mm:ss / hh:mm:ss
        try:
            if duration_sec:
                m, s = divmod(duration_sec, 60)
                h, m = divmod(m, 60)
                duration_min = f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"
            else:
                duration_min = None
        except Exception:
            duration_min = None
        thumbnail = info.get("thumbnail") or ""
        vidid = info.get("id")
        return title, duration_min, int(duration_sec or 0), thumbnail, vidid

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        norm = normalize_watch_url(link)
        if not norm:
            return None
        with yt_dlp.YoutubeDL({"quiet": True, "cookiefile": cookie_txt_file()}) as ydl:
            info = ydl.extract_info(norm, download=False)
        return info.get("title")

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        norm = normalize_watch_url(link)
        if not norm:
            return None
        with yt_dlp.YoutubeDL({"quiet": True, "cookiefile": cookie_txt_file()}) as ydl:
            info = ydl.extract_info(norm, download=False)
        dur = info.get("duration")
        if dur is None:
            return None
        m, s = divmod(dur, 60)
        h, m = divmod(m, 60)
        return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        norm = normalize_watch_url(link)
        if not norm:
            return None
        with yt_dlp.YoutubeDL({"quiet": True, "cookiefile": cookie_txt_file()}) as ydl:
            info = ydl.extract_info(norm, download=False)
        return info.get("thumbnail") or ""

    # ---------- Stream URL ----------
    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        norm = normalize_watch_url(link)
        if not norm:
            return 0, "Invalid YouTube URL"
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "--cookies", cookie_txt_file(),
            "-g",
            "-f", "best[height<=?720][width<=?1280]",
            norm,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if stdout:
            return 1, stdout.decode().split("\n")[0]
        else:
            return 0, stderr.decode()

    # ---------- Playlist ----------
    async def playlist(self, link, limit, user_id, videoid: Union[bool, str] = None):
        if videoid:
            link = self.listbase + link
        if not is_playlist_url(link):
            return []
        playlist = await shell_cmd(
            f"yt-dlp -i --get-id --flat-playlist --cookies {cookie_txt_file()} --playlist-end {limit} --skip-download {link}"
        )
        try:
            result = [x for x in playlist.split("\n") if x.strip()]
        except Exception:
            result = []
        return result

    # ---------- Track details ----------
    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        norm = normalize_watch_url(link)
        if not norm:
            return None, None
        with yt_dlp.YoutubeDL({"quiet": True, "cookiefile": cookie_txt_file()}) as ydl:
            info = ydl.extract_info(norm, download=False)

        title = info.get("title")
        dur = info.get("duration")
        if dur:
            m, s = divmod(dur, 60)
            h, m = divmod(m, 60)
            duration_min = f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"
        else:
            duration_min = None
        vidid = info.get("id")
        yturl = info.get("webpage_url") or norm
        thumbnail = info.get("thumbnail") or ""

        track_details = {
            "title": title,
            "link": yturl,
            "vidid": vidid,
            "duration_min": duration_min,
            "thumb": thumbnail,
        }
        return track_details, vidid

    # ---------- Formats (quality picker) ----------
    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        norm = normalize_watch_url(link)
        if not norm:
            return [], link
        ytdl_opts = {
            "quiet": True,
            "cookiefile": cookie_txt_file(),
            "nocheckcertificate": True,
            "geo_bypass": True,
        }
        ydl = yt_dlp.YoutubeDL(ytdl_opts)
        with ydl:
            formats_available = []
            r = ydl.extract_info(norm, download=False)
            for fmt in r.get("formats", []):
                try:
                    fmt_str = str(fmt.get("format"))
                except Exception:
                    continue
                # avoid dash only
                if "dash" in fmt_str.lower():
                    continue
                needed = all(k in fmt for k in ["format", "filesize", "format_id", "ext", "format_note"])
                if not needed:
                    continue
                formats_available.append(
                    {
                        "format": fmt["format"],
                        "filesize": fmt["filesize"],
                        "format_id": fmt["format_id"],
                        "ext": fmt["ext"],
                        "format_note": fmt["format_note"],
                        "yturl": norm,
                    }
                )
        return formats_available, norm

    # ---------- Slider (search-based pick n-th) ----------
    async def slider(self, link: str, query_type: int, videoid: Union[bool, str] = None):
        # NOTE: slider இன்னும் search API-ஐ பயன்படுத்துகிறது (தேவைப்பட்டால்தான்)
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        a = VideosSearch(link, limit=10)
        result = (await a.next()).get("result")
        title = result[query_type]["title"]
        duration_min = result[query_type]["duration"]
        vidid = result[query_type]["id"]
        thumbnail = result[query_type]["thumbnails"][0]["url"].split("?")[0]
        return title, duration_min, thumbnail, vidid

    # ---------- Download (audio/video/direct) ----------
    async def download(
        self,
        link: str,
        mystic,
        video: Union[bool, str] = None,
        videoid: Union[bool, str] = None,
        songaudio: Union[bool, str] = None,
        songvideo: Union[bool, str] = None,
        format_id: Union[bool, str] = None,
        title: Union[bool, str] = None,
    ) -> str:
        if videoid:
            link = self.base + link
        norm = normalize_watch_url(link)
        if not norm:
            return ("", True)  # invalid url
        link = norm

        loop = asyncio.get_running_loop()

        def audio_dl():
            ydl_optssx = {
                "format": "bestaudio/best",
                "outtmpl": "downloads/%(id)s.%(ext)s",
                "geo_bypass": True,
                "nocheckcertificate": True,
                "quiet": True,
                "cookiefile": cookie_txt_file(),
                "no_warnings": True,
            }
            x = yt_dlp.YoutubeDL(ydl_optssx)
            info = x.extract_info(link, False)
            xyz = os.path.join("downloads", f"{info['id']}.{info['ext']}")
            if os.path.exists(xyz):
                return xyz
            x.download([link])
            return xyz

        def video_dl():
            ydl_optssx = {
                "format": "(bestvideo[height<=?720][width<=?1280][ext=mp4])+(bestaudio[ext=m4a])",
                "outtmpl": "downloads/%(id)s.%(ext)s",
                "geo_bypass": True,
                "nocheckcertificate": True,
                "quiet": True,
                "cookiefile": cookie_txt_file(),
                "no_warnings": True,
                "prefer_ffmpeg": True,
                "merge_output_format": "mp4",
            }
            x = yt_dlp.YoutubeDL(ydl_optssx)
            info = x.extract_info(link, False)
            xyz = os.path.join("downloads", f"{info['id']}.{info['ext']}")
            if os.path.exists(xyz):
                return xyz
            x.download([link])
            return xyz

        def song_video_dl():
            formats = f"{format_id}+140"
            fpath = f"downloads/{title}"
            ydl_optssx = {
                "format": formats,
                "outtmpl": fpath,
                "geo_bypass": True,
                "nocheckcertificate": True,
                "quiet": True,
                "no_warnings": True,
                "cookiefile": cookie_txt_file(),
                "prefer_ffmpeg": True,
                "merge_output_format": "mp4",
            }
            x = yt_dlp.YoutubeDL(ydl_optssx)
            x.download([link])

        def song_audio_dl():
            fpath = f"downloads/{title}.%(ext)s"
            ydl_optssx = {
                "format": format_id,
                "outtmpl": fpath,
                "geo_bypass": True,
                "nocheckcertificate": True,
                "quiet": True,
                "no_warnings": True,
                "cookiefile": cookie_txt_file(),
                "prefer_ffmpeg": True,
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "192",
                    }
                ],
            }
            x = yt_dlp.YoutubeDL(ydl_optssx)
            x.download([link])

        if songvideo:
            await loop.run_in_executor(None, song_video_dl)
            fpath = f"downloads/{title}.mp4"
            return fpath
        elif songaudio:
            await loop.run_in_executor(None, song_audio_dl)
            fpath = f"downloads/{title}.mp3"
            return fpath
        elif video:
            if await is_on_off(1):
                direct = True
                downloaded_file = await loop.run_in_executor(None, video_dl)
            else:
                proc = await asyncio.create_subprocess_exec(
                    "yt-dlp",
                    "--cookies", cookie_txt_file(),
                    "-g",
                    "-f",
                    "best[height<=?720][width<=?1280]",
                    f"{link}",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                if stdout:
                    downloaded_file = stdout.decode().split("\n")[0]
                    direct = False
                else:
                    direct = True
                    downloaded_file = await loop.run_in_executor(None, video_dl)
        else:
            direct = True
            downloaded_file = await loop.run_in_executor(None, audio_dl)
        return downloaded_file, direct
