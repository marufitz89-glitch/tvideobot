

from __future__ import annotations

import base64
import html
import http.client
import ipaddress
import json
import logging
import os
import random
import re
import signal
import socket
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

LOG = logging.getLogger("streamly")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

TELEGRAM_TOKEN = os.getenv("TOKEN", "").strip()
META_AI_KEY = os.getenv("META_AI_API_KEY", "nxt_3a454c41e6a84aeead28d1fb4aec87a4").strip()
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
META_AI_GEM_ID = "ba0fbe0d-976e-493a-afdb-6d8469e53df0"
META_AI_ENDPOINT = f"https://nxtai.zipohostbd.workers.dev/api/use?gem={META_AI_GEM_ID}"
IMAGE_FALLBACK_ENDPOINT = "https://image.pollinations.ai/prompt/"
TIKWM_ENDPOINT = "https://www.tikwm.com/api/"
SNAPTIK_ENDPOINT = "https://snaptik.app/abc2.php"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

MAX_MEDIA_BYTES = max(8, int(os.getenv("MAX_MEDIA_MB", "48"))) * 1024 * 1024
MAX_VIDEO_SECONDS = max(60, int(os.getenv("MAX_VIDEO_MINUTES", "30"))) * 60
STATE_TTL_SECONDS = 20 * 60
MAX_TEXT_LENGTH = 3900
MAX_URL_LENGTH = 2048
HTTP_MAX_REDIRECTS = 3
DOWNLOAD_CHUNK_SIZE = 256 * 1024
PROGRESS_INTERVAL_SECONDS = 0.8
MAX_CONCURRENT_DOWNLOADS = max(1, int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2")))
REQUEST_RETRIES = max(0, min(4, int(os.getenv("REQUEST_RETRIES", "2"))))
TEMP_DIR = os.getenv("TEMP_DIR", tempfile.gettempdir())


@dataclass(frozen=True)
class Platform:
    key: str
    title: str
    emoji: str
    domains: tuple[str, ...]


TIKTOK = Platform(
    "tiktok",
    "TikTok",
    "▶",
    ("tiktok.com", "m.tiktok.com", "vm.tiktok.com", "vt.tiktok.com"),
)
PLATFORM_BY_KEY = {TIKTOK.key: TIKTOK}

CHAT_MODES: dict[int, str] = {}
CHAT_PLATFORMS: dict[int, str] = {}
DOWNLOAD_OPTIONS: dict[int, dict[str, Any]] = {}
ACTIVE_CHATS: set[int] = set()
STATE_LOCK = threading.RLock()
EXECUTOR = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS, thread_name_prefix="streamly")
SHUTDOWN = threading.Event()


def log(message: str, *args: Any) -> None:
    LOG.info(message, *args)


def format_bytes(value: int | float) -> str:
    size = float(max(0, value))
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def escape(value: Any) -> str:
    return html.escape(str(value), quote=False)


def normalize_url(value: str) -> str | None:
    candidate = value.strip()
    if len(candidate) > MAX_URL_LENGTH:
        return None
    if not re.match(r"^https?://", candidate, flags=re.IGNORECASE):
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    if parsed.username or parsed.password:
        return None
    if any(ord(ch) < 32 for ch in candidate):
        return None
    return candidate


def _is_public_hostname(hostname: str) -> bool:
    host = hostname.strip().rstrip(".").lower()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".localhost"):
        return False
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return False
    addresses = {item[4][0] for item in infos if item and item[4]}
    if not addresses:
        return False
    for address in addresses:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            return False
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            return False
    return True


def validate_remote_url(value: Any, *, allowed_hosts: set[str] | None = None) -> str | None:
    if not isinstance(value, str):
        return None
    url = normalize_url(value)
    if not url:
        return None
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().removeprefix("www.")
    if not host or not _is_public_hostname(host):
        return None
    if allowed_hosts is not None and not any(host == h or host.endswith("." + h) for h in allowed_hosts):
        return None
    return url


def platform_from_url(url: str) -> Platform | None:
    host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    for platform in PLATFORM_BY_KEY.values():
        if any(host == domain or host.endswith(f".{domain}") for domain in platform.domains):
            return platform
    return None


class _SafeRedirectHandler(__import__("urllib.request", fromlist=["HTTPRedirectHandler"]).HTTPRedirectHandler):
    def __init__(self, max_redirects: int = HTTP_MAX_REDIRECTS):
        super().__init__()
        self.max_redirects = max_redirects
        self.count = 0

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.count += 1
        if self.count > self.max_redirects:
            raise RuntimeError("Too many HTTP redirects")
        safe = validate_remote_url(newurl)
        if not safe:
            raise RuntimeError("Unsafe HTTP redirect blocked")
        return super().redirect_request(req, fp, code, msg, headers, safe)


def http_request(
    url: str,
    *,
    method: str = "GET",
    payload: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 35,
    max_bytes: int = 3 * 1024 * 1024,
) -> tuple[int, dict[str, str], bytes]:
    safe_url = validate_remote_url(url)
    if not safe_url:
        raise RuntimeError("Blocked unsafe or invalid remote URL")
    request_headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    if headers:
        request_headers.update(headers)
    last_error: Exception | None = None
    for attempt in range(REQUEST_RETRIES + 1):
        request = Request(safe_url, data=payload, headers=request_headers, method=method)
        try:
            opener = __import__("urllib.request", fromlist=["build_opener"]).build_opener(_SafeRedirectHandler())
            with opener.open(request, timeout=timeout) as response:
                chunks: list[bytes] = []
                total = 0
                while total < max_bytes:
                    chunk = response.read(min(64 * 1024, max_bytes - total))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    total += len(chunk)
                return response.status, dict(response.headers.items()), b"".join(chunks)
        except HTTPError as error:
            body = error.read(1024)
            if error.code in {408, 425, 429, 500, 502, 503, 504} and attempt < REQUEST_RETRIES:
                time.sleep(min(2 ** attempt, 5))
                continue
            raise RuntimeError(
                f"Remote service returned HTTP {error.code}: "
                f"{body.decode(errors='replace')[:180]}"
            ) from error
        except (URLError, TimeoutError, OSError, RuntimeError) as error:
            last_error = error
            if attempt < REQUEST_RETRIES:
                time.sleep(min(2 ** attempt, 5))
                continue
            if isinstance(error, RuntimeError):
                raise
            raise RuntimeError(f"Remote service could not be reached: {error}") from error
    raise RuntimeError(f"Remote service request failed: {last_error}")


def json_request(
    url: str,
    *,
    method: str = "GET",
    data: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 35,
) -> Any:
    body = json.dumps(data).encode() if data is not None else None
    request_headers = {"Content-Type": "application/json"} if data is not None else {}
    if headers:
        request_headers.update(headers)
    _, _, raw = http_request(
        url,
        method=method,
        payload=body,
        headers=request_headers,
        timeout=timeout,
    )
    try:
        return json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as error:
        raise RuntimeError("Remote service returned invalid JSON") from error


def telegram_call(method: str, data: dict[str, Any] | None = None) -> Any:
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TOKEN is not configured")
    payload = (data or {}).copy()
    status, _, raw = http_request(
        f"{TELEGRAM_API}/{method}",
        method="POST",
        payload=urlencode(payload).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=45,
        max_bytes=4 * 1024 * 1024,
    )
    if status >= 400:
        raise RuntimeError(f"Telegram API HTTP {status}")
    try:
        result = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as error:
        raise RuntimeError("Telegram returned invalid JSON") from error
    if not result.get("ok"):
        raise RuntimeError(result.get("description", "Telegram API request failed"))
    return result.get("result")


def telegram_upload(
    method: str,
    field_name: str,
    file_name: str,
    file_bytes: bytes,
    fields: dict[str, str],
) -> Any:
    boundary = f"----Streamly{uuid.uuid4().hex}"
    parts: list[bytes] = []
    for key, value in fields.items():
        parts.extend(
            [
                f"--{boundary}\r\n".encode(),
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode(),
                str(value).encode(),
                b"\r\n",
            ]
        )
    parts.extend(
        [
            f"--{boundary}\r\n".encode(),
            f'Content-Disposition: form-data; name="{field_name}"; '
            f'filename="{file_name}"\r\n'.encode(),
            b"Content-Type: application/octet-stream\r\n\r\n",
            file_bytes,
            b"\r\n",
            f"--{boundary}--\r\n".encode(),
        ]
    )
    _, _, raw = http_request(
        f"{TELEGRAM_API}/{method}",
        method="POST",
        payload=b"".join(parts),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        timeout=120,
        max_bytes=4 * 1024 * 1024,
    )
    result = json.loads(raw.decode("utf-8", errors="replace"))
    if not result.get("ok"):
        raise RuntimeError(result.get("description", "Telegram upload failed"))
    return result.get("result")


def send_message(chat_id: int | str, text: str, **extra: Any) -> Any:
    return telegram_call(
        "sendMessage",
        {"chat_id": str(chat_id), "text": text, "parse_mode": "HTML", **extra},
    )


def edit_message(chat_id: int | str, message_id: int, text: str, **extra: Any) -> Any:
    try:
        return telegram_call(
            "editMessageText",
            {
                "chat_id": str(chat_id),
                "message_id": str(message_id),
                "text": text,
                "parse_mode": "HTML",
                **extra,
            },
        )
    except RuntimeError as error:
        if "message is not modified" not in str(error).lower():
            log("Could not edit status message: %s", error)
        return None


def answer_callback(callback_id: str, text: str = "") -> None:
    try:
        telegram_call("answerCallbackQuery", {"callback_query_id": callback_id, "text": text})
    except RuntimeError as error:
        log("Callback acknowledgement failed: %s", error)


def inline_keyboard(rows: list[list[dict[str, str]]]) -> str:
    return json.dumps({"inline_keyboard": rows})


def main_keyboard() -> str:
    return inline_keyboard(
        [
            [
                {"text": "⬇️ Download TikTok", "callback_data": "mode:download", "style": "primary"},
                {"text": "🤖 AI assistant", "callback_data": "mode:ai", "style": "primary"},
            ],
            [
                {"text": "ℹ️ How it works", "callback_data": "help", "style": "primary"},
                {"text": "✖️ Cancel", "callback_data": "cancel", "style": "danger"},
            ],
        ]
    )


def platform_keyboard() -> str:
    """Show the platforms currently supported by the downloader."""
    return inline_keyboard(
        [
            [{"text": "🎵 TikTok", "callback_data": "platform:tiktok", "style": "primary"}],
            [{"text": "↩️ Back to menu", "callback_data": "home", "style": "primary"}],
            [{"text": "✖️ Cancel", "callback_data": "cancel", "style": "danger"}],
        ]
    )


def quality_keyboard(options: list[dict[str, Any]]) -> str:
    rows: list[list[dict[str, str]]] = []
    for index, option in enumerate(options[:6]):
        size = option.get("size")
        suffix = f" · {format_bytes(size)}" if size else ""
        rows.append(
            [
                {
                    "text": f"🎚️ {option['label']}{suffix}",
                    "callback_data": f"quality:{index}",
                    "style": "primary",
                }
            ]
        )
    rows.extend(
        [
            [{"text": "🔁 Download another", "callback_data": "mode:download", "style": "primary"}],
            [{"text": "✖️ Cancel", "callback_data": "cancel", "style": "danger"}],
        ]
    )
    return inline_keyboard(rows)


def format_keyboard() -> str:
    return inline_keyboard(
        [
            [
                {"text": "🎬 MP4 video", "callback_data": "format:mp4", "style": "success"},
                {"text": "🎵 MP3 audio", "callback_data": "format:mp3", "style": "success"},
            ],
            [{"text": "↩️ Choose another quality", "callback_data": "back:quality", "style": "primary"}],
            [{"text": "✖️ Cancel", "callback_data": "cancel", "style": "danger"}],
        ]
    )


def progress_text(title: str, downloaded: int, total: int, stage: str) -> str:
    if total:
        percent = min(100, downloaded * 100 / total)
        filled = min(20, int(percent / 5))
        progress = f"{percent:5.1f}%"
        status = f"{format_bytes(downloaded)} of {format_bytes(total)}"
    else:
        filled = min(20, int(time.monotonic() * 3) % 21)
        progress = "working"
        status = f"{format_bytes(downloaded)} downloaded"
    bar = "█" * filled + "░" * (20 - filled)
    return (
        f"<b>📥 {escape(title[:70])}</b>\n\n"
        f"<code>{bar}</code>\n\n"
        f"🚀 <b>Progress:</b> {progress}\n"
        f"📶 <b>Status:</b> {escape(status)}\n"
        f"🛠 <b>Stage:</b> {escape(stage)}"
    )


def cleanup_state() -> None:
    cutoff = time.time() - STATE_TTL_SECONDS
    with STATE_LOCK:
        expired = [
            chat_id
            for chat_id, item in DOWNLOAD_OPTIONS.items()
            if float(item.get("created_at", 0)) < cutoff
        ]
        for chat_id in expired:
            DOWNLOAD_OPTIONS.pop(chat_id, None)
            CHAT_PLATFORMS.pop(chat_id, None)
            if chat_id not in ACTIVE_CHATS:
                CHAT_MODES.pop(chat_id, None)


def _remote_url(value: Any) -> str | None:
    return validate_remote_url(value)


def _media_links(value: Any, key_hint: str = "") -> tuple[list[str], list[str]]:
    """Collect safe, likely video/audio URLs from resolver JSON."""
    videos: list[str] = []
    audios: list[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            hint = key.lower()
            url = _remote_url(nested)
            if url:
                if any(word in hint for word in ("music", "audio", "mp3", "sound")):
                    audios.append(url)
                elif any(word in hint for word in ("play", "video", "mp4", "download", "hd", "url")) and not any(word in hint for word in ("cover", "avatar", "image", "thumb")):
                    videos.append(url)
            nested_videos, nested_audios = _media_links(nested, hint)
            videos.extend(nested_videos)
            audios.extend(nested_audios)
    elif isinstance(value, list):
        for nested in value:
            nested_videos, nested_audios = _media_links(nested, key_hint)
            videos.extend(nested_videos)
            audios.extend(nested_audios)
    return list(dict.fromkeys(videos)), list(dict.fromkeys(audios))


def _quality_label(index: int, option: dict[str, Any]) -> str:
    height = int(option.get("height") or 0)
    if height >= 1080:
        return "1080p HD"
    if height >= 720:
        return "720p HD"
    if height >= 480:
        return "480p"
    if height >= 360:
        return "360p"
    return ("HD quality", "Standard quality", "Auto quality")[min(index, 2)]


def _tiktok_options(title: str, video_urls: list[str], audio_urls: list[str], known_sizes: dict[str, Any] | None = None, heights: dict[str, Any] | None = None) -> tuple[str, list[dict[str, Any]]]:
    if not video_urls:
        raise RuntimeError("TikTok API did not return a downloadable video URL")
    known_sizes = known_sizes or {}
    heights = heights or {}
    options: list[dict[str, Any]] = []
    for index, video_url in enumerate(video_urls[:6]):
        try:
            size = int(known_sizes.get(video_url) or 0)
        except (TypeError, ValueError):
            size = 0
        try:
            height = int(heights.get(video_url) or 0)
        except (TypeError, ValueError):
            height = 0
        option = {
            "format": video_url,
            "url": video_url,
            "audio_url": audio_urls[0] if audio_urls else "",
            "height": height,
            "size": size,
            "ext": "mp4",
        }
        option["label"] = _quality_label(index, option)
        options.append(option)
    return title, options


def _resolve_tikwm(source_url: str) -> tuple[str, list[dict[str, Any]]]:
    endpoint = f"{TIKWM_ENDPOINT}?{urlencode({'url': source_url, 'hd': '1'})}"
    response = json_request(endpoint, timeout=45)
    if not isinstance(response, dict) or response.get("code") not in {0, "0", None}:
        message = response.get("msg") if isinstance(response, dict) else ""
        raise RuntimeError(str(message or "TikWM did not resolve this TikTok URL"))
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, dict):
        raise RuntimeError("TikWM returned no media data")
    duration = int(data.get("duration") or 0)
    if duration > MAX_VIDEO_SECONDS:
        raise RuntimeError(
            f"This video is {duration // 60} minutes long. "
            f"The free service limit is {MAX_VIDEO_SECONDS // 60} minutes."
        )
    title = str(data.get("title") or "TikTok video")
    video_urls, audio_urls = _media_links(data)
    preferred_video = [
        _remote_url(data.get(key))
        for key in ("hdplay", "play", "wmplay")
        if _remote_url(data.get(key))
    ]
    preferred_audio = [
        _remote_url(data.get(key))
        for key in ("music", "music_url", "mp3", "audio")
        if _remote_url(data.get(key))
    ]
    video_urls = list(dict.fromkeys(preferred_video + video_urls))
    known_sizes = {
        _remote_url(data.get("hdplay")): data.get("hd_size"),
        _remote_url(data.get("play")): data.get("size"),
        _remote_url(data.get("wmplay")): data.get("wm_size"),
    }
    heights = {
        _remote_url(data.get("hdplay")): data.get("height"),
        _remote_url(data.get("play")): data.get("height"),
        _remote_url(data.get("wmplay")): data.get("height"),
    }
    return _tiktok_options(
        title,
        video_urls,
        list(dict.fromkeys(preferred_audio + audio_urls)),
        known_sizes=known_sizes,
        heights=heights,
    )


def _resolve_snaptik(source_url: str) -> tuple[str, list[dict[str, Any]]]:
    body = urlencode({"url": source_url}).encode()
    _, _, raw = http_request(
        SNAPTIK_ENDPOINT,
        method="POST",
        payload=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": "https://snaptik.app/",
        },
        timeout=45,
        max_bytes=4 * 1024 * 1024,
    )
    try:
        response = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as error:
        raise RuntimeError("SnapTik returned invalid JSON") from error
    if isinstance(response, dict) and response.get("success") is False:
        raise RuntimeError("SnapTik could not resolve this TikTok URL")
    video_urls, audio_urls = _media_links(response)
    title = "TikTok video"
    if isinstance(response, dict):
        title = str(response.get("title") or (response.get("data") or {}).get("title") or title)
    return _tiktok_options(title, video_urls, audio_urls)


def tiktok_info(source_url: str) -> tuple[str, list[dict[str, Any]]]:
    """Resolve public TikTok media through TikWM, then SnapTik as fallback."""
    errors: list[str] = []
    for resolver in (_resolve_tikwm, _resolve_snaptik):
        try:
            title, options = resolver(source_url)
            if options:
                # Prefer a resolver that actually exposes audio when possible.
                if any(_remote_url(item.get("audio_url")) for item in options):
                    return title, options
                errors.append(f"{resolver.__name__}: video found but no audio URL")
        except Exception as error:
            errors.append(f"{resolver.__name__}: {error}")
    detail = errors[-1] if errors else "no resolver returned media"
    LOG.warning("TikTok resolvers failed: %s", " | ".join(errors[-3:]))
    raise RuntimeError(f"TikTok download APIs could not resolve this URL: {detail[:180]}")


def _download_to_temp(media_url: str, title: str, progress_callback: Callable[[int, int, str], None]) -> tuple[str, int]:
    safe_url = validate_remote_url(media_url)
    if not safe_url:
        raise RuntimeError("Unsafe media URL returned by resolver")
    parsed = urlparse(safe_url)
    suffix = ".mp3" if ".mp3" in parsed.path.lower() else ".mp4"
    fd, path = tempfile.mkstemp(prefix="streamly-", suffix=suffix, dir=TEMP_DIR)
    os.close(fd)
    downloaded = 0
    total = 0
    last_update = 0.0
    try:
        request = Request(safe_url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"}, method="GET")
        opener = __import__("urllib.request", fromlist=["build_opener"]).build_opener(_SafeRedirectHandler())
        with opener.open(request, timeout=60) as response, open(path, "wb") as output:
            try:
                total = int(response.headers.get("Content-Length") or 0)
            except (TypeError, ValueError):
                total = 0
            if total > MAX_MEDIA_BYTES:
                raise RuntimeError(f"The file is {format_bytes(total)}, above the {format_bytes(MAX_MEDIA_BYTES)} limit.")
            progress_callback(0, total, "Downloading…")
            while True:
                chunk = response.read(DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                downloaded += len(chunk)
                if downloaded > MAX_MEDIA_BYTES:
                    raise RuntimeError(f"The downloaded file exceeded {format_bytes(MAX_MEDIA_BYTES)}.")
                output.write(chunk)
                now = time.monotonic()
                if now - last_update >= PROGRESS_INTERVAL_SECONDS:
                    last_update = now
                    progress_callback(downloaded, total, "Downloading…")
            if total and downloaded != total:
                total = downloaded
            progress_callback(downloaded, total, "Download complete — Telegram upload শুরু হচ্ছে…")
            return path, downloaded
    except Exception:
        try:
            os.remove(path)
        except OSError:
            pass
        raise


def _multipart_stream_upload(method: str, field_name: str, file_path: str, file_name: str, fields: dict[str, str], progress_callback: Callable[[int, int, str], None]) -> Any:
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TOKEN is not configured")
    parsed = urlparse(f"{TELEGRAM_API}/{method}")
    if parsed.scheme != "https" or not parsed.hostname:
        raise RuntimeError("Invalid Telegram API endpoint")
    boundary = f"----Streamly{uuid.uuid4().hex}"
    chunks: list[bytes] = []
    for key, value in fields.items():
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
        chunks.append(str(value).encode())
        chunks.append(b"\r\n")
    chunks.append(f"--{boundary}\r\n".encode())
    chunks.append(f'Content-Disposition: form-data; name="{field_name}"; filename="{file_name}"\r\n'.encode())
    chunks.append(b"Content-Type: application/octet-stream\r\n\r\n")
    closing = f"\r\n--{boundary}--\r\n".encode()
    file_size = os.path.getsize(file_path)
    content_length = sum(len(x) for x in chunks) + file_size + len(closing)
    connection = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=120)
    sent = 0
    try:
        connection.putrequest("POST", parsed.path + (("?" + parsed.query) if parsed.query else ""))
        connection.putheader("Content-Type", f"multipart/form-data; boundary={boundary}")
        connection.putheader("Content-Length", str(content_length))
        connection.putheader("User-Agent", USER_AGENT)
        connection.endheaders()
        for chunk in chunks:
            connection.send(chunk)
        progress_callback(0, file_size, "Uploading to Telegram…")
        last_update = time.monotonic()
        with open(file_path, "rb") as file:
            while True:
                chunk = file.read(DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                connection.send(chunk)
                sent += len(chunk)
                now = time.monotonic()
                if now - last_update >= PROGRESS_INTERVAL_SECONDS or sent == file_size:
                    last_update = now
                    progress_callback(sent, file_size, "Uploading to Telegram…")
        connection.send(closing)
        response = connection.getresponse()
        raw = response.read(4 * 1024 * 1024)
        if response.status >= 400:
            raise RuntimeError(f"Telegram upload HTTP {response.status}")
        result = json.loads(raw.decode("utf-8", errors="replace"))
        if not result.get("ok"):
            raise RuntimeError(result.get("description", "Telegram upload failed"))
        return result.get("result")
    finally:
        connection.close()


def download_tiktok(source_url: str, option: dict[str, Any], output_format: str, progress_callback: Callable[[int, int, str], None]) -> tuple[str, str, str]:
    """Download resolver media to bounded temporary storage for real progress reporting."""
    if output_format not in {"mp4", "mp3"}:
        raise RuntimeError("Unsupported output format")
    media_url = validate_remote_url(option.get("audio_url" if output_format == "mp3" else "url"))
    if not media_url:
        raise RuntimeError("This TikTok API did not provide a usable media URL.")
    try:
        known_size = int(option.get("size") or 0)
    except (TypeError, ValueError):
        known_size = 0
    if known_size > MAX_MEDIA_BYTES:
        raise RuntimeError(f"The file is {format_bytes(known_size)}, above the {format_bytes(MAX_MEDIA_BYTES)} Telegram limit.")
    title = str(option.get("title") or "TikTok media")
    path, _ = _download_to_temp(media_url, title, progress_callback)
    return path, title, ("mp3" if output_format == "mp3" else "mp4")


def safe_filename(title: str, extension: str) -> str:
    clean = re.sub(r"[^\w\s.-]", "", title, flags=re.UNICODE).strip()
    clean = re.sub(r"\s+", " ", clean)[:70] or "streamly-download"
    return f"{clean}.{extension}"


def send_download(chat_id: int, status_id: int, source_url: str, option: dict[str, Any]) -> None:
    last_update = 0.0
    temp_path: str | None = None

    def update(downloaded: int, total: int, stage: str) -> None:
        nonlocal last_update
        now = time.monotonic()
        if now - last_update < PROGRESS_INTERVAL_SECONDS and stage not in {"Download complete — Telegram upload শুরু হচ্ছে…"}:
            return
        last_update = now
        edit_message(chat_id, status_id, progress_text(str(option.get("title", "TikTok video")), downloaded, total, stage))

    try:
        output_format = str(option["output_format"])
        temp_path, title, extension = download_tiktok(source_url, option, output_format, update)
        file_name = safe_filename(title, extension)
        caption = f"<b>{escape(title[:900])}</b>\n\nDownloaded as {output_format.upper()} by Streamly."
        method = "sendAudio" if output_format == "mp3" else "sendVideo"
        field = "audio" if output_format == "mp3" else "video"
        _user_stats(chat_id)["downloads"] = _user_stats(chat_id).get("downloads", 0) + 1
        _user_stats(chat_id)["xp"] += 5
        _user_stats(chat_id)["points"] += 5
        _multipart_stream_upload(
            method,
            field,
            temp_path,
            file_name,
            {
                "chat_id": str(chat_id),
                "caption": caption,
                **({"title": title[:200]} if output_format == "mp3" else {"supports_streaming": "true"}),
            },
            update,
        )
        edit_message(chat_id, status_id, "<b>Download complete</b>\n\nআপনার ফাইল পাঠানো হয়েছে।", reply_markup=main_keyboard())
    except Exception as error:
        log("Download failed for chat %s: %s", chat_id, error)
        message = str(error).lower()
        if "sign in" in message or "age" in message or "private" in message:
            detail = "এই ভিডিওটি public automated access-এর জন্য available নয়। Private বা sign-in bypass করা যাবে না।"
        elif "limit" in message or "larger than" in message or "file is" in message and "mb" in message:
            detail = "ফাইলটি Telegram bot limit-এর চেয়ে বড়। ছোট quality বেছে আবার চেষ্টা করুন।"
        elif "unsafe" in message or "blocked" in message:
            detail = "নিরাপত্তার কারণে এই media URL গ্রহণ করা হয়নি। আবার public TikTok URL দিয়ে চেষ্টা করুন।"
        else:
            detail = "লিংকটি public কিনা এবং ভিডিওটি available কিনা দেখে আবার চেষ্টা করুন। প্রয়োজনে অন্য quality বেছে নিন।"
        edit_message(chat_id, status_id, f"<b>ডাউনলোড করা যায়নি</b>\n\n{escape(detail)}", reply_markup=platform_keyboard())
    finally:
        if temp_path:
            try:
                os.remove(temp_path)
            except OSError:
                pass
        with STATE_LOCK:
            ACTIVE_CHATS.discard(chat_id)


def resolve_download(chat_id: int, status_id: int, source_url: str) -> None:
    try:
        edit_message(
            chat_id,
            status_id,
            "<b>🎵 TikTok</b>\n\n১/৩  Public video যাচাই করছি…",
        )
        title, options = tiktok_info(source_url)
        with STATE_LOCK:
            DOWNLOAD_OPTIONS[chat_id] = {
                "title": title,
                "options": options,
                "source_url": source_url,
                "created_at": time.time(),
            }
        edit_message(
            chat_id,
            status_id,
            f"<b>🎵 {escape(title[:80])}</b>\n\n"
            f"২/৩  {len(options)}টি quality পাওয়া গেছে।\n"
            "এখন quality নির্বাচন করুন — তারপর MP4 বা MP3 বেছে নিতে পারবেন:",
            reply_markup=quality_keyboard(options),
        )
    except Exception as error:
        log("Resolve failed for chat %s: %s", chat_id, error)
        reason = str(error).lower()
        if "sign in" in reason or "private" in reason or "age" in reason:
            detail = "এই ভিডিওটি public automated access-এর জন্য available নয়।"
        elif "minutes long" in reason:
            detail = str(error)
        else:
            detail = (
                "Video-টি public কিনা, URL ঠিক আছে কিনা এবং region/age restriction "
                "আছে কিনা দেখে আবার চেষ্টা করুন।"
            )
        edit_message(
            chat_id,
            status_id,
            f"<b>TikTok video resolve করা যায়নি</b>\n\n{escape(detail)}",
            reply_markup=platform_keyboard(),
        )


def ai_request(message: str) -> Any:
    if not META_AI_KEY:
        raise RuntimeError("META_AI_API_KEY is not configured")
    return json_request(
        META_AI_ENDPOINT,
        method="POST",
        data={"api_key": META_AI_KEY, "message": message},
        headers={"Accept": "application/json"},
        timeout=75,
    )


def extract_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("response", "reply", "message", "text", "content", "answer", "output"):
            if key in value:
                result = extract_text(value[key])
                if result:
                    return result
        for nested in value.values():
            result = extract_text(nested)
            if result:
                return result
    if isinstance(value, list):
        return "\n".join(result for item in value if (result := extract_text(item)))
    return ""


def extract_image(value: Any) -> tuple[str | None, bytes | None]:
    if isinstance(value, dict):
        for key, nested in value.items():
            lower = key.lower()
            if isinstance(nested, str) and nested.startswith(("http://", "https://")):
                if any(word in lower for word in ("image", "photo", "picture", "url", "src")):
                    return nested, None
            if isinstance(nested, str) and nested.startswith("data:image/"):
                try:
                    return None, base64.b64decode(nested.split(",", 1)[1])
                except (ValueError, IndexError):
                    pass
            url, raw = extract_image(nested)
            if url or raw:
                return url, raw
    elif isinstance(value, list):
        for nested in value:
            url, raw = extract_image(nested)
            if url or raw:
                return url, raw
    return None, None


def fallback_image_url(prompt: str) -> str:
    cleaned = re.sub(r"\s+", " ", prompt).strip()[:700]
    return f"{IMAGE_FALLBACK_ENDPOINT}{quote(cleaned, safe='')}?width=1024&height=1024&nologo=true"


def send_ai_response(chat_id: int, prompt: str, status_id: int) -> None:
    try:
        response = ai_request(prompt)
        image_url, image_bytes = extract_image(response)
        text = extract_text(response).strip()
        image_request = prompt.lstrip().lower().startswith("/image")
        if image_request and not image_url and not image_bytes:
            image_url = fallback_image_url(prompt.lstrip()[len("/image") :].strip())
        if image_url:
            safe_image = validate_remote_url(image_url)
            if not safe_image:
                raise RuntimeError("AI returned an unsafe image URL")
            telegram_call(
                "sendPhoto",
                {
                    "chat_id": str(chat_id),
                    "photo": safe_image,
                    "caption": "Generated image",
                },
            )
        elif image_bytes:
            telegram_upload(
                "sendPhoto",
                "photo",
                "streamly-generated.png",
                image_bytes,
                {"chat_id": str(chat_id), "caption": "Generated image"},
            )
        if image_request:
            edit_message(
                chat_id,
                status_id,
                f"<b>Image ready</b>\n\n{escape(text[:700] or 'ছবি তৈরি করা হয়েছে।')}",
            )
            return
        edit_message(
            chat_id,
            status_id,
            f"<b>AI assistant</b>\n\n{escape(text[:MAX_TEXT_LENGTH] or 'Response পাওয়া গেছে।')}",
        )
    except Exception as error:
        log("AI request failed: %s", error)
        edit_message(
            chat_id,
            status_id,
            "<b>AI assistant</b>\n\nএই মুহূর্তে উত্তর আনা যায়নি। কিছুক্ষণ পর আবার চেষ্টা করুন।",
        )


def welcome_text(first_name: str = "") -> str:
    greeting = f"স্বাগতম, {escape(first_name)}" if first_name else "স্বাগতম"
    return (
        f"<b>{greeting} — Streamly</b>\n\n"
        "Public TikTok video থেকে MP4 বা MP3 তৈরি করে Telegram-এ পাঠান। "
        "চাইলে AI assistant-ও ব্যবহার করতে পারবেন।\n\n"
        "<i>শুরু করতে নিচের একটি mode বেছে নিন।</i>"
    )


def help_text() -> str:
    return (
        "<b>Streamly কীভাবে ব্যবহার করবেন</b>\n\n"
        "১. <b>Download TikTok</b> চাপুন\n"
        "২. Public TikTok URL পাঠান\n"
        "৩. Quality বেছে নিন\n"
        "৪. MP4 video বা MP3 audio নির্বাচন করুন\n\n"
        f"সীমা: সর্বোচ্চ {MAX_VIDEO_SECONDS // 60} মিনিট এবং "
        f"{format_bytes(MAX_MEDIA_BYTES)}-এর মধ্যে ফাইল।\n\n"
        "Private, paid, age-restricted বা sign-in-only content bypass করা হয় না। "
        "শুধু নিজের বা অনুমোদিত content download করুন।"
    )



# -------------------- Multi-feature hub --------------------
FEATURE_USAGE: dict[int, dict[str, int]] = {}
USER_NOTES: dict[int, list[str]] = {}
USER_TODOS: dict[int, list[dict[str, Any]]] = {}
USER_BOOKMARKS: dict[int, list[str]] = {}
USER_AI_HISTORY: dict[int, list[dict[str, str]]] = {}
USER_STATS: dict[int, dict[str, Any]] = {}
USER_STREAK: dict[int, dict[str, Any]] = {}
USER_REMINDERS: dict[int, list[str]] = {}


def _user_stats(chat_id: int) -> dict[str, Any]:
    return USER_STATS.setdefault(chat_id, {"messages": 0, "downloads": 0, "ai": 0, "images": 0, "tools": 0, "points": 0, "xp": 0})


def _bump(chat_id: int, kind: str = "tools", points: int = 1) -> None:
    st = _user_stats(chat_id)
    st["messages"] += 1
    st[kind] = st.get(kind, 0) + 1
    st["points"] += points
    st["xp"] += points


def _feature_menu() -> str:
    return (
        "<b>🧩 Streamly Feature Center</b>\n\n"
        "📥 <b>Media</b>: /download /image\n"
        "🤖 <b>AI</b>: /ai /summarize /translate /rewrite /explain\n"
        "🎮 <b>Games</b>: /quiz /rps /guess /dice /choice\n"
        "👤 <b>Profile</b>: /profile /daily /streak /leaderboard\n"
        "🛠 <b>Tools</b>: /calc /convert /base64 /hash /uuid /json /textstats\n"
        "📚 <b>Productivity</b>: /note /todo /bookmark /reminder\n"
        "🌐 <b>Web</b>: /headers /dns /ssl /sitecheck /meta\n"
        "📢 <b>Telegram</b>: /id /chatid /rules /welcome\n\n"
        "<i>Use /features to see the complete command catalog.</i>"
    )


def _all_features_text() -> str:
    groups = {
        "Media": ["/download", "/image", "/history", "/favorites", "/quality"],
        "AI": ["/ai", "/summarize", "/translate", "/rewrite", "/explain", "/ideas", "/code", "/ask"],
        "Games": ["/quiz", "/rps", "/guess", "/dice", "/choice", "/fact", "/joke", "/wouldyou", "/challenge", "/leaderboard"],
        "Profile": ["/profile", "/daily", "/streak", "/points", "/level", "/stats", "/achievements", "/rank"],
        "Utilities": ["/calc", "/convert", "/base64", "/hash", "/uuid", "/json", "/textstats", "/timestamp", "/slug", "/reverse", "/sort", "/dedupe"],
        "Productivity": ["/note", "/notes", "/todo", "/todos", "/done", "/bookmark", "/bookmarks", "/reminder", "/template", "/countdown"],
        "Web": ["/headers", "/dns", "/ssl", "/sitecheck", "/meta", "/pingurl", "/redirects", "/urlinfo"],
        "Telegram": ["/id", "/chatid", "/rules", "/welcome", "/help", "/cancel", "/status", "/ping"],
        "Fun": ["/quote", "/motivate", "/random", "/pick", "/poll", "/wouldyou", "/truth", "/dare", "/number", "/coin"],
    }
    lines = ["<b>🚀 Streamly — 100+ Feature Catalog</b>", ""]
    for name, commands in groups.items():
        lines.append(f"<b>▸ {name}</b>")
        lines.append("  " + " · ".join(commands))
    lines.append("")
    lines.append("<i>Commands are processed locally where possible; network tools validate destinations before connecting.</i>")
    return "\n".join(lines)


def _safe_calc(expr: str) -> str:
    import ast, operator
    ops = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv,
           ast.FloorDiv: operator.floordiv, ast.Mod: operator.mod, ast.Pow: operator.pow, ast.USub: operator.neg,
           ast.UAdd: operator.pos}
    tree = ast.parse(expr, mode="eval")
    def ev(node: Any) -> Any:
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.BinOp) and type(node.op) in ops:
            a, b = ev(node.left), ev(node.right)
            if type(node.op) is ast.Pow and abs(b) > 100: raise ValueError("exponent too large")
            return ops[type(node.op)](a, b)
        if isinstance(node, ast.UnaryOp) and type(node.op) in ops:
            return ops[type(node.op)](ev(node.operand))
        raise ValueError("unsupported expression")
    value = ev(tree.body)
    if isinstance(value, float) and not value.is_integer(): return f"{value:.12g}"
    return str(int(value)) if isinstance(value, float) else str(value)


def _tool_command(chat_id: int, command: str, arg: str) -> str | None:
    import base64, hashlib, json as _json, random, re, uuid, urllib.parse, datetime
    _bump(chat_id)
    a = arg.strip()
    if command in {"/summarize","/translate","/rewrite","/explain","/ideas","/code","/ask"}:
        if not a: return f"Usage: <code>{command} your text</code>"
        st=_user_stats(chat_id); st["ai"] += 1
        return "__AI_ASYNC__"
    if command == "/leaderboard":
        top=sorted(USER_STATS.items(), key=lambda kv: kv[1].get("xp",0), reverse=True)[:10]
        if not top: return "🏆 No leaderboard data yet."
        return "🏆 <b>Top Streamly users</b>\n"+"\n".join(f"{i}. User <code>{uid}</code> — {st.get('xp',0)} XP" for i,(uid,st) in enumerate(top,1))
    if command in {"/urlinfo","/sitecheck","/headers","/dns","/ssl","/meta","/pingurl"}:
        if not a: return f"Usage: <code>{command} https://example.com</code>"
        url=normalize_url(a)
        safe=validate_remote_url(url) if url else None
        if not safe: return "❌ URL rejected by security validation."
        try:
            parsed=urllib.parse.urlsplit(safe)
            if command == "/urlinfo": return f"🔗 Scheme: <b>{parsed.scheme}</b>\nHost: <code>{escape(parsed.hostname or '')}</code>\nPath: <code>{escape(parsed.path or '/')}</code>\nPort: <b>{parsed.port or (443 if parsed.scheme=='https' else 80)}</b>"
            started=time.monotonic(); result=http_request("GET", safe, headers={"User-Agent": USER_AGENT, "Range":"bytes=0-0"}, timeout=10); elapsed=(time.monotonic()-started)*1000
            headers=result.headers
            if command in {"/sitecheck","/pingurl"}: return f"🌐 Status: <b>{result.status}</b>\n⏱ Response: <b>{elapsed:.0f} ms</b>"
            if command == "/headers": return "📋 <b>Response headers</b>\n"+"\n".join(f"<code>{escape(k)}</code>: {escape(v)[:300]}" for k,v in list(headers.items())[:30])
            if command == "/ssl": return f"🔒 TLS URL validated: <b>{parsed.scheme == 'https'}</b>\nHost: <code>{escape(parsed.hostname or '')}</code>"
            if command == "/meta": return f"🧾 Content-Type: <code>{escape(headers.get('Content-Type','unknown'))}</code>\nContent-Length: <code>{escape(headers.get('Content-Length','unknown'))}</code>"
            if command == "/dns":
                ips=sorted({item[4][0] for item in socket.getaddrinfo(parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM)})
                return "🧭 DNS addresses\n"+"\n".join(f"<code>{escape(ip)}</code>" for ip in ips[:20])
        except Exception as exc:
            return f"❌ Network tool failed: <code>{escape(str(exc)[:300])}</code>"
    if command == "/features": return _all_features_text()
    if command == "/calc":
        if not a: return "Usage: <code>/calc 12*(8+2)</code>"
        try: return f"🧮 <b>Result</b>: <code>{escape(_safe_calc(a))}</code>"
        except Exception: return "❌ Invalid or unsafe expression."
    if command == "/base64":
        if not a: return "Usage: <code>/base64 text</code> or <code>/base64 decode:SGVsbG8=</code>"
        try:
            if a.lower().startswith("decode:"):
                out=base64.b64decode(a[7:].strip(), validate=True).decode("utf-8")
                return f"🔓 <code>{escape(out[:3900])}</code>"
            return f"🔐 <code>{base64.b64encode(a.encode()).decode()}</code>"
        except Exception: return "❌ Base64 input is invalid."
    if command == "/hash":
        if not a: return "Usage: <code>/hash text</code>"
        return f"<b>SHA-256</b>\n<code>{hashlib.sha256(a.encode()).hexdigest()}</code>\n\n<b>MD5</b>\n<code>{hashlib.md5(a.encode()).hexdigest()}</code>"
    if command == "/uuid": return f"🆔 <code>{uuid.uuid4()}</code>"
    if command == "/reverse": return f"🔄 <code>{escape(a[::-1])}</code>" if a else "Usage: /reverse text"
    if command == "/sort":
        if not a: return "Usage: /sort apple,banana,apple,orange"
        return "🔢 " + ", ".join(sorted(x.strip() for x in a.split(",") if x.strip()))
    if command == "/dedupe":
        if not a: return "Usage: /dedupe apple,banana,apple"
        seen=[]
        for x in (x.strip() for x in a.split(",")):
            if x and x not in seen: seen.append(x)
        return "🧹 " + ", ".join(seen)
    if command == "/slug":
        if not a: return "Usage: /slug Your title here"
        slug=re.sub(r"[^a-z0-9]+", "-", a.lower()).strip("-")
        return f"🔗 <code>{escape(slug)}</code>"
    if command == "/textstats":
        if not a: return "Usage: /textstats your text"
        words=re.findall(r"\b\w+\b", a, flags=re.UNICODE)
        return f"📊 Characters: <b>{len(a)}</b>\nWords: <b>{len(words)}</b>\nLines: <b>{a.count(chr(10))+1}</b>\nUnique words: <b>{len(set(w.lower() for w in words))}</b>"
    if command == "/timestamp":
        if a:
            try: dt=datetime.datetime.fromisoformat(a.replace("Z","+00:00")); return f"🕒 Unix timestamp: <code>{int(dt.timestamp())}</code>"
            except Exception: pass
        return f"🕒 Current Unix timestamp: <code>{int(time.time())}</code>"
    if command == "/convert":
        m=re.fullmatch(r"\s*(-?\d+(?:\.\d+)?)\s*([a-zA-Z°]+)\s*(?:to|in)\s*([a-zA-Z°]+)\s*", a)
        if not m: return "Usage: <code>/convert 10 km to mile</code> or <code>/convert 100 c to f</code>"
        n=float(m.group(1)); u=m.group(2).lower().replace("°",""); v=m.group(3).lower().replace("°","")
        factors={("km","mile"):0.621371,("mile","km"):1.609344,("m","ft"):3.28084,("ft","m"):0.3048,("kg","lb"):2.20462262,("lb","kg"):0.45359237,("c","f"):("temp"),("f","c"):("temp")}
        key=(u,v)
        if key not in factors:return "❌ Supported: km↔mile, m↔ft, kg↔lb, C↔F"
        if factors[key]=="temp": out=n*9/5+32 if key==("c","f") else (n-32)*5/9
        else: out=n*factors[key]
        return f"🔄 <b>{n:g} {u}</b> = <b>{out:.6g} {v}</b>"
    if command in {"/note","/addnote"}:
        if not a:return "Usage: /note buy a notebook"
        USER_NOTES.setdefault(chat_id, []).append(a); USER_NOTES[chat_id]=USER_NOTES[chat_id][-50:]
        return "📝 Note saved. Use /notes to view."
    if command == "/notes":
        notes=USER_NOTES.get(chat_id,[])
        return "📝 <b>Your notes</b>\n"+"\n".join(f"{i+1}. {escape(x)}" for i,x in enumerate(notes)) if notes else "📝 No notes yet."
    if command == "/todo":
        if not a:return "Usage: /todo finish project"
        USER_TODOS.setdefault(chat_id,[]).append({"text":a,"done":False}); USER_TODOS[chat_id]=USER_TODOS[chat_id][-50:]
        return "✅ Todo added. Use /todos."
    if command == "/todos":
        ts=USER_TODOS.get(chat_id,[])
        return "📋 <b>Todos</b>\n"+"\n".join(f"{i+1}. {'☑️' if x['done'] else '⬜'} {escape(x['text'])}" for i,x in enumerate(ts)) if ts else "📋 No todos yet."
    if command == "/done":
        try:i=int(a)-1; USER_TODOS[chat_id][i]["done"]=True; return "☑️ Todo completed."
        except Exception:return "Usage: /done 1"
    if command == "/bookmark":
        if not a:return "Usage: /bookmark https://example.com"
        USER_BOOKMARKS.setdefault(chat_id,[]).append(a); USER_BOOKMARKS[chat_id]=USER_BOOKMARKS[chat_id][-50:]
        return "🔖 Bookmark saved."
    if command == "/bookmarks":
        b=USER_BOOKMARKS.get(chat_id,[])
        return "🔖 <b>Bookmarks</b>\n"+"\n".join(f"{i+1}. {escape(x)}" for i,x in enumerate(b)) if b else "🔖 No bookmarks yet."
    if command == "/profile":
        st=_user_stats(chat_id); level=st["xp"]//100+1
        return f"👤 <b>Your Streamly Profile</b>\n\n⭐ XP: <b>{st['xp']}</b>\n🏆 Level: <b>{level}</b>\n💰 Points: <b>{st['points']}</b>\n📥 Downloads: <b>{st['downloads']}</b>\n🤖 AI uses: <b>{st['ai']}</b>\n🛠 Tools: <b>{st['tools']}</b>"
    if command in {"/stats","/points","/level","/rank"}: return _tool_command(chat_id,"/profile","")
    if command == "/daily":
        today=time.strftime("%Y-%m-%d"); state=USER_STREAK.setdefault(chat_id,{"date":"","streak":0})
        if state["date"]!=today:
            yesterday=(datetime.date.today()-datetime.timedelta(days=1)).isoformat()
            state["streak"]=state["streak"]+1 if state["date"]==yesterday else 1; state["date"]=today; _user_stats(chat_id)["points"]+=10; _user_stats(chat_id)["xp"]+=10
            return f"🎁 Daily reward claimed! +10 points\n🔥 Streak: <b>{state['streak']}</b>"
        return "🎁 Daily reward already claimed today."
    if command == "/streak":
        return f"🔥 Current streak: <b>{USER_STREAK.get(chat_id,{}).get('streak',0)}</b>"
    if command == "/achievements":
        st=_user_stats(chat_id); badges=[]
        if st["messages"]>=10:badges.append("💬 Active User")
        if st["downloads"]>=5:badges.append("📥 Downloader")
        if st["ai"]>=5:badges.append("🤖 AI Explorer")
        if st["tools"]>=10:badges.append("🛠 Tool Master")
        return "🏅 <b>Achievements</b>\n"+"\n".join(badges or ["No badges yet — keep using Streamly!"])
    if command == "/rps":
        if a.lower() not in {"rock","paper","scissors","r","p","s"}: return "Usage: /rps rock|paper|scissors"
        user={"r":"rock","p":"paper","s":"scissors"}.get(a.lower(),a.lower()); bot=random.choice(["rock","paper","scissors"])
        win=(user,bot) in {("rock","scissors"),("paper","rock"),("scissors","paper")}
        return f"🎮 You: <b>{user}</b>\nBot: <b>{bot}</b>\n\n"+("🏆 You win! +5 XP" if win else "🤝 Draw!" if user==bot else "😄 Bot wins — try again!")
    if command == "/dice": return f"🎲 You rolled: <b>{random.randint(1,6)}</b>"
    if command == "/coin": return f"🪙 <b>{random.choice(['Heads','Tails'])}</b>"
    if command in {"/random","/number"}:
        if a:
            try:
                lo,hi=[int(x.strip()) for x in a.split(",",1)]; return f"🎲 <b>{random.randint(lo,hi)}</b>"
            except Exception: pass
        return f"🎲 Random number: <b>{random.randint(1,100)}</b>"
    if command in {"/choice","/pick"}:
        opts=[x.strip() for x in a.split(",") if x.strip()]
        return f"🎯 Picked: <b>{escape(random.choice(opts))}</b>" if opts else "Usage: /choice pizza,burger,pasta"
    if command == "/guess":
        return f"🎯 Guessing challenge: pick a number from <b>1–10</b>.\nTry: <code>/guess 7</code>" if not a else ("🎉 Correct!" if a.isdigit() and int(a)==random.randint(1,10) else "🙂 Not this time. Try again!")
    if command in {"/fact","/joke","/quote","/motivate"}:
        bank={"/fact":["Octopuses have three hearts.","A day on Venus is longer than its year."],"/joke":["Why did the computer take a break? It needed to refresh.","I told my code a joke; it returned an exception."],"/quote":["Small progress is still progress.","Build, test, learn, repeat."],"/motivate":["Keep learning one small thing at a time.","Your next project can teach you more than your last one."]}
        return "✨ "+random.choice(bank[command])
    if command in {"/wouldyou","/truth","/dare"}:
        bank={"/wouldyou":["Would you rather master coding or design first?","Would you rather build a game or an AI tool?"],"/truth":["What project do you most want to finish?","Which programming language do you enjoy most?"],"/dare":["Dare: write a tiny function in a language you are learning.","Dare: improve one part of your current project today."]}
        return "🎲 "+random.choice(bank[command])
    if command == "/challenge": return "🔥 Daily challenge: build one tiny feature, test it, and write down what you learned."
    if command == "/poll": return "📊 Quick poll idea:\n1️⃣ Yes\n2️⃣ No\n3️⃣ Maybe\n\nUse Telegram's native poll feature to collect votes."
    if command == "/id": return f"🆔 Your Telegram user ID: <code>{chat_id}</code>"
    if command == "/chatid": return f"💬 Chat ID: <code>{chat_id}</code>"
    if command == "/rules": return "📜 <b>Streamly rules</b>\n• Respect others\n• Don't spam\n• Only download content you are allowed to use\n• Don't send private credentials or secrets"
    if command == "/welcome": return "👋 Welcome to Streamly! Use /features to explore the tools."
    if command == "/countdown": return "⏳ Use <code>/countdown 2026-12-31 23:59</code> to calculate a countdown."
    if command == "/template": return "🧩 Templates:\n<code>/template email</code>\n<code>/template bug</code>\n<code>/template project</code>"
    if command == "/ping": return "🏓 Pong! Streamly is responding."
    return None

def process_message(message: dict[str, Any]) -> None:
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id is None:
        return
    text = (message.get("text") or "").strip()
    first_name = (message.get("from") or {}).get("first_name", "")
    if not text:
        send_message(chat_id, "Text URL পাঠান অথবা নিচের menu ব্যবহার করুন।", reply_markup=main_keyboard())
        return

    command = text.split(maxsplit=1)[0].lower()
    arg = text[len(command):].strip()
    feature_commands = {"/features","/calc","/base64","/hash","/uuid","/reverse","/sort","/dedupe","/slug","/textstats","/timestamp","/convert","/note","/addnote","/notes","/todo","/todos","/done","/bookmark","/bookmarks","/profile","/stats","/points","/level","/rank","/daily","/streak","/achievements","/rps","/dice","/coin","/random","/number","/choice","/pick","/guess","/fact","/joke","/quote","/motivate","/wouldyou","/truth","/dare","/challenge","/poll","/id","/chatid","/rules","/welcome","/countdown","/template","/ping","/summarize","/translate","/rewrite","/explain","/ideas","/code","/ask","/urlinfo","/sitecheck","/headers","/dns","/ssl","/meta","/pingurl","/leaderboard"}
    if command in feature_commands:
        result = _tool_command(chat_id, command, arg)
        if result == "__AI_ASYNC__":
            status = send_message(chat_id, "💭 Thinking....")
            prompt_map={"/summarize":"Summarize this clearly:","/translate":"Translate this into clear Bangla:","/rewrite":"Rewrite this professionally:","/explain":"Explain this simply:","/ideas":"Give practical ideas for:","/code":"Help me write safe code for:","/ask":"Answer this question:"}
            EXECUTOR.submit(send_ai_response, chat_id, prompt_map[command]+"\n"+arg, status["message_id"])
        elif result is not None:
            send_message(chat_id, result, reply_markup=main_keyboard() if command in {"/features","/profile","/help","/rules"} else None)
        return

    if command == "/start":
        CHAT_MODES[chat_id] = "home"
        send_message(chat_id, welcome_text(first_name), reply_markup=main_keyboard())
        return
    if command == "/help":
        send_message(chat_id, help_text(), reply_markup=main_keyboard())
        return
    if command in {"/cancel", "/stop"}:
        CHAT_MODES[chat_id] = "home"
        with STATE_LOCK:
            DOWNLOAD_OPTIONS.pop(chat_id, None)
        send_message(chat_id, "Cancelled. আবার শুরু করতে পারেন।", reply_markup=main_keyboard())
        return
    if command == "/download":
        CHAT_MODES[chat_id] = "platform"
        send_message(chat_id, "কোন platform-এর video download করবেন?", reply_markup=platform_keyboard())
        return
    if command == "/ai":
        prompt = text[len(command) :].strip()
        CHAT_MODES[chat_id] = "ai"
        if prompt:
            status = send_message(chat_id, "💭 Thinking....")
            EXECUTOR.submit(send_ai_response, chat_id, prompt, status["message_id"])
        else:
            send_message(chat_id, "AI mode চালু। আপনার প্রশ্ন লিখুন।")
        return
    if command == "/image":
        prompt = text[len(command) :].strip()
        if not prompt:
            send_message(chat_id, "এভাবে লিখুন:\n<code>/image a futuristic city at night</code>")
            return
        status = send_message(chat_id, "AI image তৈরি করছি…")
        EXECUTOR.submit(send_ai_response, chat_id, f"/image {prompt}", status["message_id"])
        return

    mode = CHAT_MODES.get(chat_id, "home")
    detected = platform_from_url(normalize_url(text) or "")
    if detected and mode in {"home", "ai"}:
        if mode == "ai":
            CHAT_MODES[chat_id] = "home"
        CHAT_PLATFORMS[chat_id] = detected.key
        mode = "awaiting_url"

    if mode == "platform":
        send_message(chat_id, "আগে TikTok বেছে নিন।", reply_markup=platform_keyboard())
        return
    if mode == "awaiting_url":
        url = normalize_url(text)
        platform = PLATFORM_BY_KEY.get(CHAT_PLATFORMS.get(chat_id, ""))
        detected = platform_from_url(url or "") if url else None
        if not url or not platform or not detected or detected.key != platform.key:
            send_message(
                chat_id,
                "এটি valid public TikTok URL মনে হচ্ছে না। আবার URL পাঠান।",
                reply_markup=platform_keyboard(),
            )
            return
        with STATE_LOCK:
            if chat_id in ACTIVE_CHATS:
                send_message(chat_id, "আপনার আগের download এখনও চলছে। একটু অপেক্ষা করুন।")
                return
            ACTIVE_CHATS.add(chat_id)
        status = send_message(chat_id, "<b>🎵 TikTok</b>\n\nDownload শুরু করছি…")
        CHAT_MODES[chat_id] = "home"
        EXECUTOR.submit(resolve_download, chat_id, status["message_id"], url)
        return
    if mode == "ai":
        status = send_message(chat_id, "💭 Thinking....")
        EXECUTOR.submit(send_ai_response, chat_id, text, status["message_id"])
        return
    send_message(chat_id, "একটি mode বেছে নিন—TikTok download বা AI assistant।", reply_markup=main_keyboard())


def process_callback(callback: dict[str, Any]) -> None:
    callback_id = callback.get("id", "")
    data = callback.get("data", "")
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")
    answer_callback(callback_id)
    if chat_id is None or message_id is None:
        return

    if data == "home":
        CHAT_MODES[chat_id] = "home"
        edit_message(chat_id, message_id, welcome_text(), reply_markup=main_keyboard())
    elif data == "help":
        edit_message(chat_id, message_id, help_text(), reply_markup=main_keyboard())
    elif data == "cancel":
        CHAT_MODES[chat_id] = "home"
        with STATE_LOCK:
            DOWNLOAD_OPTIONS.pop(chat_id, None)
        edit_message(chat_id, message_id, "Cancelled. আবার শুরু করতে পারেন।", reply_markup=main_keyboard())
    elif data == "mode:download":
        CHAT_MODES[chat_id] = "platform"
        edit_message(chat_id, message_id, "কোন platform-এর video download করবেন?", reply_markup=platform_keyboard())
    elif data == "mode:ai":
        CHAT_MODES[chat_id] = "ai"
        edit_message(chat_id, message_id, "AI assistant mode চালু। আপনার প্রশ্ন লিখুন।")
    elif data == "platform:tiktok":
        CHAT_MODES[chat_id] = "awaiting_url"
        CHAT_PLATFORMS[chat_id] = "tiktok"
        edit_message(
            chat_id,
            message_id,
            "<b>🎵 TikTok selected</b>\n\nএখন public video URL পাঠান।",
            reply_markup=inline_keyboard(
                [
                    [{"text": "Change platform", "callback_data": "mode:download"}],
                    [{"text": "Cancel", "callback_data": "cancel"}],
                ]
            ),
        )
    elif data == "back:quality":
        item = DOWNLOAD_OPTIONS.get(chat_id)
        if not item or time.time() - float(item.get("created_at", 0)) > STATE_TTL_SECONDS:
            edit_message(chat_id, message_id, "Quality options expired। আবার link পাঠান।", reply_markup=platform_keyboard())
            return
        CHAT_MODES[chat_id] = "quality"
        edit_message(
            chat_id,
            message_id,
            f"<b>🎚 Quality নির্বাচন করুন</b>\n\n{escape(item['title'][:100])}",
            reply_markup=quality_keyboard(item["options"]),
        )
    elif data.startswith("quality:"):
        item = DOWNLOAD_OPTIONS.get(chat_id)
        if item and time.time() - float(item.get("created_at", 0)) > STATE_TTL_SECONDS:
            item = None
            with STATE_LOCK:
                DOWNLOAD_OPTIONS.pop(chat_id, None)
        try:
            index = int(data.split(":", 1)[1])
            option = item["options"][index] if item else None
        except (ValueError, IndexError, TypeError, KeyError):
            option = None
        if not option:
            edit_message(chat_id, message_id, "এই quality selection-টি আর active নেই। আবার URL দিন.", reply_markup=platform_keyboard())
            return
        option = dict(option)
        option["title"] = item["title"]
        option["source_url"] = item["source_url"]
        with STATE_LOCK:
            DOWNLOAD_OPTIONS[chat_id]["selected"] = option
        CHAT_MODES[chat_id] = "format"
        edit_message(
            chat_id,
            message_id,
            f"<b>✅ Quality selected</b>\n\n{escape(option['label'])}\n\nএখন output format নির্বাচন করুন:",
            reply_markup=format_keyboard(),
        )
    elif data.startswith("format:"):
        output_format = data.split(":", 1)[1].lower()
        item = DOWNLOAD_OPTIONS.get(chat_id) or {}
        selected = item.get("selected")
        if output_format not in {"mp3", "mp4"} or not selected:
            edit_message(chat_id, message_id, "এই selection-টি আর active নেই। আবার link দিন।", reply_markup=platform_keyboard())
            return
        option = dict(selected)
        option["output_format"] = output_format
        CHAT_MODES[chat_id] = "home"
        EXECUTOR.submit(send_download, chat_id, message_id, item["source_url"], option)


def polling_loop() -> None:
    offset = 0
    backoff = 2
    while not SHUTDOWN.is_set():
        try:
            updates = telegram_call(
                "getUpdates",
                {
                    "offset": str(offset),
                    "timeout": "25",
                    "allowed_updates": json.dumps(["message", "callback_query"]),
                },
            )
            backoff = 2
            for update in updates or []:
                offset = max(offset, int(update.get("update_id", 0)) + 1)
                try:
                    if update.get("callback_query"):
                        process_callback(update["callback_query"])
                    elif update.get("message"):
                        process_message(update["message"])
                except Exception:
                    LOG.exception("Update handling failed")
            cleanup_state()
        except Exception as error:
            log("Polling error: %s", error)
            SHUTDOWN.wait(backoff)
            backoff = min(backoff * 2, 30)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in {"/", "/healthz", "/health"}:
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(
            {
                "ok": True,
                "service": "streamly",
                "tiktok": True,
                "active_downloads": len(ACTIVE_CHATS),
                "executor_workers": MAX_CONCURRENT_DOWNLOADS,
                "uptime": int(time.monotonic()),
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        return


def start_health_server() -> ThreadingHTTPServer:
    port = int(os.getenv("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, name="health-server", daemon=True)
    thread.start()
    log("Health server listening on 0.0.0.0:%s", port)
    return server


# ---------------------------------------------------------------------------
# Optional health self-ping. Platform sleep behavior is provider-dependent.
# ---------------------------------------------------------------------------
KEEP_ALIVE_MIN_SECONDS = 12 * 60   # 12 minutes
KEEP_ALIVE_MAX_SECONDS = 14 * 60   # 14 minutes


def _keep_alive_url() -> str | None:
    """
    Figure out the public URL to ping. Render automatically sets
    RENDER_EXTERNAL_URL for web services — no manual config needed there.
    KEEP_ALIVE_URL can be set manually to override/for other hosts.
    """
    explicit = os.getenv("KEEP_ALIVE_URL", "").strip()
    if explicit:
        return explicit.rstrip("/") + "/healthz"
    render_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if render_url:
        return render_url.rstrip("/") + "/healthz"
    return None


def keep_alive_loop() -> None:
    url = _keep_alive_url()
    if not url:
        log(
            "Keep-alive disabled: no RENDER_EXTERNAL_URL or KEEP_ALIVE_URL "
            "found in environment."
        )
        return
    log("Keep-alive enabled — pinging %s every ~12-14 minutes", url)
    while not SHUTDOWN.wait(random.uniform(KEEP_ALIVE_MIN_SECONDS, KEEP_ALIVE_MAX_SECONDS)):
        try:
            status, _, _ = http_request(url, method="GET", timeout=20, max_bytes=4096)
            log("Keep-alive ping ok (HTTP %s)", status)
        except Exception as error:
            log("Keep-alive ping failed: %s", error)


def start_keep_alive() -> None:
    thread = threading.Thread(target=keep_alive_loop, name="keep-alive", daemon=True)
    thread.start()


def shutdown_handler(_signum: int, _frame: Any) -> None:
    log("Shutdown signal received")
    SHUTDOWN.set()


def main() -> None:
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TOKEN secret is missing")
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    health_server = start_health_server()
    start_keep_alive()
    try:
        try:
            telegram_call("deleteWebhook", {"drop_pending_updates": "false"})
            me = telegram_call("getMe")
        except Exception as error:
            LOG.error("Telegram authentication/startup failed: %s", error)
            LOG.error("Check the TOKEN environment variable and BotFather token; the process will retry on restart.")
            return
        log("Bot connected as @%s", me.get("username", "unknown"))
        polling_thread = threading.Thread(target=polling_loop, name="telegram-polling", daemon=True)
        polling_thread.start()
        while not SHUTDOWN.wait(1):
            pass
    finally:
        SHUTDOWN.set()
        health_server.shutdown()
        EXECUTOR.shutdown(wait=False, cancel_futures=True)
        log("Streamly stopped")

if __name__ == "__main__":
    main()
