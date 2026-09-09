# Streamly V3 — Inline Button Edition

Streamly V3 keeps the existing TikTok downloader/AI backend and replaces the normal feature-discovery UX with inline Telegram buttons.

## Highlights
- Inline-button home dashboard and category menus
- TikTok MP4/MP3 flow with real quality selection
- AI chat, summarize, translate, rewrite, explain, ideas, code and ask flows
- AI image prompt flow
- Games, profile, XP, streak and leaderboard
- Calculator, converter, Base64, hashing, UUID, JSON, text tools
- Notes, todos, bookmarks, reminders, templates and countdown
- Safe web/URL inspection tools with SSRF validation
- Native Telegram polls
- Download history
- Persistent-in-process quiz and guess game state
- Reminder worker while the process is running
- Legacy slash commands remain compatible, but they are no longer required for normal use
- No third-party Python runtime dependency

## Environment
Set `TOKEN` to a valid Telegram BotFather token. The existing Meta AI key behavior is preserved through `META_AI_API_KEY` with the existing fallback value.

## Run
```bash
python app.py
```

## Docker
```bash
docker build -t streamly-v3 .
docker run -e TOKEN="YOUR_BOT_TOKEN" -p 8080:8080 streamly-v3
```

User notes/todos/history/reminders are process-memory in this V3 build; a restart clears those in-memory records. The downloader security validation and Telegram error handling remain enabled.
