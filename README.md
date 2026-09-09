# Streamly Bot — Multi-Feature Upgrade

This build extends the existing Streamly Telegram bot with a mixed feature system instead of adding only downloader variants.

## Included categories
- TikTok MP4/MP3 downloader with progress
- AI assistant, image flow, summarize/translate/rewrite/explain/ideas/code/ask modes
- Games and random utilities
- User profile, XP, points, daily reward, streaks, achievements, leaderboard
- Calculator, unit converter, Base64, hashes, UUID, text statistics, slug/reverse/sort/dedupe
- Notes, todos, bookmarks, templates, countdown helper
- URL inspection, safe site check, headers, DNS, TLS validation, metadata and latency tools
- Telegram ID/chat utilities and rules
- Existing security validation, retries, health server and Render/Docker support

## Environment
Set `TOKEN` to a valid Telegram bot token. The bot cannot operate with an invalid token.

The existing Meta AI fallback configuration is preserved.

## Run
```bash
python app.py
```

## Docker
```bash
docker build -t streamly .
docker run -e TOKEN=YOUR_TOKEN -p 10000:10000 streamly
```
