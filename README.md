# Horse Racing Scraper

Personal Python project to scrape daily horse‑racing data from Unibet.fr, execute a bookmarklet 3 minutes before post‑time, and surface everything via a FastAPI web UI.

## Quick start

```bash
# Install dependencies
poetry install

# Install Playwright browser (once)
poetry run playwright install chromium

# Run dev server
poetry run uvicorn app.main:app --reload
```

The scheduler will print "tick" once per minute so you can verify it’s alive.

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `sqlite:///./data.sqlite` | DB connection string |
| `TZ` | `Europe/Paris` | Time zone for scheduler |

Create a `.env` file in the project root to override.

## Deployment

See `docs/technical_spec.md` for the spec and the Railway section for deployment steps.