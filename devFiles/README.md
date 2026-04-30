# Stellar Process Tools

Internal Flask portal for Stellar Engineering Process tools.

The portal has two executable web tools:

- P&ID Scanner: upload one PDF and download an Excel tag report.
- P&ID Comparison Checker: upload one PDF and one `.xlsx` tag list and download annotated outputs.

It also hosts download-only Dynamo/Excel tools:

- Dynamo Data Cube scripts.
- Place One-Line From Excel script and Excel template.

## Project Layout

```text
web_host_code/              Flask application and PythonAnywhere WSGI entrypoint
web_files/                  Jinja templates
images_videos/img/          Logo and placeholder image assets
images_videos/videos/       Bundled example videos
tool_files/                 Scanner/comparator code and downloadable tool files
temporary_memory/           Ignored runtime job output folder
feedback.json               Optional legacy/seed feedback import
requirements.txt            Python dependencies
runtime.txt                 Target Python runtime marker
```

## Python Version

Target Python version: **3.12**.

The app may run on newer local Python versions, but Python 3.12 is the conservative deployment target for PythonAnywhere-style hosting.

## Local Setup

From the project root:

```powershell
cd C:\Users\snows\Desktop\AwesomeTools\StellarProcessApp
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python web_host_code\app.py
```

Open:

```text
http://127.0.0.1:5000/login
```

The development server is for local testing only.

## Environment Variables

Set these in production host configuration. Do not commit real secrets.

```text
PIDTOOL_SECRET_KEY           Required in production for stable sessions
PIDTOOL_TEAM_USERNAME        Team login username
PIDTOOL_TEAM_HASH            Werkzeug password hash for team login
PIDTOOL_ADMIN_USERNAME       Admin login username
PIDTOOL_ADMIN_HASH           Werkzeug password hash for admin login
PIDTOOL_MAX_UPLOAD_MB        Optional upload limit, default 512
PIDTOOL_JOB_RETENTION_HOURS  Optional cleanup window, default 24
PIDTOOL_FEEDBACK_DB_PATH     Optional SQLite feedback DB path
PIDTOOL_SHEET_FALLBACKS      Optional sheet fallback map, default DG7002:6
PIDTOOL_ENV                  Set to production on hosted deployments
```

When `PIDTOOL_ENV=production`, the app refuses to start unless `PIDTOOL_SECRET_KEY`, `PIDTOOL_TEAM_HASH`, and `PIDTOOL_ADMIN_HASH` are set.

To generate a password hash:

```powershell
.venv\Scripts\python.exe -c "from werkzeug.security import generate_password_hash; print(generate_password_hash('YOUR_PASSWORD'))"
```

## Runtime Data

- `temporary_memory/` is ignored and used for generated upload/output jobs.
- `feedback.sqlite3` is the default SQLite feedback store and is ignored by Git.
- `feedback.json` is kept as a legacy/seed import. If the SQLite feedback table is empty, valid entries from `feedback.json` are imported once.
- `images_videos/videos/` currently contains bundled demo videos. If admin-uploaded videos become production data, back them up or move them to durable storage.

## Git Workflow

Use GitHub as the recovery point for stable project states:

```powershell
git status
git add .
git commit -m "Describe the change"
git push
```

Committed and pushed source files can be recovered from Git history. Ignored runtime files such as `.venv/`, `temporary_memory/`, and cache folders are not backed up by Git.

## Deployment Notes

The PythonAnywhere WSGI entrypoint is:

```text
web_host_code/pythonanywhere_wsgi.py
```

Recommended deployment rhythm:

```text
develop locally -> run tests -> browser check -> commit -> push -> pull/reload host
```

Before reloading the hosted app, verify locally:

- `/login` renders.
- Static assets under `/static/...` load.
- Authenticated portal pages render.
- Download-only tool files are available.
- Scanner/comparator upload flows work with representative files.

Use a production branch or manual deployment step when the site becomes business-critical. Avoid automatically deploying every development commit directly to the live site.

## Tests

Run the standard-library test suite from the project root:

```powershell
.venv\Scripts\python.exe -m unittest discover -v
```

The current tests cover route rendering, static assets, login CSRF, feedback persistence, and download path rejection. Scanner/comparator PDF fixture tests should be added once representative sample files are available.

## Media Notes

The bundled MP4 example videos are tracked as source/demo assets. They should be compressed or moved to durable media storage if they grow or change often. This environment does not currently include `ffmpeg`, so video recompression was not performed here.

## Current Design Decisions

- Scanner/comparator processing remains synchronous because the current app is small and internal. Move to background jobs if PDFs become large or multiple users process files at the same time.
- Dependency ranges are kept in `requirements.txt` for now. Add a lock or constraints file when exact production reproducibility becomes necessary.
- P&ID sheet fallback mapping is configurable with `PIDTOOL_SHEET_FALLBACKS`; the default preserves the original `DG7002:6` behavior.
- Shared P&ID validation and sheet mapping live in `tool_files/pid_common.py`.
- CSRF protection is enabled for POST routes. Existing GET logout routes remain for compatibility, but templates now use POST logout.

## Adding A Tool

Add or edit portal tools in `TOOL_REGISTRY` inside `web_host_code/app.py`.

The registry drives:

- navigation labels
- portal cards
- feedback source labels
- example video slots
- download-only tool pages
