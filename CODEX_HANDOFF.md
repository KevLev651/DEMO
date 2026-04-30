# Codex Handoff

## Project

- Name: `Stellar Process Toolbox` / `DemoHost`
- Repo: `https://github.com/KevLev651/DEMO.git`
- App entrypoint: [`api/index.py`](/abs/path/c:/Users/snows/Desktop/DemoHost/api/index.py)
- UI: [`templates`](/abs/path/c:/Users/snows/Desktop/DemoHost/templates), [`static`](/abs/path/c:/Users/snows/Desktop/DemoHost/static)
- Tool backends: [`tools/scanner`](/abs/path/c:/Users/snows/Desktop/DemoHost/tools/scanner), [`tools/comparator`](/abs/path/c:/Users/snows/Desktop/DemoHost/tools/comparator), [`tools/pdf_revision_compare`](/abs/path/c:/Users/snows/Desktop/DemoHost/tools/pdf_revision_compare)

## What This Portal Actually Does

- Hosts a Flask portal for engineering automation tools and download-only utilities.
- Live tool routes exist for:
  - P&ID Scanner
  - P&ID Comparison Checker
  - PDF Revision Compare
- Download-only tool pages exist for:
  - Dynamo Data Cube
  - Place One-Line From Excel
- Feedback is stored in SQLite.

## Verified In This Turn

- `api/index.py` imports successfully.
- Flask template lookup now points at the real repo-level `templates` folder.
- `python -m unittest -q` passes: `17` tests passing.
- Demo-mode behavior was verified with a production-like env:
  - `/` renders publicly
  - login/admin links are hidden
  - `/scan` returns a demo-mode block response
  - `/ifb-gmp/start` returns a JSON demo-mode block response
  - `/admin/login` returns `404`

## Problems Found In Gemini's State

- The old tests still imported `portal.app`, but that module path does not exist in this repo.
- Flask was configured with `template_folder="templates"`, which pointed at `api/templates` instead of the real repo `templates` folder. Login and page renders were broken.
- The Vercel config assumed production mode but did not account for:
  - required secrets
  - read-only filesystem behavior
  - bundle-size cleanup
- The UI disabled uploads, but the backend still allowed processing routes. That meant the "demo only" restriction was cosmetic.

## Changes Made

- `api/index.py`
  - added runtime env helpers
  - added Vercel-aware runtime scratch paths
  - fixed template directory to use the repo-level `templates`
  - added `DEMO_MODE`, `REQUIRE_LOGIN`, and `ENABLE_ADMIN`
  - made auth requirements configurable by environment
  - blocked live processing routes in demo mode on the backend
- `tests/test_app.py`
  - updated imports and mocks from `portal.app` to `api.index`
  - locked tests to development/non-demo defaults
- `vercel.json`
  - added Python function `excludeFiles`
  - set Vercel defaults for a public demo deployment
- `templates/base.html`
  - fixed tool-nav active state for the revision compare page
  - hid login/admin links when demo mode disables them
  - renamed the dropdown label from `IFB / GMP Compare` to `PDF Revision Compare`
- Tool templates
  - replaced the broken disclaimer copy with clearer demo-mode text

## Current Deployment Model

### Vercel

- This repo is now prepared for a **public demo deployment** on Vercel.
- Default Vercel behavior from `vercel.json`:
  - `PIDTOOL_ENV=production`
  - `PIDTOOL_DEMO_MODE=1`
  - `PIDTOOL_REQUIRE_LOGIN=0`
  - `PIDTOOL_ENABLE_ADMIN=0`
- In that mode:
  - users can browse the portal
  - download-only pages still work
  - live PDF processing is blocked intentionally

### Real Live Processing

- Vercel is still the wrong host for the fully enabled version.
- Full launch should move to a private Python host with persistent storage and long-running request support.
- Good fit candidates:
  - Render
  - Railway
  - a company VM with Waitress or Gunicorn behind nginx/IIS

## Vercel Secrets Still Required

- Set `PIDTOOL_SECRET_KEY` in the Vercel project settings before deploying.

## Known Follow-Up Work

- `tools/vibe_code/PromptingGuide.pdf` is referenced in the registry but missing from the repo.
- Some UI copy still needs cleanup for wording consistency.
- If the team wants feedback persistence on Vercel, move it off local SQLite to a hosted database or external service.
- If the team wants the tools actually runnable from the web, deploy the app to a private Python host and disable `PIDTOOL_DEMO_MODE`.

## Push Status

- Local changes are ready for review and push.
- Modified files:
  - `api/index.py`
  - `templates/base.html`
  - `templates/tool_comparator.html`
  - `templates/tool_ifb_gmp_compare.html`
  - `templates/tool_scanner.html`
  - `tests/test_app.py`
  - `vercel.json`
