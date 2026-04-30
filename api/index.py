import json
import logging
import os
import secrets
import shutil
import sys
import sqlite3
import tempfile
import uuid
import zipfile
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from contextlib import contextmanager
from functools import wraps
from pathlib import Path, PurePosixPath
import threading

from flask import (
    Flask,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from tools.comparator.comparator import run_comparison
from tools.scanner.scanner import run_scan

IS_VERCEL = bool(os.environ.get("VERCEL") or os.environ.get("VERCEL_URL"))
IS_PRODUCTION = os.environ.get("PIDTOOL_ENV", "development").strip().lower() == "production"


def env_bool(name, default=False):
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


_runtime_root_raw = os.environ.get("PIDTOOL_RUNTIME_ROOT")
if _runtime_root_raw:
    RUNTIME_ROOT = Path(_runtime_root_raw).expanduser()
elif IS_VERCEL:
    RUNTIME_ROOT = Path(tempfile.gettempdir()) / "stellar-process-toolbox"
else:
    RUNTIME_ROOT = BASE_DIR

STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"
VIDEO_DIR = STATIC_DIR / "videos"
FEEDBACK_PATH = BASE_DIR / "data" / "feedback.json"
_feedback_db_raw_path = os.environ.get("PIDTOOL_FEEDBACK_DB_PATH")
FEEDBACK_DB_PATH = (
    Path(_feedback_db_raw_path).expanduser()
    if _feedback_db_raw_path
    else RUNTIME_ROOT / "data" / "feedback.sqlite3"
)
if not FEEDBACK_DB_PATH.is_absolute():
    FEEDBACK_DB_PATH = BASE_DIR / FEEDBACK_DB_PATH
FEEDBACK_UPLOAD_DIR = RUNTIME_ROOT / "data" / "feedback_uploads"
MAX_FEEDBACK_ATTACHMENTS = 8
JOBS_DIR = RUNTIME_ROOT / "runs"
PDF_COMPARE_ROOT = Path(
    os.environ.get("IFB_GMP_COMPARE_ROOT", r"C:\Users\snows\Desktop\PDFCompare")
).expanduser().resolve()
IFB_GMP_PUBLIC_ARTIFACTS = (
    ("bluebeam_review.pdf", "Bluebeam Review PDF"),
    ("report.xlsx", "Excel Report"),
    ("changed_pairs.zip", "Changed Pairs ZIP"),
)
IFB_GMP_PUBLIC_ARTIFACT_NAMES = {name for name, _label in IFB_GMP_PUBLIC_ARTIFACTS}
IFB_GMP_WORKER_COUNT = 1
IFB_GMP_EXECUTOR = ThreadPoolExecutor(
    max_workers=IFB_GMP_WORKER_COUNT,
    thread_name_prefix="ifb-gmp-web",
)
IFB_GMP_RUNS = {}
IFB_GMP_RUNS_LOCK = threading.Lock()

REVISION_COMPARE_DIR = BASE_DIR / "tools" / "pdf_revision_compare"
if REVISION_COMPARE_DIR.parent.exists() and str(REVISION_COMPARE_DIR.parent) not in sys.path:
    sys.path.insert(0, str(REVISION_COMPARE_DIR.parent))

try:
    from tools.pdf_revision_compare.core import run_compare_job as run_ifb_gmp_compare_job
    IFB_GMP_IMPORT_ERROR = None
except Exception as exc:  # noqa: BLE001
    run_ifb_gmp_compare_job = None
    IFB_GMP_IMPORT_ERROR = exc


def env_int(name, default, minimum=None):
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    if minimum is not None:
        return max(value, minimum)
    return value


JOB_RETENTION_HOURS = env_int("PIDTOOL_JOB_RETENTION_HOURS", 24, minimum=1)
DEMO_MODE = env_bool("PIDTOOL_DEMO_MODE", IS_VERCEL and IS_PRODUCTION)
REQUIRE_LOGIN = env_bool("PIDTOOL_REQUIRE_LOGIN", not DEMO_MODE)
ENABLE_ADMIN = env_bool("PIDTOOL_ENABLE_ADMIN", not DEMO_MODE)
TEAM_USERNAME = os.environ.get("PIDTOOL_TEAM_USERNAME", "StellarProcess")
TEAM_HASH = os.environ.get("PIDTOOL_TEAM_HASH")
ADMIN_USERNAME = os.environ.get("PIDTOOL_ADMIN_USERNAME", "Kev123")
ADMIN_HASH = os.environ.get("PIDTOOL_ADMIN_HASH")
PUBLIC_ENDPOINTS = {"team_login", "admin_login", "static"}
ALLOWED_VIDEO_EXT = (".mp4", ".webm", ".mov")
VIDEO_MIME_TYPES = {
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".mov": "video/mp4",
}
# Add and edit portal tools here. The registry drives nav, portal cards,
# feedback sources, example videos, and download-only tool pages.
TOOL_REGISTRY = {
    "scanner": {
        "title": "P&ID Scanner",
        "nav_label": "Scanner",
        "feedback_label": "P&ID Scanner",
        "description": (
            "Scan one P&ID PDF and generate a structured Excel tag report "
            "for process controls review."
        ),
        "endpoint": "tool_scanner",
        "image": "img/placeholder_pid.svg",
        "image_aspect": "383 / 192",
        "image_surface": "bg-slate-950",
        "image_padding": "p-2",
        "portal_use": "You need an Excel report of instrument tags from a P&ID PDF.",
        "short_summary": (
            "Reads a P&ID PDF and exports a reviewable Excel tag report with "
            "confidence and page diagnostics."
        ),
        "detailed_explanation": (
            "The scanner is a first-pass P&ID tag takeoff tool. It reads one PDF, "
            "finds likely process-control tags, groups repeated hits, assigns confidence, "
            "and writes the results into an Excel workbook with scan results, summary, "
            "review queue, and page diagnostics."
        ),
        "what_how": (
            "Use the P&ID Scanner when you have one P&ID PDF and need a clean tag list "
            "to review in Excel. Upload the PDF, run the tool, then download the scan "
            "report. The workbook is meant to be a starting checklist for the controls "
            "team: it shows the tags the drawing appears to contain, where they were "
            "found, and how confident the tool is about each find. High-confidence rows "
            "are the strongest hits. Lower-confidence rows are still useful, but they "
            "belong in a human review pass before anyone treats them as final."
        ),
        "behind_scenes": (
            "Behind the scenes, the website gives the upload its own temporary job folder "
            "and calls the scanner on that PDF. For this web tool, the scanner reads the "
            "searchable text already inside the drawing pages. It looks for text shaped "
            "like common P&ID tags, including instrument tags, equipment tags, and line "
            "style identifiers, then throws away obvious noise and combines repeated finds. "
            "Each result gets evidence notes and a confidence rating. The Excel report is "
            "then built with a main Scan Results sheet, a Summary sheet, a Review Queue "
            "for anything less certain, and Page Diagnostics so a reviewer can tell which "
            "sheets produced useful text. The tool is not approving the design; it is doing "
            "the first pass of walking the drawing and writing down what it can see."
        ),
        "video_slots": [
            {
                "key": "scanner",
                "title": "P&ID Scanner",
                "placeholder": "img/placeholder_pid.svg",
                "placeholder_aspect": "383 / 192",
                "placeholder_surface": "bg-slate-950",
                "placeholder_padding": "p-2",
            }
        ],
        "status_label": "Ready",
    },
    "vibe_code": {
        "title": "Vibe Code Your Own Tools",
        "nav_label": "Vibe Code",
        "feedback_label": "Vibe Code Demo",
        "description": (
            "Learn how to use AI tools like Copilot to quickly generate your own "
            "automation tools and scripts."
        ),
        "url_slug": "vibe-code-demo",
        "launch_label": "Coming Soon",
        "endpoint": "index",
        "image": "img/vibe_coding_demo.png",
        "image_aspect": "1028 / 682",
        "image_surface": "bg-slate-950",
        "image_padding": "p-0",
        "portal_use": "You want to learn how to use AI to automate your own tedious tasks.",
        "short_summary": (
            "A quick guide and demo on using approved company AI tools to generate "
            "simple tools through natural language prompting."
        ),
        "detailed_explanation": (
            "Vibe coding is the process of using natural language and 'vibe-based' "
            "guidance to direct AI assistants in writing functional code. This demo "
            "shows you how to use tools like GitHub Copilot or ChatGPT to bridge "
            "the gap between a manual task and a finished automation script, even "
            "without deep programming knowledge."
        ),
        "behind_scenes": (
            "We show real-world examples of prompting: defining the input, the logic, "
            "and the desired output. By learning how to talk to the AI effectively, "
            "you can build simple Excel macros, Python scripts, or Dynamo nodes that "
            "save hours of work. It's about empowering everyone on the team to become "
            "a developer of their own productivity."
        ),
        "downloads": [
            {
                "slug": "prompt_guide",
                "label": "Simple Prompting Guide",
                "filename": "tools/vibe_code/PromptingGuide.pdf",
                "download_name": "PromptingGuide.pdf",
                "description": "A PDF guide on effective prompting for engineering tasks.",
            }
        ],
        "video_slots": [
            {
                "key": "vibe_code",
                "title": "Vibe Code Demo",
                "placeholder": "img/vibe_coding_demo.png",
                "placeholder_aspect": "1028 / 682",
                "placeholder_surface": "bg-slate-950",
                "placeholder_padding": "p-0",
            }
        ],
        "status_label": "Reserved",
    },
    "comparator": {
        "title": "P&ID Comparison Checker",
        "nav_label": "Comparator",
        "feedback_label": "P&ID Comparison Checker",
        "description": (
            "Compare one P&ID PDF against an Excel tag list and publish "
            "annotated review outputs."
        ),
        "endpoint": "tool_comparator",
        "image": "img/placeholder_comparator.svg",
        "image_aspect": "383 / 192",
        "image_surface": "bg-slate-950",
        "image_padding": "p-2",
        "portal_use": "You need annotated comparison outputs from a P&ID PDF and Excel tag list.",
        "short_summary": (
            "Checks an Excel P&ID tag list against one drawing PDF and returns "
            "annotated Excel and PDF review files."
        ),
        "detailed_explanation": (
            "The comparison checker is a tag-list-to-drawing review tool. It reads one "
            "P&ID PDF and one .xlsx tag list, lines up workbook tags with drawing text, "
            "colors high-confidence matches yellow, and everything else light red, "
            "then returns an annotated workbook and annotated PDF."
        ),
        "what_how": (
            "Use the P&ID Comparison Checker when an Excel tag list and a P&ID drawing "
            "are supposed to agree. Upload the PDF and the .xlsx file, run the comparison, "
            "then download the marked-up workbook and marked-up PDF. The reviewer can use "
            "the colors like a shop-floor check sheet: yellow means the tool found a strong "
            "match (High confidence), and light red means it found a discrepancy or "
            "low-confidence item that needs review."
        ),
        "behind_scenes": (
            "Behind the scenes, the website keeps the uploaded PDF and workbook together "
            "in one temporary job folder. The comparer reads the Excel rows like the expected "
            "tag checklist, then reads the searchable text from the PDF and tries to match "
            "drawing text back to workbook entries. It scores each match by how strong the "
            "evidence is, including whether the tag text agrees and whether the sheet context "
            "makes sense. The workbook receives comparison columns plus summary and candidate "
            "sheets. The PDF output removes old source annotations before writing this run's "
            "highlights, and the highlights are placed on the matched text spans instead of "
            "covering a whole area of the drawing."
        ),
        "video_slots": [
            {
                "key": "comparator",
                "title": "P&ID Comparison Checker",
                "placeholder": "img/placeholder_comparator.svg",
                "placeholder_aspect": "383 / 192",
                "placeholder_surface": "bg-slate-950",
                "placeholder_padding": "p-2",
            }
        ],
        "status_label": "Ready",
    },
    "pdf_revision_compare": {
        "title": "PDF Revision Compare",
        "nav_label": "PDF Revision Compare",
        "feedback_label": "PDF Revision Compare",
        "description": (
            "Compares revision A against revision B and finds BOM discrepancies before/after"
        ),
        "endpoint": "tool_pdf_revision_compare",
        "image": "img/ifb_gmp_compare_preview.svg",
        "image_aspect": "746 / 503",
        "image_surface": "bg-white",
        "image_padding": "p-0",
        "portal_use": "You need the Revision A and Revision B drawing packages.",
        "short_summary": (
            "Compares revision A against revision B and finds BOM discrepancies before/after"
        ),
        "detailed_explanation": (
            "The PDF Revision Compare tool finds discrepancies between two drawing "
            "sets. It accepts Revision A and Revision B folders or ZIP files, pairs matching "
            "sheets, runs the comparison in the background, and exposes only the final "
            "Bluebeam review PDF, Excel report, and changed-pairs ZIP."
        ),
        "what_how": (
            "Use PDF Revision Compare when a drawing package has moved from Revision A to "
            "Revision B and the team needs a BOM review package, not a generic PDF diff. Upload the "
            "Revision A set and the Revision B set as two ZIP files or two browser folders, start the run, "
            "and let it process in the background. When it finishes, download exactly three "
            "files: the Bluebeam review PDF for fast sheet review, the Excel report for the "
            "organized reviewer workbook, and the changed-pairs ZIP for detailed before-and-after "
            "sheet checks. Light blue marks added or new items, green marks removed "
            "items, red marks modified items, and pink marks relocated items."
        ),
        "behind_scenes": (
            "Behind the scenes, the website extracts or collects the Revision A PDFs into one folder "
            "and the Revision B PDFs into another, then starts the comparison engine as a background "
            "job so the page does not freeze. The engine pairs matching sheets first by strong "
            "drawing identifiers, such as sheet numbers, and then uses fallback clues when "
            "the naming is not perfect. After the sheets are paired, it looks for review-worthy "
            "scope changes: BOM-like tables, schedule rows, title block differences, "
            "added sheets, removed sheets, and meaningful visual drawing changes. The result is trimmed "
            "down to the three coworker-facing files only. Logs, manifests, working folders, and internal "
            "artifacts stay hidden from the public download buttons."
        ),
        "status_label": "Ready",
        "video_slots": [
            {
                "key": "ifb_gmp",
                "title": "PDF Revision Compare",
                "placeholder": "img/ifb_gmp_compare_preview.svg",
                "placeholder_aspect": "746 / 503",
                "placeholder_surface": "bg-white",
                "placeholder_padding": "p-0",
            },
        ],
    },
    "data_cube": {
        "title": "Dynamo Data Cube",
        "nav_label": "Data Cube",
        "feedback_label": "Dynamo Data Cube",
        "description": (
            "Download the Dynamo Data Cube Populate and Update scripts used "
            "with the Utility Matrix connector workflow."
        ),
        "url_slug": "data-cube",
        "image": "img/data_cube_preview.png",
        "image_aspect": "1030 / 562",
        "image_surface": "bg-white",
        "image_padding": "p-0",
        "portal_use": "You need the Dynamo scripts for Data Cube populate or update workflows.",
        "short_summary": (
            "Downloads the Dynamo scripts that insert missing Data Cube connectors "
            "and sync connector data from the Utility Matrix."
        ),
        "detailed_explanation": (
            "The Dynamo Data Cube tool provides the current Populate and Update Dynamo "
            "graphs. Populate reads the Utility Matrix and helps place missing process "
            "connector families. Update lets the user sync and move selected connector "
            "families while reporting what changed."
        ),
        "what_how": (
            "Use Dynamo Data Cube when the Utility Matrix is the source of truth for process "
            "connectors in a Revit model. Download Populate when the model needs missing "
            "connector families inserted and given their starting data. Download Update when "
            "the connectors already exist and the job is to sync changed Utility Matrix values "
            "or move selected connector families into position. In plain terms, Populate helps "
            "get the connector placeholders into the model, and Update helps keep those placeholders "
            "lined up with the spreadsheet."
        ),
        "behind_scenes": (
            "Behind the scenes, Script 1 reads the Utility Matrix Main sheet and the UM-Revit "
            "Mapping sheet. It keeps the connection rows the process team cares about, skips rows "
            "marked Obsolete, checks Connection ID values, and works with the connector placement "
            "logic to insert or identify the right connector families. It produces an insert report "
            "so the user can see what happened. Script 2 is the update pass. It lets the user choose "
            "which connector families to process, compares the Utility Matrix values to the Revit "
            "connector parameters, writes only the fields that need updating, can move selected "
            "connectors, and produces an update report. The website itself is the controlled download "
            "point; the actual model work happens when the coworker runs the Dynamo graph in Revit."
        ),
        "downloads": [
            {
                "slug": "populate",
                "label": "Dynamo Data Cube Populate",
                "filename": "tools/dynamo_data_cube/Script1.dyn",
                "download_name": "Dynamo Data Cube Populate.dyn",
                "description": "Dynamo script for populating the Data Cube.",
            },
            {
                "slug": "update",
                "label": "Dynamo Data Cube Update",
                "filename": "tools/dynamo_data_cube/Script2.dyn",
                "download_name": "Dynamo Data Cube Update.dyn",
                "description": "Dynamo script for updating the Data Cube.",
            },
        ],
        "video_slots": [
            {
                "key": "data_cube",
                "title": "Dynamo Data Cube",
                "placeholder": "img/placeholder_controls.svg",
                "placeholder_aspect": "1028 / 682",
                "placeholder_surface": "bg-white",
                "placeholder_padding": "p-0",
            },
        ],
    },
    "one_line": {
        "title": "Place One-Line From Excel",
        "nav_label": "One-Line",
        "feedback_label": "Place One-Line From Excel",
        "description": (
            "Download the Dynamo one-line placement script and motor data "
            "template for Excel-driven one-line work."
        ),
        "url_slug": "one-line-from-excel",
        "image": "img/one_line_example_backdrop.png",
        "image_aspect": "4536 / 3240",
        "image_surface": "bg-white",
        "image_padding": "p-0",
        "portal_use": "You need the one-line placement Dynamo script and motor data Excel template.",
        "short_summary": (
            "Downloads the Dynamo graph and Excel template for placing Revit "
            "one-line annotation families from an MCC Info table."
        ),
        "detailed_explanation": (
            "The Place One-Line From Excel tool provides the Dynamo graph and matching "
            "motor data workbook template for one-line annotation placement. The graph "
            "reads the MCC Info sheet, places matching Revit annotation families in the "
            "active view, and fills their parameters from the spreadsheet."
        ),
        "what_how": (
            "Use Place One-Line From Excel when one-line annotation work should come from a "
            "motor data spreadsheet instead of placing every item by hand. Download the Dynamo "
            "graph and the Excel template, fill or check the MCC Info table, open the target Revit "
            "view or sheet, make sure the needed generic annotation families are loaded, then run "
            "the graph. The FAMILY TYPE value in Excel needs to match the Revit family type name, "
            "because that is how the graph knows which annotation symbol to place."
        ),
        "behind_scenes": (
            "Behind the scenes, the Dynamo graph reads the MCC Info worksheet and treats the first "
            "row as the column headers. For each valid row, it looks at FAMILY TYPE, finds the matching "
            "generic annotation family symbol in Revit, places an instance in the active view, and "
            "fills common one-line fields such as equipment ID, MCC, unit number, description, motor "
            "horsepower, breaker information, and wire size. It spaces the placed symbols into rows so "
            "the view starts organized instead of piled on top of itself. Rows with no matching family "
            "type are skipped and reported. The website does not run Revit work directly; it keeps the "
            "graph and its matching workbook template together for a clean handoff."
        ),
        "downloads": [
            {
                "slug": "script",
                "label": "Place One-Line From Excel",
                "filename": "tools/place_one_line_from_excel/PlaceOneline_FromExcel.dyn",
                "download_name": "Place One-Line From Excel.dyn",
                "description": "Dynamo script for placing one-line motor data from Excel.",
            },
            {
                "slug": "template",
                "label": "OneLineMotorDataTemplate",
                "filename": "tools/place_one_line_from_excel/OneLineMotorDataTemplate.xlsm",
                "download_name": "OneLineMotorDataTemplate.xlsm",
                "description": "Excel macro template for one-line motor data.",
            },
        ],
        "video_slots": [
            {
                "key": "one_line",
                "title": "Place One-Line From Excel",
                "placeholder": "img/placeholder_piping.svg",
                "placeholder_aspect": "1207 / 747",
                "placeholder_surface": "bg-white",
                "placeholder_padding": "p-0",
            },
        ],
        "status_label": "Ready",
    },
}

FEEDBACK_SOURCE_LABELS = {
    "portal": "Home",
    **{
        slug: metadata.get("feedback_label", metadata["title"])
        for slug, metadata in TOOL_REGISTRY.items()
    },
}
VIDEO_SLOT_DETAILS = {
    slot["key"]: {
        "title": slot["title"],
        "placeholder": slot["placeholder"],
        "placeholder_aspect": slot.get("placeholder_aspect", "2 / 1"),
        "placeholder_surface": slot.get("placeholder_surface", "bg-slate-950"),
        "placeholder_padding": slot.get("placeholder_padding", "p-2"),
    }
    for metadata in TOOL_REGISTRY.values()
    for slot in metadata["video_slots"]
}
DOWNLOAD_TOOLS_BY_URL_SLUG = {
    metadata["url_slug"]: slug
    for slug, metadata in TOOL_REGISTRY.items()
    if metadata.get("downloads")
}

app = Flask(
    __name__,
    template_folder=str(TEMPLATES_DIR),
    static_folder=str(STATIC_DIR),
    static_url_path="/static",
)
if IS_PRODUCTION:
    required_config = {"PIDTOOL_SECRET_KEY": os.environ.get("PIDTOOL_SECRET_KEY")}
    if REQUIRE_LOGIN:
        required_config["PIDTOOL_TEAM_HASH"] = TEAM_HASH
    if ENABLE_ADMIN:
        required_config["PIDTOOL_ADMIN_HASH"] = ADMIN_HASH
    missing_config = [name for name, value in required_config.items() if not value]
    if missing_config:
        raise RuntimeError(
            "Missing required production configuration: "
            + ", ".join(missing_config)
        )

app.secret_key = os.environ.get("PIDTOOL_SECRET_KEY", "dev-only-change-me")
app.config["MAX_CONTENT_LENGTH"] = env_int(
    "PIDTOOL_MAX_UPLOAD_MB", 512, minimum=1
) * 1024 * 1024
app.logger.setLevel(logging.INFO)

RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
JOBS_DIR.mkdir(parents=True, exist_ok=True)

if TEAM_HASH is None and REQUIRE_LOGIN:
    TEAM_HASH = generate_password_hash("Jax2026")
    os.environ["PIDTOOL_TEAM_HASH"] = TEAM_HASH

if ADMIN_HASH is None and ENABLE_ADMIN:
    ADMIN_HASH = generate_password_hash("Jax2026")
    os.environ["PIDTOOL_ADMIN_HASH"] = ADMIN_HASH

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "team_login"
login_manager.login_message = None


class PortalUser(UserMixin):
    def __init__(self, role):
        self.id = role

    def get_id(self):
        return self.id

    @property
    def is_admin(self):
        return self.id == "admin"


@login_manager.user_loader
def load_user(user_id):
    if user_id in {"team", "admin"}:
        return PortalUser(user_id)
    return None


@app.before_request
def require_private_access():
    if not REQUIRE_LOGIN:
        return None
    if request.endpoint in PUBLIC_ENDPOINTS or current_user.is_authenticated:
        return None
    next_path = request.full_path if request.query_string else request.path
    if request.endpoint and request.endpoint.startswith("admin_"):
        return redirect(url_for("admin_login", next=next_path))
    return redirect(url_for("team_login", next=next_path))


def csrf_token():
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


@app.before_request
def validate_csrf_token():
    if request.method != "POST":
        return None

    submitted_token = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
    if submitted_token and secrets.compare_digest(submitted_token, csrf_token()):
        return None

    if request.is_json or request.endpoint == "feedback":
        return jsonify({"error": "Invalid CSRF token."}), 400
    abort(400)


@app.errorhandler(RequestEntityTooLarge)
def request_too_large(_error):
    limit_mb = max(1, app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024))
    message = (
        f"Upload is too large for this server. Current limit is {limit_mb} MB. "
        "Use smaller test packages or raise PIDTOOL_MAX_UPLOAD_MB for larger drawing sets."
    )
    if request.endpoint in {"ifb_gmp_start", "feedback"} or request.path.startswith("/ifb-gmp/"):
        return jsonify({"error": message}), 413
    return render_template("result.html", title="Upload Failed", error=message), 413


def admin_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not ENABLE_ADMIN:
            abort(404)
        if not current_user.is_authenticated or not current_user.is_admin:
            next_path = request.full_path if request.query_string else request.path
            return redirect(url_for("admin_login", next=next_path))
        return view_func(*args, **kwargs)

    return wrapped


def private_access_redirect():
    next_url = request.args.get("next")
    if next_url and next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("index"))


def admin_access_redirect():
    next_url = request.args.get("next")
    if next_url and next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("admin_dashboard"))


def demo_blocked_message(tool_label):
    return (
        f"{tool_label} is disabled in the public demo deployment. "
        "Use a private Python host for live processing or turn off PIDTOOL_DEMO_MODE."
    )


def demo_processing_response(tool_label, *, json_response=False):
    message = demo_blocked_message(tool_label)
    if json_response:
        return jsonify({"error": message}), 503
    return (
        render_template(
            "result.html",
            title="Demo Mode",
            error=message,
            back_tool_label=tool_label,
            back_tool_url=request.referrer or url_for("index"),
        ),
        503,
    )


def load_legacy_feedback_seed():
    if not FEEDBACK_PATH.exists():
        return []
    try:
        with FEEDBACK_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, FileNotFoundError):
        app.logger.warning("Legacy feedback JSON could not be loaded.", exc_info=True)
        return []

    if not isinstance(data, list):
        return []

    entries = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        message = str(entry.get("message") or "").strip()
        if not message:
            continue
        entries.append(
            {
                "id": str(entry.get("id") or uuid.uuid4()),
                "timestamp": str(
                    entry.get("timestamp")
                    or datetime.now(timezone.utc).isoformat()
                ),
                "source": normalize_feedback_source(entry.get("source")),
                "category": normalize_feedback_category(entry.get("category")),
                "message": message,
            }
        )
    return entries


@contextmanager
def feedback_connection():
    FEEDBACK_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(FEEDBACK_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def ensure_feedback_store():
    with feedback_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS feedback (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL,
                category TEXT NOT NULL,
                message TEXT NOT NULL
            )
            """
        )
        row_count = connection.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]
        if row_count == 0:
            seed_entries = load_legacy_feedback_seed()
            connection.executemany(
                """
                INSERT OR IGNORE INTO feedback
                    (id, timestamp, source, category, message)
                VALUES
                    (:id, :timestamp, :source, :category, :message)
                """,
                seed_entries,
            )


def load_feedback():
    ensure_feedback_store()
    with feedback_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, timestamp, source, category, message
            FROM feedback
            ORDER BY timestamp DESC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def save_feedback(entries):
    ensure_feedback_store()
    normalized_entries = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        message = str(entry.get("message") or "").strip()
        if not message:
            continue
        normalized_entries.append(
            {
                "id": str(entry.get("id") or uuid.uuid4()),
                "timestamp": str(
                    entry.get("timestamp")
                    or datetime.now(timezone.utc).isoformat()
                ),
                "source": normalize_feedback_source(entry.get("source")),
                "category": normalize_feedback_category(entry.get("category")),
                "message": message,
            }
        )

    with feedback_connection() as connection:
        connection.execute("DELETE FROM feedback")
        connection.executemany(
            """
            INSERT INTO feedback
                (id, timestamp, source, category, message)
            VALUES
                (:id, :timestamp, :source, :category, :message)
            """,
            normalized_entries,
        )


def add_feedback_entry(source, category, message):
    ensure_feedback_store()
    entry = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "category": category,
        "message": message.strip(),
    }
    with feedback_connection() as connection:
        connection.execute(
            """
            INSERT INTO feedback
                (id, timestamp, source, category, message)
            VALUES
                (:id, :timestamp, :source, :category, :message)
            """,
            entry,
        )
    return entry


def normalize_feedback_source(raw_source):
    source = str(raw_source or "portal").strip().lower()
    return source if source in FEEDBACK_SOURCE_LABELS else "portal"


def normalize_feedback_category(raw_category):
    category = str(raw_category or "General Comment").strip().lower()
    category_map = {
        "bug / issue report": "Bug / Issue Report",
        "bug": "Bug / Issue Report",
        "issue": "Bug / Issue Report",
        "issue report": "Bug / Issue Report",
        "problem": "Bug / Issue Report",
        "feature request": "Feature Request",
        "feature": "Feature Request",
        "suggestion": "Feature Request",
        "workflow improvement": "Workflow Improvement",
        "workflow": "Workflow Improvement",
        "improvement": "Workflow Improvement",
        "tool recommendation": "Tool Recommendation",
        "tool": "Tool Recommendation",
        "recommendation": "Tool Recommendation",
        "general comment": "General Comment",
        "comment": "General Comment",
        "general": "General Comment",
        # legacy
        "addition": "Feature Request",
        "compliment": "General Comment",
    }
    return category_map.get(category, "General Comment")


def get_feedback_payload():
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        return payload.get("source"), payload.get("category"), payload.get("message")
    return request.form.get("source"), request.form.get("category"), request.form.get("message")


def get_tool_video(tool_name):
    for extension in ALLOWED_VIDEO_EXT:
        candidate = VIDEO_DIR / f"{tool_name}_example{extension}"
        if candidate.exists():
            return {
                "url": url_for("static", filename=f"videos/{candidate.name}"),
                "filename": candidate.name,
                "mime_type": VIDEO_MIME_TYPES[extension],
            }
    return {
        "url": None,
        "filename": None,
        "mime_type": None,
    }


def list_job_artifacts():
    jobs = []
    for child in sorted(JOBS_DIR.iterdir(), key=lambda path: path.stat().st_mtime, reverse=True):
        if not child.is_dir():
            continue
        files = [item.name for item in child.iterdir() if item.is_file()]
        jobs.append(
            {
                "id": child.name,
                "created": datetime.fromtimestamp(child.stat().st_mtime, tz=timezone.utc).isoformat(),
                "files": files,
                "file_count": len(files),
            }
        )
    return jobs


def cleanup_old_jobs():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=JOB_RETENTION_HOURS)
    for child in JOBS_DIR.iterdir():
        if not child.is_dir():
            continue
        modified = datetime.fromtimestamp(child.stat().st_mtime, tz=timezone.utc)
        if modified < cutoff:
            shutil.rmtree(child, ignore_errors=True)


def safe_job_dir(job_id):
    job_id = str(job_id or "").strip()
    if not job_id or "/" in job_id or "\\" in job_id or job_id in {".", ".."}:
        return None
    target = (JOBS_DIR / job_id).resolve()
    try:
        if target.parent != JOBS_DIR.resolve():
            return None
    except FileNotFoundError:
        return None
    return target


def safe_file_in_dir(parent_dir, filename):
    filename = str(filename or "").strip()
    if (
        not filename
        or "/" in filename
        or "\\" in filename
        or filename in {".", ".."}
        or Path(filename).name != filename
    ):
        return None

    parent_dir = parent_dir.resolve()
    target = (parent_dir / filename).resolve()
    if target.parent != parent_dir:
        return None
    return target


def safe_project_file(relative_path):
    relative_path = Path(str(relative_path or ""))
    if relative_path.is_absolute() or any(part in {"", ".", ".."} for part in relative_path.parts):
        return None

    base_dir = BASE_DIR.resolve()
    target = (base_dir / relative_path).resolve()
    try:
        target.relative_to(base_dir)
    except ValueError:
        return None
    return target


def delete_job(job_id):
    target = safe_job_dir(job_id)
    if target is not None and target.exists() and target.is_dir():
        shutil.rmtree(target, ignore_errors=True)
        return True
    return False


def purge_all_jobs():
    deleted = 0
    for child in JOBS_DIR.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
            deleted += 1
    return deleted


def make_job_dir():
    cleanup_old_jobs()
    job_id = uuid.uuid4().hex
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=False)
    return job_id, job_dir


def remember_session_job(job_id):
    session_jobs = session.get("job_ids", [])
    if job_id not in session_jobs:
        session_jobs.append(job_id)
    session["job_ids"] = session_jobs
    session.modified = True


def cleanup_session_jobs():
    session_jobs = session.pop("job_ids", [])
    session.modified = True
    deleted = 0
    for job_id in session_jobs:
        if delete_job(job_id):
            deleted += 1
    return deleted


def save_upload(file_storage, target_dir, allowed_ext):
    if file_storage is None or not file_storage.filename:
        raise ValueError("Missing uploaded file.")
    original_name = Path(file_storage.filename).name
    extension = Path(original_name).suffix.lower()
    if extension not in allowed_ext:
        raise ValueError(f"Unsupported file type: {original_name}")
    safe_name = f"{uuid.uuid4().hex}{extension}"
    target_path = target_dir / safe_name
    file_storage.save(target_path)
    return target_path, original_name


def build_zip(zip_path, files):
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path, archive_name in files:
            archive.write(file_path, arcname=archive_name)
    return zip_path


@dataclass
class IfbGmpRunState:
    job_id: str
    job_dir: Path
    ifb_dir: Path
    gmp_dir: Path
    output_root: Path
    output_dir: Path
    max_workers: int
    status: str = "queued"
    progress: int = 0
    message: str = "Queued"
    error: str = ""
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )
    updated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )
    logs: list = field(default_factory=list)
    cancel_requested: bool = False
    future: Future | None = None


def ifb_gmp_default_workers():
    cpu_count = max(1, os.cpu_count() or 1)
    return max(1, min(6, cpu_count))


def ifb_gmp_int(value, default, minimum, maximum):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def touch_ifb_gmp_run(run, **updates):
    with IFB_GMP_RUNS_LOCK:
        for key, value in updates.items():
            setattr(run, key, value)
        run.updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def log_ifb_gmp_run(run, message):
    with IFB_GMP_RUNS_LOCK:
        run.logs.append(
            f"{datetime.now(timezone.utc).replace(microsecond=0).isoformat()} {message}"
        )
        run.logs = run.logs[-80:]
        run.updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_upload_relative_path(filename):
    raw = str(filename or "").replace("\\", "/").strip("/")
    if not raw:
        raise ValueError("Uploaded file is missing a filename.")
    relative = PurePosixPath(raw)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ValueError(f"Unsafe upload path: {filename}")
    return Path(*relative.parts)


def assert_path_inside(parent, target):
    parent_resolved = parent.resolve()
    target_resolved = target.resolve()
    try:
        target_resolved.relative_to(parent_resolved)
    except ValueError as exc:
        raise ValueError("Upload path escapes the run folder.") from exc


def pdf_count(folder):
    return sum(
        1
        for path in folder.rglob("*")
        if path.is_file() and path.suffix.lower() == ".pdf"
    )


def validate_pdf_folder(folder, label):
    if pdf_count(folder) <= 0:
        raise ValueError(f"{label} must contain at least one PDF.")


def extract_ifb_gmp_zip(upload, destination, label):
    filename = str(upload.filename or "")
    if Path(filename).suffix.lower() != ".zip":
        raise ValueError(f"{label} upload must be a .zip file.")

    destination.mkdir(parents=True, exist_ok=True)
    try:
        archive = zipfile.ZipFile(upload.stream)
    except zipfile.BadZipFile as exc:
        raise ValueError(f"{label} zip could not be opened.") from exc

    with archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            relative_path = safe_upload_relative_path(member.filename)
            target = destination / relative_path
            assert_path_inside(destination, target)
            if target.suffix.lower() != ".pdf":
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, target.open("wb") as handle:
                shutil.copyfileobj(source, handle)


def save_ifb_gmp_pdf_uploads(files, destination):
    destination.mkdir(parents=True, exist_ok=True)
    for upload in files:
        if not upload or not upload.filename:
            continue
        relative_path = safe_upload_relative_path(upload.filename)
        if relative_path.suffix.lower() != ".pdf":
            continue
        target = destination / relative_path
        assert_path_inside(destination, target)
        target.parent.mkdir(parents=True, exist_ok=True)
        upload.save(target)


def prepare_ifb_gmp_side_uploads(files, destination, label):
    uploads = [upload for upload in files if upload and upload.filename]
    if not uploads:
        raise ValueError(f"Upload at least one Revision {label} ZIP or PDF.")

    zip_uploads = []
    pdf_uploads = []
    unsupported = []
    for upload in uploads:
        suffix = Path(str(upload.filename)).suffix.lower()
        if suffix == ".zip":
            zip_uploads.append(upload)
        elif suffix == ".pdf":
            pdf_uploads.append(upload)
        else:
            unsupported.append(Path(str(upload.filename)).name)

    if unsupported:
        raise ValueError(
            f"Revision {label} upload contains unsupported files: {', '.join(unsupported)}. "
            "Use ZIP files or PDFs only."
        )

    destination.mkdir(parents=True, exist_ok=True)
    for upload in zip_uploads:
        extract_ifb_gmp_zip(upload, destination, f"Revision {label}")
    if pdf_uploads:
        save_ifb_gmp_pdf_uploads(pdf_uploads, destination)
    validate_pdf_folder(destination, label)


def prepare_ifb_gmp_inputs(run):
    rev_a_uploads = request.files.getlist("rev_a_uploads") + request.files.getlist("rev_a_folder_uploads")
    rev_b_uploads = request.files.getlist("rev_b_uploads") + request.files.getlist("rev_b_folder_uploads")
    prepare_ifb_gmp_side_uploads(rev_a_uploads, run.ifb_dir, "A")
    prepare_ifb_gmp_side_uploads(rev_b_uploads, run.gmp_dir, "B")


def prune_ifb_gmp_public_output(output_dir):
    if not output_dir.exists():
        return
    for child in output_dir.iterdir():
        if child.name in IFB_GMP_PUBLIC_ARTIFACT_NAMES:
            continue
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)


def ifb_gmp_downloads(run):
    if run.status != "complete":
        return []
    downloads = []
    for artifact_name, label in IFB_GMP_PUBLIC_ARTIFACTS:
        path = run.output_dir / artifact_name
        if path.is_file():
            downloads.append(
                {
                    "name": artifact_name,
                    "label": label,
                    "size": path.stat().st_size,
                    "url": url_for(
                        "ifb_gmp_download",
                        job_id=run.job_id,
                        artifact_name=artifact_name,
                    ),
                }
            )
    return downloads


def ifb_gmp_run_payload(run):
    return {
        "job_id": run.job_id,
        "status": run.status,
        "progress": run.progress,
        "message": run.message,
        "error": run.error,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "logs": run.logs[-12:],
        "downloads": ifb_gmp_downloads(run),
    }


def run_ifb_gmp_background(job_id):
    with IFB_GMP_RUNS_LOCK:
        run = IFB_GMP_RUNS[job_id]

    if run_ifb_gmp_compare_job is None:
        touch_ifb_gmp_run(
            run,
            status="failed",
            error=f"IFB/GMP compare engine could not be imported: {IFB_GMP_IMPORT_ERROR}",
            message="Failed",
        )
        return

    touch_ifb_gmp_run(run, status="running", progress=0, message="Starting compare job.")
    request_payload = {
        "mode": "paired",
        "run_id": "public",
        "output_root": str(run.output_root),
        "performance": {"max_workers": run.max_workers},
        "inputs": {
            "ifb_folder": str(run.ifb_dir),
            "gmp_folder": str(run.gmp_dir),
        },
    }

    def progress(**payload):
        percent = payload.get("overall_percent", payload.get("percent", run.progress))
        message = str(payload.get("message") or payload.get("stage") or run.message)
        touch_ifb_gmp_run(
            run,
            progress=ifb_gmp_int(percent, run.progress, 0, 100),
            message=message,
        )

    def log(message):
        log_ifb_gmp_run(run, str(message))

    def cancel_requested():
        return run.cancel_requested

    try:
        result = run_ifb_gmp_compare_job(
            request_payload,
            progress_callback=progress,
            log_callback=log,
            cancel_requested=cancel_requested,
        )
        run.output_dir = Path(result["output_dir"]).resolve()
        prune_ifb_gmp_public_output(run.output_dir)
        missing = [
            name
            for name in IFB_GMP_PUBLIC_ARTIFACT_NAMES
            if not (run.output_dir / name).is_file()
        ]
        if missing:
            raise RuntimeError(
                "Compare completed without expected output: "
                + ", ".join(sorted(missing))
            )
        extra = sorted(
            path.name
            for path in run.output_dir.iterdir()
            if path.name not in IFB_GMP_PUBLIC_ARTIFACT_NAMES
        )
        if extra:
            raise RuntimeError(
                "Public output folder contains unexpected files: " + ", ".join(extra)
            )
        touch_ifb_gmp_run(run, status="complete", progress=100, message="Complete")
    except Exception as exc:
        status = "cancelled" if run.cancel_requested else "failed"
        touch_ifb_gmp_run(
            run,
            status=status,
            message="Cancelled" if status == "cancelled" else "Failed",
            error=str(exc),
        )
        log_ifb_gmp_run(run, f"{type(exc).__name__}: {exc}")


def tool_href(slug, metadata):
    if metadata.get("endpoint"):
        return url_for(metadata["endpoint"])
    return url_for("tool_downloads", tool_slug=metadata["url_slug"])


def build_tool_catalog():
    tools = []
    for slug, metadata in TOOL_REGISTRY.items():
        tool = dict(metadata)
        tool["slug"] = slug
        tool["href"] = tool_href(slug, metadata)
        tool["short_summary"] = metadata.get("short_summary", metadata["description"])
        tool["detailed_explanation"] = metadata.get(
            "detailed_explanation",
            f"{metadata['description']} {metadata.get('portal_use', '')}".strip(),
        )
        tool["what_how"] = metadata.get("what_how", tool["short_summary"])
        tool["behind_scenes"] = metadata.get("behind_scenes", tool["detailed_explanation"])
        video_keys = [slot["key"] for slot in metadata["video_slots"]]
        videos = [get_tool_video(video_key) for video_key in video_keys]
        uploaded_videos = [video for video in videos if video["url"]]
        tool["video"] = uploaded_videos[0] if uploaded_videos else (
            videos[0] if videos else {"url": None, "filename": None, "mime_type": None}
        )
        tool["video_count"] = len(uploaded_videos)
        tool["video_total"] = len(videos)
        if metadata.get("status_label"):
            tool["video_status_label"] = metadata["status_label"]
        elif tool["video_total"] == 1:
            tool["video_status_label"] = "Example video ready" if tool["video_count"] else "No example video"
        else:
            tool["video_status_label"] = f"{tool['video_count']}/{tool['video_total']} examples"
        tools.append(tool)
    return tools


def build_tool_navigation():
    return [
        {
            "slug": slug,
            "label": metadata.get("nav_label", metadata["title"]),
            "href": tool_href(slug, metadata),
            "endpoint": metadata.get("endpoint", "tool_downloads"),
            "url_slug": metadata.get("url_slug"),
        }
        for slug, metadata in TOOL_REGISTRY.items()
    ]


@app.context_processor
def inject_portal_metadata():
    return {
        "feedback_source_labels": FEEDBACK_SOURCE_LABELS,
        "portal_tools": build_tool_navigation(),
        "csrf_token": csrf_token,
        "require_login": REQUIRE_LOGIN,
        "enable_admin": ENABLE_ADMIN,
        "demo_mode": DEMO_MODE,
    }


def get_download_tool(tool_slug):
    registry_slug = DOWNLOAD_TOOLS_BY_URL_SLUG.get(tool_slug)
    if registry_slug is None:
        return None, None
    return registry_slug, TOOL_REGISTRY[registry_slug]


def build_download_items(tool_slug, tool):
    downloads = []
    for metadata in tool["downloads"]:
        file_path = safe_project_file(metadata["filename"])
        file_exists = file_path is not None and file_path.is_file()
        downloads.append(
            {
                **metadata,
                "display_filename": Path(metadata["filename"]).name,
                "exists": file_exists,
                "download_url": url_for(
                    "download_tool_file",
                    tool_slug=tool_slug,
                    download_slug=metadata["slug"],
                ),
                "size_kb": round(file_path.stat().st_size / 1024, 1)
                if file_exists
                else None,
            }
        )
    return downloads


def build_download_examples(tool):
    examples = []
    for slot in tool["video_slots"]:
        video_key = slot["key"]
        examples.append(
            {
                "key": video_key,
                "title": slot["title"],
                "video": get_tool_video(video_key),
                "placeholder": VIDEO_SLOT_DETAILS[video_key]["placeholder"],
                "placeholder_aspect": VIDEO_SLOT_DETAILS[video_key]["placeholder_aspect"],
                "placeholder_surface": VIDEO_SLOT_DETAILS[video_key]["placeholder_surface"],
                "placeholder_padding": VIDEO_SLOT_DETAILS[video_key]["placeholder_padding"],
            }
        )
    return examples


@app.get("/")
def index():
    return render_template("index.html", tools=build_tool_catalog())


@app.get("/about")
def about():
    return render_template("about.html")


@app.get("/tools/scanner")
def tool_scanner():
    return render_template("tool_scanner.html")


@app.get("/tools/comparator")
def tool_comparator():
    return render_template("tool_comparator.html")


@app.get("/tools/ifb-gmp-compare")
def tool_ifb_gmp_compare():
    return redirect(url_for("tool_pdf_revision_compare"))


@app.get("/tools/pdf-revision-comparison")
def tool_pdf_revision_compare():
    return render_template(
        "tool_ifb_gmp_compare.html",
        default_max_workers=ifb_gmp_default_workers(),
        max_worker_limit=8,
        import_error=str(IFB_GMP_IMPORT_ERROR) if IFB_GMP_IMPORT_ERROR else "",
    )


@app.get("/tools/<tool_slug>")
def tool_downloads(tool_slug):
    registry_slug, tool = get_download_tool(tool_slug)
    if tool is None:
        abort(404)
    return render_template(
        "tool_downloads.html",
        page_feedback_source=registry_slug,
        tool={
            **tool,
            "slug": registry_slug,
        },
        downloads=build_download_items(tool_slug, tool),
    )


@app.get("/tools/<tool_slug>/download/<download_slug>")
def download_tool_file(tool_slug, download_slug):
    _registry_slug, tool = get_download_tool(tool_slug)
    if tool is None:
        abort(404)
    download = next(
        (item for item in tool["downloads"] if item["slug"] == download_slug),
        None,
    )
    if download is None:
        abort(404)
    file_path = safe_project_file(download["filename"])
    if file_path is None or not file_path.is_file():
        abort(404)
    return send_file(
        file_path,
        as_attachment=True,
        download_name=download.get("download_name", download["filename"]),
    )


@app.post("/feedback")
def feedback():
    source, category, message = get_feedback_payload()
    source = normalize_feedback_source(source)
    category = normalize_feedback_category(category)
    message = str(message or "").strip()
    if not message:
        return {"error": "Feedback message cannot be empty."}, 400

    submitter_email = str(request.form.get("submitter_email") or "").strip()
    if submitter_email:
        message += f"\n\n[Submitted by: {submitter_email}]"

    uploaded_files = request.files.getlist("attachments")
    if uploaded_files:
        FEEDBACK_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        saved = []
        for f in uploaded_files[:8]:
            if not f or not f.filename:
                continue
            safe_name = secure_filename(f.filename)
            if not safe_name:
                continue
            entry_id = str(uuid.uuid4())[:8]
            dest = FEEDBACK_UPLOAD_DIR / f"{entry_id}_{safe_name}"
            f.save(dest)
            saved.append(safe_name)
        if saved:
            message += f"\n\n[Attachments: {', '.join(saved)}]"

    add_feedback_entry(source, category, message)
    return {"success": True}


@app.route("/login", methods=["GET", "POST"])
def team_login():
    if not REQUIRE_LOGIN:
        return private_access_redirect()
    if current_user.is_authenticated:
        return private_access_redirect()

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username == TEAM_USERNAME and check_password_hash(TEAM_HASH, password):
            login_user(PortalUser("team"))
            return private_access_redirect()
        error = "Invalid name or password."
    return render_template(
        "login.html",
        error=error,
        form_endpoint="team_login",
        login_title="Team login",
        login_description="Private access for Stellar Process tool users.",
        button_label="Enter portal",
        show_admin_access=True,
    )


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if not ENABLE_ADMIN:
        abort(404)
    if current_user.is_authenticated and current_user.is_admin:
        return admin_access_redirect()

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username == ADMIN_USERNAME and check_password_hash(ADMIN_HASH, password):
            login_user(PortalUser("admin"))
            return admin_access_redirect()
        error = "Invalid username or password."
    return render_template(
        "login.html",
        error=error,
        form_endpoint="admin_login",
        login_title="Admin login",
        login_description="Admin access for feedback and temporary job cleanup.",
        button_label="Enter admin",
        show_team_access=True,
    )


@app.get("/admin")
@admin_required
def admin_dashboard():
    filter_source = request.args.get("filter", "all")
    feedback_entries = sorted(
        load_feedback(),
        key=lambda entry: entry.get("timestamp", ""),
        reverse=True,
    )
    if filter_source in FEEDBACK_SOURCE_LABELS:
        feedback_entries = [e for e in feedback_entries if e["source"] == filter_source]
    return render_template(
        "admin.html",
        feedback_source_labels=FEEDBACK_SOURCE_LABELS,
        feedback=feedback_entries,
        jobs=list_job_artifacts(),
        current_filter=filter_source,
    )


@app.post("/admin/feedback/delete/<feedback_id>")
@admin_required
def admin_delete_feedback(feedback_id):
    entries = load_feedback()
    remaining = [entry for entry in entries if entry["id"] != feedback_id]
    save_feedback(remaining)
    flash("Feedback entry removed.", "success")
    return redirect(url_for("admin_dashboard"))


@app.post("/admin/jobs/delete/<job_id>")
@admin_required
def admin_delete_job(job_id):
    if delete_job(job_id):
        flash("Job directory deleted.", "success")
    else:
        flash("Job not found.", "error")
    return redirect(url_for("admin_dashboard"))


@app.post("/admin/jobs/cleanup")
@admin_required
def admin_cleanup_jobs():
    cleanup_old_jobs()
    flash("Expired jobs cleaned up.", "success")
    return redirect(url_for("admin_dashboard"))


@app.post("/admin/jobs/purge")
@admin_required
def admin_purge_jobs():
    deleted = purge_all_jobs()
    flash(f"Deleted {deleted} job folder(s).", "success")
    return redirect(url_for("admin_dashboard"))


@app.post("/session/jobs/cleanup")
@login_required
def session_jobs_cleanup():
    deleted = cleanup_session_jobs()
    return {"success": True, "deleted": deleted}


@app.post("/ifb-gmp/start")
def ifb_gmp_start():
    if DEMO_MODE:
        return demo_processing_response("PDF Revision Compare", json_response=True)
    job_dir = None
    try:
        job_id, job_dir = make_job_dir()
        max_workers = ifb_gmp_int(
            request.form.get("max_workers"),
            ifb_gmp_default_workers(),
            1,
            8,
        )
        run = IfbGmpRunState(
            job_id=job_id,
            job_dir=job_dir,
            ifb_dir=job_dir / "ifb",
            gmp_dir=job_dir / "gmp",
            output_root=job_dir / "ifb_gmp_output",
            output_dir=job_dir / "ifb_gmp_output" / "public",
            max_workers=max_workers,
        )
        prepare_ifb_gmp_inputs(run)
        remember_session_job(job_id)
        with IFB_GMP_RUNS_LOCK:
            IFB_GMP_RUNS[job_id] = run
            run.future = IFB_GMP_EXECUTOR.submit(run_ifb_gmp_background, job_id)
        return jsonify(ifb_gmp_run_payload(run)), 202
    except ValueError as exc:
        if job_dir is not None:
            shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify({"error": str(exc)}), 400
    except Exception:
        if job_dir is not None:
            shutil.rmtree(job_dir, ignore_errors=True)
        app.logger.exception("IFB/GMP compare job could not be started.")
        return jsonify({"error": "IFB/GMP compare job could not be started."}), 500


@app.get("/ifb-gmp/status/<job_id>")
def ifb_gmp_status(job_id):
    with IFB_GMP_RUNS_LOCK:
        run = IFB_GMP_RUNS.get(job_id)
        if run is None:
            abort(404)
        payload = ifb_gmp_run_payload(run)
    return jsonify(payload)


@app.post("/ifb-gmp/cancel/<job_id>")
def ifb_gmp_cancel(job_id):
    with IFB_GMP_RUNS_LOCK:
        run = IFB_GMP_RUNS.get(job_id)
        if run is None:
            abort(404)
        if run.status not in {"complete", "failed", "cancelled"}:
            run.cancel_requested = True
            run.message = "Cancelling"
            run.updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            if run.future is not None and run.future.cancel():
                run.status = "cancelled"
                run.progress = 0
                run.message = "Cancelled"
        payload = ifb_gmp_run_payload(run)
    return jsonify(payload)


@app.get("/ifb-gmp/download/<job_id>/<artifact_name>")
def ifb_gmp_download(job_id, artifact_name):
    if artifact_name not in IFB_GMP_PUBLIC_ARTIFACT_NAMES:
        abort(404)

    with IFB_GMP_RUNS_LOCK:
        run = IFB_GMP_RUNS.get(job_id)
        if run is None or run.status != "complete":
            abort(404)
        output_dir = run.output_dir.resolve()

    file_path = (output_dir / artifact_name).resolve()
    try:
        file_path.relative_to(output_dir)
    except ValueError:
        abort(404)
    if not file_path.is_file():
        abort(404)
    return send_file(file_path, as_attachment=True, download_name=artifact_name)


@app.get("/admin/logout")
@app.post("/admin/logout")
@admin_required
def admin_logout():
    cleanup_session_jobs()
    logout_user()
    return redirect(url_for("admin_login"))


@app.get("/logout")
@app.post("/logout")
@login_required
def logout():
    cleanup_session_jobs()
    logout_user()
    return redirect(url_for("team_login"))


@app.post("/scan")
def scan():
    if DEMO_MODE:
        return demo_processing_response("Scanner")
    try:
        job_id, job_dir = make_job_dir()
        remember_session_job(job_id)
        pdf_path, original_name = save_upload(request.files.get("pdf"), job_dir, {".pdf"})
        summary = run_scan(
            str(pdf_path),
            mode="all",
            ocr_mode="auto",
            ocr_dpi=300,
            debug_text=False,
        )
        unique_tags = summary.get("unique_tags", summary.get("unique_full_tags", 0))
        unique_sheets = summary.get("unique_sheets", summary.get("pages", 0))
        download_name = f"{Path(original_name).stem}_scan_report.xlsx"
        return render_template(
            "result.html",
            title="Scanner Complete",
            summary_lines=[
                f"Total tags found: {summary['total_tags']}",
                f"Unique tags: {unique_tags}",
                f"Sheets scanned: {unique_sheets}",
            ],
            back_tool_label="Scanner",
            back_tool_url=url_for("tool_scanner"),
            downloads=[
                {
                    "label": "Download Excel Report",
                    "href": url_for(
                        "download",
                        job_id=job_id,
                        filename=Path(summary["output_excel"]).name,
                        download_name=download_name,
                    ),
                }
            ],
        )
    except ValueError as exc:
        return render_template(
            "result.html",
            title="Scanner Failed",
            error=str(exc),
            back_tool_label="Scanner",
            back_tool_url=url_for("tool_scanner"),
        ), 400
    except Exception:
        app.logger.exception("Scanner failed while processing an uploaded PDF.")
        return render_template(
            "result.html",
            title="Scanner Failed",
            error="Scanner failed while processing the PDF. Please verify the file and try again.",
            back_tool_label="Scanner",
            back_tool_url=url_for("tool_scanner"),
        ), 400


@app.post("/compare")
def compare():
    if DEMO_MODE:
        return demo_processing_response("Comparator")
    try:
        job_id, job_dir = make_job_dir()
        remember_session_job(job_id)
        pdf_path, pdf_name = save_upload(request.files.get("pdf"), job_dir, {".pdf"})
        xlsx_path, xlsx_name = save_upload(request.files.get("xlsx"), job_dir, {".xlsx"})
        summary = run_comparison(
            str(pdf_path),
            str(xlsx_path),
            mode="all",
            annotate_pdf=True,
            annotate_unmatched_pdf=True,
            annotation_confidence="medium",
            ocr_mode="auto",
            ocr_dpi=300,
            debug_text=False,
        )
        output_excel = summary.get("output_excel") or summary.get("output_xlsx")
        output_pdf = summary.get("output_pdf")
        if not output_excel:
            raise ValueError("Comparison did not produce the annotated Excel output.")

        downloads = [
            {
                "label": "Download Annotated Excel",
                "href": url_for(
                    "download",
                    job_id=job_id,
                    filename=Path(output_excel).name,
                    download_name=f"{Path(xlsx_name).stem}_annotated.xlsx",
                ),
            }
        ]
        if output_pdf:
            downloads.append(
                {
                    "label": "Download Annotated PDF",
                    "href": url_for(
                        "download",
                        job_id=job_id,
                        filename=Path(output_pdf).name,
                        download_name=f"{Path(pdf_name).stem}_annotated.pdf",
                    ),
                }
            )

        return render_template(
            "result.html",
            title="Comparison Complete",
            summary_lines=[
                f"PDF pages mapped: {summary['pages_mapped']}",
                f"Excel rows matched: {summary.get('matched', 0)}",
                f"High confidence matches: {summary.get('matched_high', 0)}",
                f"Rows not found in PDF: {summary.get('not_found', 0)}",
                f"Rows without mapped PDF page: {summary.get('no_page', 0)}",
            ],
            back_tool_label="Comparator",
            back_tool_url=url_for("tool_comparator"),
            downloads=downloads,
        )
    except ValueError as exc:
        return render_template(
            "result.html",
            title="Comparison Failed",
            error=str(exc),
            back_tool_label="Comparator",
            back_tool_url=url_for("tool_comparator"),
        ), 400
    except Exception:
        app.logger.exception("Comparison failed while processing uploaded files.")
        return render_template(
            "result.html",
            title="Comparison Failed",
            error="Comparison failed while processing the uploaded files. Please verify the files and try again.",
            back_tool_label="Comparator",
            back_tool_url=url_for("tool_comparator"),
        ), 400


@app.get("/download/<job_id>/<filename>")
def download(job_id, filename):
    job_dir = safe_job_dir(job_id)
    if job_dir is None:
        abort(404)
    file_path = safe_file_in_dir(job_dir, filename)
    if file_path is None:
        abort(404)
    if not file_path.is_file():
        abort(404)

    requested_name = request.args.get("download_name") or filename
    return send_file(file_path, as_attachment=True, download_name=requested_name)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
