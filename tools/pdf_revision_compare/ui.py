from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from PySide6.QtCore import QMimeData, QThread, QTimer, Qt, QUrl, Signal
from PySide6.QtGui import QDragEnterEvent, QDropEvent, QDesktopServices, QPalette
from PySide6.QtWidgets import (
    QApplication,
    QButtonGroup,
    QFileDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSizePolicy,
    QSpacerItem,
    QStackedWidget,
    QStyle,
    QToolButton,
    QVBoxLayout,
    QWidget,
)


def _safe_resolve(path: str | os.PathLike[str]) -> Path:
    candidate = Path(path).expanduser()
    try:
        return candidate.resolve()
    except Exception:
        return candidate.absolute()


def _display_path(path: str | None, limit: int = 72) -> str:
    if not path:
        return "Not selected"
    text = str(path)
    if len(text) <= limit:
        return text
    head = max(18, (limit - 5) // 2)
    tail = max(18, limit - head - 3)
    return f"{text[:head]}...{text[-tail:]}"


def _count_pdfs(folder: str | None) -> int:
    if not folder:
        return 0
    root = Path(folder)
    if not root.exists() or not root.is_dir():
        return 0
    return sum(1 for entry in root.rglob("*") if entry.is_file() and entry.suffix.lower() == ".pdf")


def _folder_from_drop_path(path: str) -> str | None:
    candidate = _safe_resolve(path)
    if candidate.is_dir():
        return str(candidate)
    if candidate.is_file():
        return str(candidate.parent)
    return None


def _paths_from_mime(mime: QMimeData) -> list[str]:
    folders: list[str] = []
    for url in mime.urls():
        if not url.isLocalFile():
            continue
        folder = _folder_from_drop_path(url.toLocalFile())
        if folder and folder not in folders:
            folders.append(folder)
    return folders


def _timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _default_output_root(*paths: str | None) -> str:
    folders = [Path(p).resolve() for p in paths if p]
    if folders:
        try:
            base = Path(os.path.commonpath([str(folder) for folder in folders]))
        except Exception:
            base = folders[0].parent
    else:
        base = Path.cwd()
    return str((base / "IFB_GMP_Compare_Output").resolve())


def _open_in_file_manager(path: str | None) -> None:
    if not path:
        return
    target = Path(path)
    if target.is_file():
        target = target.parent
    if target.exists():
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))


def _extract_text(result: Any, keys: Iterable[str]) -> str | None:
    if isinstance(result, dict):
        for key in keys:
            value = result.get(key)
            if value:
                return str(value)
    for key in keys:
        value = getattr(result, key, None)
        if value:
            return str(value)
    return None


def _extract_output_dir(result: Any) -> str | None:
    return _extract_text(result, ("output_dir", "output_folder", "output_path", "package_dir", "run_dir"))


def _extract_report_path(result: Any) -> str | None:
    return _extract_text(result, ("report_path", "excel_path", "manifest_path"))


def _format_status_label(mode: str, mixed: str | None, before: str | None, after: str | None) -> str:
    if mode == "mixed":
        return "Mixed folder" if mixed else "Waiting for folder"
    if before and after:
        return "IFB / GMP ready"
    if before or after:
        return "One folder selected"
    return "Waiting for IFB and GMP folders"


def _format_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0 or math.isinf(seconds) or math.isnan(seconds):
        return "--:--"
    total_seconds = int(round(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


@dataclass
class _SelectionSnapshot:
    mode: str
    mixed_folder: str | None
    before_folder: str | None
    after_folder: str | None
    output_root: str


class FolderDropCard(QFrame):
    folderChanged = Signal(str)
    browseRequested = Signal()
    clearRequested = Signal()

    def __init__(self, title: str, description: str, accent: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._accent = accent
        self._path: str | None = None
        self._pdf_count = 0
        self.setAcceptDrops(True)
        self.setObjectName("folderCard")

        self._title = QLabel(title)
        self._title.setObjectName("cardTitle")
        self._description = QLabel(description)
        self._description.setWordWrap(True)
        self._description.setObjectName("cardDescription")

        self._path_label = QLabel("Drop a folder here")
        self._path_label.setWordWrap(True)
        self._path_label.setObjectName("cardPath")

        self._count_label = QLabel("0 PDFs")
        self._count_label.setObjectName("cardCount")

        self._browse_button = QPushButton("Browse")
        self._browse_button.clicked.connect(self.browseRequested.emit)
        self._clear_button = QToolButton()
        self._clear_button.setText("Clear")
        self._clear_button.clicked.connect(self.clearRequested.emit)

        buttons = QHBoxLayout()
        buttons.setContentsMargins(0, 0, 0, 0)
        buttons.setSpacing(8)
        buttons.addWidget(self._browse_button)
        buttons.addWidget(self._clear_button)
        buttons.addStretch(1)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)
        layout.addWidget(self._title)
        layout.addWidget(self._description)
        layout.addSpacing(2)
        layout.addWidget(self._path_label)
        layout.addWidget(self._count_label)
        layout.addStretch(1)
        layout.addLayout(buttons)

        self._apply_visual_state(False)

    def set_folder(self, path: str | None) -> None:
        self._path = path
        self._pdf_count = _count_pdfs(path)
        self._path_label.setText(_display_path(path))
        self._path_label.setToolTip(path or "")
        self._count_label.setText(f"{self._pdf_count} PDF{'s' if self._pdf_count != 1 else ''}")
        self._apply_visual_state(bool(path))

    def clear(self) -> None:
        self.set_folder(None)

    def folder(self) -> str | None:
        return self._path

    def pdf_count(self) -> int:
        return self._pdf_count

    def set_interactive(self, enabled: bool) -> None:
        self._browse_button.setEnabled(enabled)
        self._clear_button.setEnabled(enabled)
        self.setAcceptDrops(enabled)

    def _apply_visual_state(self, has_folder: bool) -> None:
        self.setProperty("selected", has_folder)
        self.style().unpolish(self)
        self.style().polish(self)
        self.update()

    def dragEnterEvent(self, event: QDragEnterEvent) -> None:
        if _paths_from_mime(event.mimeData()):
            event.acceptProposedAction()

    def dropEvent(self, event: QDropEvent) -> None:
        folders = _paths_from_mime(event.mimeData())
        if folders:
            self.folderChanged.emit(folders[0])
            event.acceptProposedAction()


class StatusCard(QFrame):
    def __init__(self, title: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("statusCard")
        self._title = QLabel(title)
        self._title.setObjectName("statusTitle")
        self._value = QLabel("Not selected")
        self._value.setWordWrap(True)
        self._value.setObjectName("statusValue")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 16)
        layout.setSpacing(6)
        layout.addWidget(self._title)
        layout.addWidget(self._value)
        layout.addStretch(1)

    def set_value(self, text: str) -> None:
        self._value.setText(text)

    def set_title(self, text: str) -> None:
        self._title.setText(text)

    def text(self) -> str:
        return self._value.text()


class CompareWorker(QThread):
    progress = Signal(object)
    log = Signal(str)
    completed = Signal(object)
    failed = Signal(str)
    cancelled = Signal(object)

    def __init__(self, request: dict[str, Any], parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._request = request
        self._cancel_requested = False

    def request_cancel(self) -> None:
        self._cancel_requested = True

    def is_cancel_requested(self) -> bool:
        return self._cancel_requested

    def _progress_callback(self, *args: Any, **kwargs: Any) -> None:
        payload: dict[str, Any] = dict(kwargs)
        percent: int | None = None
        message = ""
        if kwargs:
            if "percent" in kwargs:
                percent = kwargs["percent"]
            elif "value" in kwargs:
                percent = kwargs["value"]
            message = str(kwargs.get("message") or kwargs.get("text") or "")
        if args:
            if len(args) == 1:
                first = args[0]
                if isinstance(first, (int, float)):
                    percent = int(first if first > 1 else first * 100)
                else:
                    message = str(first)
            else:
                first, second = args[0], args[1]
                if isinstance(first, (int, float)):
                    percent = int(first if first > 1 else first * 100)
                message = str(second)
        percent = 0 if percent is None else max(0, min(100, int(percent)))
        payload["percent"] = percent
        payload["message"] = message
        self.progress.emit(payload)

    def _log_callback(self, message: Any) -> None:
        self.log.emit(str(message))

    def run(self) -> None:  # pragma: no cover - driven through the GUI
        try:
            from universal_pdf_compare.core import JobCancelledError, run_compare_job
        except Exception as exc:  # pragma: no cover - handled in GUI
            self.failed.emit(
                "The compare core is not available yet. "
                f"Expected universal_pdf_compare.core.run_compare_job, but import failed: {exc}"
            )
            return

        try:
            result = run_compare_job(
                self._request,
                progress_callback=self._progress_callback,
                log_callback=self._log_callback,
                cancel_requested=self.is_cancel_requested,
            )
        except JobCancelledError as exc:  # pragma: no cover - handled in GUI
            payload = exc.result or {"message": str(exc)}
            if not isinstance(payload, dict):
                payload = {"message": str(exc)}
            payload.setdefault("message", str(exc))
            self.cancelled.emit(payload)
            return
        except Exception as exc:  # pragma: no cover - handled in GUI
            self.failed.emit(str(exc))
            return

        self.completed.emit(result)


class UniversalPdfCompareWindow(QMainWindow):
    def __init__(
        self,
        initial_mode: str = "paired",
        mixed_folder: str | None = None,
        before_folder: str | None = None,
        after_folder: str | None = None,
        output_root: str | None = None,
        max_workers: int | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("IFB / GMP PDF Compare")
        self.setAcceptDrops(True)
        self.resize(1360, 920)

        self._mode = "mixed" if initial_mode == "mixed" else "paired"
        self._mixed_folder = mixed_folder
        self._before_folder = before_folder
        self._after_folder = after_folder
        self._output_root = output_root or _default_output_root(mixed_folder, before_folder, after_folder)
        self._output_root_user_selected = output_root is not None
        self._max_workers = max_workers
        self._active_output_dir: str | None = None
        self._worker: CompareWorker | None = None
        self._close_after_worker = False
        self._job_state = "idle"
        self._job_started_at: float | None = None
        self._last_activity_at: float | None = None
        self._last_progress_percent = 0
        self._last_progress_payload: dict[str, Any] = {}
        self._health_timer = QTimer(self)
        self._health_timer.setInterval(1000)
        self._health_timer.timeout.connect(self._on_health_timer)

        self._build_ui()
        self._apply_initial_state()
        self._update_run_state()

    def _build_ui(self) -> None:
        central = QWidget(self)
        central.setObjectName("root")
        self.setCentralWidget(central)

        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(24, 24, 24, 24)
        main_layout.setSpacing(18)

        hero = QFrame()
        hero.setObjectName("heroCard")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(24, 24, 24, 24)
        hero_layout.setSpacing(8)
        self._title_label = QLabel("IFB / GMP PDF Compare")
        self._title_label.setObjectName("heroTitle")
        self._subtitle_label = QLabel(
            "Select the IFB and GMP drawing folders, then build a sheet-centered review package with green, yellow, and red highlighted PDFs."
        )
        self._subtitle_label.setWordWrap(True)
        self._subtitle_label.setObjectName("heroSubtitle")
        hero_layout.addWidget(self._title_label)
        hero_layout.addWidget(self._subtitle_label)
        main_layout.addWidget(hero)

        mode_row = QHBoxLayout()
        mode_row.setSpacing(10)

        self._mixed_mode_button = QPushButton("Advanced mixed folder")
        self._mixed_mode_button.setCheckable(True)
        self._paired_mode_button = QPushButton("IFB / GMP folders")
        self._paired_mode_button.setCheckable(True)
        self._mode_group = QButtonGroup(self)
        self._mode_group.addButton(self._mixed_mode_button)
        self._mode_group.addButton(self._paired_mode_button)
        self._mixed_mode_button.clicked.connect(lambda: self.set_mode("mixed"))
        self._paired_mode_button.clicked.connect(lambda: self.set_mode("paired"))

        self._mode_hint = QLabel("Drag IFB and GMP folders into the window, or browse for each folder.")
        self._mode_hint.setObjectName("modeHint")
        self._run_badge = QLabel("Idle")
        self._run_badge.setObjectName("runBadge")

        mode_row.addWidget(self._mixed_mode_button)
        mode_row.addWidget(self._paired_mode_button)
        mode_row.addStretch(1)
        mode_row.addWidget(self._run_badge)
        mode_row.addWidget(self._mode_hint)
        main_layout.addLayout(mode_row)
        self._mixed_mode_button.setVisible(False)

        self._state_grid = QGridLayout()
        self._state_grid.setHorizontalSpacing(12)
        self._state_grid.setVerticalSpacing(12)
        self._mode_card = StatusCard("Mode")
        self._inputs_card = StatusCard("Inputs")
        self._output_card = StatusCard("Output")
        self._status_card = StatusCard("Status")
        self._state_grid.addWidget(self._mode_card, 0, 0)
        self._state_grid.addWidget(self._inputs_card, 0, 1)
        self._state_grid.addWidget(self._output_card, 0, 2)
        self._state_grid.addWidget(self._status_card, 0, 3)
        main_layout.addLayout(self._state_grid)

        self._input_stack = QStackedWidget()
        self._mixed_card = FolderDropCard(
            "Mixed PDF folder",
            "Advanced fallback: drop a single folder that contains related before/after PDFs.",
            "#2c6bed",
        )
        self._before_card = FolderDropCard(
            "IFB folder",
            "Drop the folder containing the IFB or older drawing PDFs.",
            "#1d7a5f",
        )
        self._after_card = FolderDropCard(
            "GMP folder",
            "Drop the folder containing the GMP or newer drawing PDFs.",
            "#d97706",
        )

        mixed_page = QWidget()
        mixed_layout = QVBoxLayout(mixed_page)
        mixed_layout.setContentsMargins(0, 0, 0, 0)
        mixed_layout.addWidget(self._mixed_card)
        self._input_stack.addWidget(mixed_page)

        paired_page = QWidget()
        paired_layout = QGridLayout(paired_page)
        paired_layout.setContentsMargins(0, 0, 0, 0)
        paired_layout.setHorizontalSpacing(12)
        paired_layout.addWidget(self._before_card, 0, 0)

        swap_holder = QFrame()
        swap_layout = QVBoxLayout(swap_holder)
        swap_layout.setContentsMargins(0, 0, 0, 0)
        swap_layout.setSpacing(8)
        swap_layout.addItem(QSpacerItem(1, 1, QSizePolicy.Minimum, QSizePolicy.Expanding))
        self._swap_button = QToolButton()
        self._swap_button.setText("Swap")
        self._swap_button.clicked.connect(self._swap_before_after)
        self._swap_button.setToolTip("Swap the IFB and GMP folders.")
        swap_layout.addWidget(self._swap_button, alignment=Qt.AlignCenter)
        swap_layout.addItem(QSpacerItem(1, 1, QSizePolicy.Minimum, QSizePolicy.Expanding))
        paired_layout.addWidget(swap_holder, 0, 1)
        paired_layout.addWidget(self._after_card, 0, 2)
        self._input_stack.addWidget(paired_page)
        main_layout.addWidget(self._input_stack)

        action_row = QHBoxLayout()
        action_row.setSpacing(10)
        self._output_button = QPushButton("Choose Output...")
        self._output_button.clicked.connect(self._choose_output_root)
        self._run_button = QPushButton("Run Compare")
        self._run_button.setObjectName("primaryButton")
        self._run_button.clicked.connect(self._run_compare)
        self._stop_button = QPushButton("Stop")
        self._stop_button.clicked.connect(self._request_stop)
        self._stop_button.setEnabled(False)
        self._clear_button = QPushButton("Clear")
        self._clear_button.clicked.connect(self._clear_all)
        self._open_output_button = QPushButton("Open Output Folder")
        self._open_output_button.clicked.connect(self._open_output_folder)
        self._open_output_button.setEnabled(False)
        action_row.addWidget(self._output_button)
        action_row.addWidget(self._clear_button)
        action_row.addStretch(1)
        action_row.addWidget(self._open_output_button)
        action_row.addWidget(self._stop_button)
        action_row.addWidget(self._run_button)
        main_layout.addLayout(action_row)

        progress_row = QHBoxLayout()
        progress_row.setSpacing(12)
        self._progress_title = QLabel("Full program progress")
        self._progress_title.setObjectName("progressTitle")
        self._progress_bar = QProgressBar()
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(0)
        self._progress_bar.setFormat("%p%")
        self._progress_bar.setMinimumWidth(420)
        self._progress_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._progress_label = QLabel("Ready")
        self._progress_label.setObjectName("progressLabel")
        self._progress_label.setFixedWidth(560)
        self._progress_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self._progress_label.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        progress_row.addWidget(self._progress_title)
        progress_row.addWidget(self._progress_bar, 1)
        progress_row.addWidget(self._progress_label)
        main_layout.addLayout(progress_row)

        runtime_row = QHBoxLayout()
        runtime_row.setSpacing(12)
        self._runtime_label = QLabel("Elapsed 00:00 | ETA --:--")
        self._runtime_label.setObjectName("runtimeLabel")
        self._current_file_label = QLabel("Current file: None")
        self._current_file_label.setObjectName("currentFileLabel")
        self._current_file_label.setWordWrap(True)
        runtime_row.addWidget(self._runtime_label)
        runtime_row.addWidget(self._current_file_label, 1)
        main_layout.addLayout(runtime_row)

        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setPlaceholderText("Live run log appears here.")
        self._log.setObjectName("logPanel")
        self._log.setMinimumHeight(240)
        main_layout.addWidget(self._log, 1)

        footer = QLabel(
            "Review starts in report.xlsx; highlighted PDFs show where to look on changed sheets."
        )
        footer.setObjectName("footerNote")
        main_layout.addWidget(footer)

        self._mixed_card.browseRequested.connect(self._choose_mixed_folder)
        self._mixed_card.clearRequested.connect(self._clear_mixed_folder)
        self._mixed_card.folderChanged.connect(self._on_mixed_folder_changed)
        self._before_card.browseRequested.connect(lambda: self._choose_folder_for_slot("before"))
        self._before_card.clearRequested.connect(lambda: self._clear_slot("before"))
        self._before_card.folderChanged.connect(lambda path: self._set_folder_slot("before", path))
        self._after_card.browseRequested.connect(lambda: self._choose_folder_for_slot("after"))
        self._after_card.clearRequested.connect(lambda: self._clear_slot("after"))
        self._after_card.folderChanged.connect(lambda path: self._set_folder_slot("after", path))

        self.set_mode(self._mode)

    def _apply_initial_state(self) -> None:
        if self._mixed_folder:
            self._mixed_card.set_folder(self._mixed_folder)
        if self._before_folder:
            self._before_card.set_folder(self._before_folder)
        if self._after_folder:
            self._after_card.set_folder(self._after_folder)
        self._refresh_summary_cards()
        self._refresh_input_view()

    def set_mode(self, mode: str) -> None:
        self._mode = "paired" if mode == "paired" else "mixed"
        self._mixed_mode_button.setChecked(self._mode == "mixed")
        self._paired_mode_button.setChecked(self._mode == "paired")
        self._input_stack.setCurrentIndex(0 if self._mode == "mixed" else 1)
        self._ensure_output_root()
        self._refresh_summary_cards()
        self._update_run_state()

    def _refresh_input_view(self) -> None:
        self._mixed_card.set_folder(self._mixed_folder)
        self._before_card.set_folder(self._before_folder)
        self._after_card.set_folder(self._after_folder)

    def _refresh_summary_cards(self) -> None:
        self._mode_card.set_value("Advanced mixed folder" if self._mode == "mixed" else "IFB / GMP folders")
        if self._mode == "mixed":
            if self._mixed_folder:
                self._inputs_card.set_value(
                    f"{_display_path(self._mixed_folder)}\n{self._mixed_card.pdf_count()} PDFs"
                )
            else:
                self._inputs_card.set_value("Drop a mixed folder")
        else:
            before = _display_path(self._before_folder)
            after = _display_path(self._after_folder)
            before_count = self._before_card.pdf_count()
            after_count = self._after_card.pdf_count()
            self._inputs_card.set_value(
                f"IFB: {before} ({before_count} PDFs)\nGMP: {after} ({after_count} PDFs)"
            )
        self._output_card.set_value(_display_path(self._output_root))

    def _set_job_state(self, state: str) -> None:
        self._job_state = state
        self._refresh_status_card()
        self._refresh_run_badge()

    def _refresh_run_badge(self) -> None:
        labels = {
            "idle": "Idle",
            "running": "Running",
            "stopping": "Stopping",
            "broken": "Broken",
            "complete": "Complete",
            "failed": "Failed",
            "cancelled": "Stopped",
        }
        self._run_badge.setText(labels.get(self._job_state, self._job_state.title()))
        self._run_badge.setProperty("state", self._job_state)
        self._run_badge.style().unpolish(self._run_badge)
        self._run_badge.style().polish(self._run_badge)
        self._run_badge.update()

    def _current_file_text(self) -> str:
        stage = str(self._last_progress_payload.get("stage") or "")
        if stage == "pairing-summary":
            return "Preparing compare queue"
        if stage == "writing-report":
            return "Writing report package"
        if stage == "complete":
            return "Complete"
        if stage == "cancelled":
            return "Stopped"
        sheet_id = self._last_progress_payload.get("sheet_id")
        current_file = self._last_progress_payload.get("current_file")
        if sheet_id and current_file:
            return f"{sheet_id} | {Path(str(current_file)).name}"
        if sheet_id:
            return str(sheet_id)
        if current_file:
            return Path(str(current_file)).name
        return "None"

    def _update_runtime_meta(self) -> None:
        elapsed = None if self._job_started_at is None else max(0.0, time.monotonic() - self._job_started_at)
        eta = None
        if elapsed is not None and 0 < self._last_progress_percent < 100:
            remaining_fraction = (100 - self._last_progress_percent) / max(self._last_progress_percent, 1)
            eta = elapsed * remaining_fraction
        pair_index = self._last_progress_payload.get("pair_index")
        pair_total = self._last_progress_payload.get("pair_total")
        page_index = self._last_progress_payload.get("page_index")
        page_total = self._last_progress_payload.get("page_total")
        pair_text = ""
        if pair_index and pair_total:
            pair_text = f" | Pair {pair_index}/{pair_total}"
        page_text = ""
        if page_index is not None and page_total:
            page_text = f" | Page {int(page_index) + 1}/{int(page_total)}"
        self._runtime_label.setText(
            f"Elapsed {_format_duration(elapsed)} | ETA {_format_duration(eta)}{pair_text}{page_text}"
        )
        self._current_file_label.setText(f"Current file: {self._current_file_text()}")

    def _stall_threshold_seconds(self) -> float:
        stage = str(self._last_progress_payload.get("stage") or "")
        if stage.startswith("rendering"):
            return 180.0
        if stage.startswith("analyzing") or stage.startswith("comparing"):
            return 120.0
        if stage == "scan":
            return 60.0
        return 90.0

    def _refresh_status_card(self) -> None:
        if self._job_state == "running":
            status = self._last_progress_payload.get("message") or "Running"
        elif self._job_state == "stopping":
            status = "Stopping after the current safe checkpoint..."
        elif self._job_state == "broken":
            status = "No activity detected. Check the log and output folder."
        elif self._job_state == "complete":
            status = "Complete"
        elif self._job_state == "cancelled":
            status = "Stopped with partial output"
        elif self._job_state == "failed":
            status = "Failed"
        else:
            status = _format_status_label(self._mode, self._mixed_folder, self._before_folder, self._after_folder)
        self._status_card.set_value(status)

    def _mark_activity(self) -> None:
        self._last_activity_at = time.monotonic()
        if self._worker is not None and self._job_state == "broken":
            self._set_job_state("running")
            self._log_line("Activity resumed after stall warning.")

    def _on_health_timer(self) -> None:
        if self._worker is None:
            self._health_timer.stop()
            return
        self._update_runtime_meta()
        if self._last_activity_at is None:
            return
        stalled_for = time.monotonic() - self._last_activity_at
        if stalled_for > self._stall_threshold_seconds() and self._job_state == "running":
            self._set_job_state("broken")
            self._set_progress_message("No activity detected")
            self._log_line(
                f"No activity detected for {_format_duration(stalled_for)}. Marking run as broken until activity resumes.",
                mark_activity=False,
            )

    def _log_line(self, message: str, *, mark_activity: bool = True) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self._log.appendPlainText(f"[{stamp}] {message}")
        scrollbar = self._log.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        if mark_activity and self._worker is not None:
            self._mark_activity()

    def _ensure_output_root(self) -> None:
        if self._output_root_user_selected:
            self._refresh_summary_cards()
            return
        if self._mode == "mixed" and self._mixed_folder:
            self._output_root = _default_output_root(self._mixed_folder)
        elif self._mode == "paired":
            self._output_root = _default_output_root(self._before_folder, self._after_folder)
        self._refresh_summary_cards()

    def _choose_output_root(self) -> None:
        start_dir = self._output_root if self._output_root and Path(self._output_root).exists() else str(Path.cwd())
        folder = QFileDialog.getExistingDirectory(self, "Choose output folder", start_dir)
        if folder:
            self._output_root = str(_safe_resolve(folder))
            self._output_root_user_selected = True
            self._refresh_summary_cards()
            self._log_line(f"Output root set to {self._output_root}")
            self._update_run_state()

    def _choose_mixed_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, "Choose mixed PDF folder", self._mixed_folder or str(Path.cwd()))
        if folder:
            self._mixed_folder = str(_safe_resolve(folder))
            self._ensure_output_root()
            self._refresh_input_view()
            self._log_line(f"Mixed folder selected: {self._mixed_folder}")
            self._update_run_state()

    def _choose_folder_for_slot(self, slot: str) -> None:
        start_dir = self._before_folder if slot == "before" else self._after_folder
        title = "Choose IFB folder" if slot == "before" else "Choose GMP folder"
        folder = QFileDialog.getExistingDirectory(self, title, start_dir or str(Path.cwd()))
        if folder:
            self._set_folder_slot(slot, str(_safe_resolve(folder)))

    def _clear_mixed_folder(self) -> None:
        self._mixed_folder = None
        self._refresh_input_view()
        self._refresh_summary_cards()
        self._log_line("Mixed folder cleared.")
        self._update_run_state()

    def _clear_slot(self, slot: str) -> None:
        if slot == "before":
            self._before_folder = None
        else:
            self._after_folder = None
        self._refresh_input_view()
        self._refresh_summary_cards()
        self._log_line(f"{'IFB' if slot == 'before' else 'GMP'} folder cleared.")
        self._update_run_state()

    def _set_folder_slot(self, slot: str, path: str) -> None:
        if slot == "before":
            self._before_folder = path
        else:
            self._after_folder = path
        self._ensure_output_root()
        self._refresh_input_view()
        self._log_line(f"{'IFB' if slot == 'before' else 'GMP'} folder set to {path}")
        self._update_run_state()

    def _on_mixed_folder_changed(self, path: str) -> None:
        self._mixed_folder = path
        self._ensure_output_root()
        self._refresh_input_view()
        self._log_line(f"Mixed folder dropped: {path}")
        self._update_run_state()

    def _swap_before_after(self) -> None:
        self._before_folder, self._after_folder = self._after_folder, self._before_folder
        self._ensure_output_root()
        self._refresh_input_view()
        self._refresh_summary_cards()
        self._log_line("IFB and GMP folders swapped.")
        self._update_run_state()

    def _clear_all(self) -> None:
        self._mixed_folder = None
        self._before_folder = None
        self._after_folder = None
        self._active_output_dir = None
        self._job_started_at = None
        self._last_activity_at = None
        self._last_progress_percent = 0
        self._last_progress_payload = {}
        self._set_job_state("idle")
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(0)
        self._set_progress_message("Ready")
        self._runtime_label.setText("Elapsed 00:00 | ETA --:--")
        self._current_file_label.setText("Current file: None")
        self._open_output_button.setEnabled(False)
        self._health_timer.stop()
        self._log.clear()
        self._ensure_output_root()
        self._refresh_input_view()
        self._log_line("Inputs cleared.")
        self._update_run_state()

    def _validation_message(self) -> str | None:
        if self._mode == "mixed":
            if not self._mixed_folder:
                return "Select a mixed PDF folder."
            folder = Path(self._mixed_folder)
            if not folder.is_dir():
                return "Mixed PDF folder must be an existing folder."
            if _count_pdfs(self._mixed_folder) == 0:
                return "Mixed PDF folder has no PDFs."
        else:
            if not self._before_folder:
                return "Select the IFB folder."
            if not self._after_folder:
                return "Select the GMP folder."
            before = Path(self._before_folder)
            after = Path(self._after_folder)
            if not before.is_dir():
                return "IFB folder must be an existing folder."
            if not after.is_dir():
                return "GMP folder must be an existing folder."
            if before.resolve() == after.resolve():
                return "IFB and GMP folders must be different."
            if _count_pdfs(self._before_folder) == 0:
                return "IFB folder has no PDFs."
            if _count_pdfs(self._after_folder) == 0:
                return "GMP folder has no PDFs."
        if not self._output_root:
            return "Choose an output folder."
        output = Path(self._output_root)
        try:
            if output.exists():
                if not output.is_dir():
                    return "Output path must be a folder."
                probe_dir = output
            else:
                probe_dir = output.parent
                if not probe_dir.exists():
                    return "Output folder parent does not exist."
            probe = probe_dir / ".pdfcompare_write_probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
        except Exception as exc:  # noqa: BLE001
            return f"Output folder is not writable: {exc}"
        return None

    def _current_selection_valid(self) -> bool:
        return self._validation_message() is None

    def _update_run_state(self) -> None:
        validation_message = self._validation_message()
        ready = validation_message is None
        self._run_button.setEnabled(ready and self._worker is None)
        self._stop_button.setEnabled(self._worker is not None and self._job_state in {"running", "broken"})
        self._output_button.setEnabled(self._worker is None)
        self._clear_button.setEnabled(self._worker is None)
        self._mixed_mode_button.setEnabled(self._worker is None)
        self._paired_mode_button.setEnabled(self._worker is None)
        self._swap_button.setEnabled(self._worker is None)
        self._mixed_card.set_interactive(self._worker is None)
        self._before_card.set_interactive(self._worker is None)
        self._after_card.set_interactive(self._worker is None)
        self._refresh_status_card()
        if self._worker is None and validation_message:
            self._status_card.set_value(validation_message)

    def _set_progress_message(self, message: str) -> None:
        text = str(message or "")
        self._progress_label.setToolTip(text)
        width = self._progress_label.width() or self._progress_label.maximumWidth() or 560
        elided = self._progress_label.fontMetrics().elidedText(text, Qt.ElideMiddle, max(80, width - 6))
        self._progress_label.setText(elided)
        self._refresh_run_badge()
        self._refresh_summary_cards()

    def _run_compare(self) -> None:
        if self._worker is not None:
            return
        if not self._current_selection_valid():
            message = self._validation_message() or "Select the IFB and GMP folders before running the compare."
            QMessageBox.information(
                self,
                "Missing input",
                message,
            )
            return

        if not self._output_root:
            self._output_root = _default_output_root(self._mixed_folder, self._before_folder, self._after_folder)

        run_id = _timestamp_slug()
        request: dict[str, Any] = {
            "mode": self._mode,
            "run_id": run_id,
            "output_root": self._output_root,
            "performance": {
                "max_workers": self._max_workers,
            },
            "inputs": {
                "mixed_folder": self._mixed_folder,
                "before_folder": self._before_folder,
                "after_folder": self._after_folder,
                "ifb_folder": self._before_folder,
                "gmp_folder": self._after_folder,
            },
            "ui": {
                "source": "PySide6",
                "window_title": self.windowTitle(),
            },
        }

        self._active_output_dir = str(Path(self._output_root) / run_id)
        self._job_started_at = time.monotonic()
        self._last_activity_at = self._job_started_at
        self._last_progress_percent = 0
        self._last_progress_payload = {}
        self._set_job_state("running")
        self._log_line("Starting compare job...")
        self._log_line(f"Output package will be written under {self._active_output_dir}")
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(0)
        self._set_progress_message("Preparing job...")
        self._runtime_label.setText("Elapsed 00:00 | ETA --:--")
        self._current_file_label.setText("Current file: Preparing job")
        self._run_button.setEnabled(False)
        self._stop_button.setEnabled(True)
        self._output_button.setEnabled(False)
        self._clear_button.setEnabled(False)
        self._open_output_button.setEnabled(False)
        self._mixed_mode_button.setEnabled(False)
        self._paired_mode_button.setEnabled(False)
        self._swap_button.setEnabled(False)
        self._health_timer.start()

        self._worker = CompareWorker(request, self)
        self._worker.progress.connect(self._on_worker_progress)
        self._worker.log.connect(self._log_line)
        self._worker.completed.connect(self._on_worker_completed)
        self._worker.cancelled.connect(self._on_worker_cancelled)
        self._worker.failed.connect(self._on_worker_failed)
        self._worker.finished.connect(self._on_worker_finished)
        self._worker.start()

    def _request_stop(self) -> None:
        if self._worker is None:
            return
        self._worker.request_cancel()
        self._set_job_state("stopping")
        self._stop_button.setEnabled(False)
        self._set_progress_message("Stopping after current checkpoint...")
        self._log_line("Stop requested. Waiting for a safe checkpoint...")

    def _on_worker_progress(self, payload: Any) -> None:
        data = payload if isinstance(payload, dict) else {"percent": 0, "message": str(payload)}
        percent = max(0, min(100, int(data.get("percent", 0))))
        percent = max(self._last_progress_percent, percent)
        message = str(data.get("message") or f"{percent}%")
        if self._progress_bar.maximum() == 0:
            self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(percent)
        self._set_progress_message(message)
        self._last_progress_percent = percent
        self._last_progress_payload = data
        self._mark_activity()
        if self._job_state == "broken":
            self._set_job_state("running")
        self._update_runtime_meta()
        self._refresh_status_card()

    def _on_worker_completed(self, result: Any) -> None:
        output_dir = _extract_output_dir(result) or self._active_output_dir or self._output_root
        report_path = _extract_report_path(result)
        self._active_output_dir = output_dir
        self._set_job_state("complete")
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(100)
        self._set_progress_message("Complete")
        self._last_progress_percent = 100
        self._update_runtime_meta()
        self._health_timer.stop()
        self._stop_button.setEnabled(False)
        self._open_output_button.setEnabled(bool(output_dir))
        if output_dir:
            self._log_line(f"Output folder: {output_dir}")
        if report_path:
            self._log_line(f"Report: {report_path}")
        self._log_line("Compare job completed successfully.")

    def _on_worker_cancelled(self, result: Any) -> None:
        output_dir = _extract_output_dir(result) or self._active_output_dir or self._output_root
        report_path = _extract_report_path(result)
        message = _extract_text(result, ("message",)) or "Compare job stopped by user."
        self._active_output_dir = output_dir
        self._set_job_state("cancelled")
        self._progress_bar.setRange(0, 100)
        self._set_progress_message("Stopped")
        self._update_runtime_meta()
        self._health_timer.stop()
        self._stop_button.setEnabled(False)
        self._open_output_button.setEnabled(bool(output_dir))
        self._log_line(message)
        if output_dir:
            self._log_line(f"Partial output folder: {output_dir}")
        if report_path:
            self._log_line(f"Partial report: {report_path}")

    def _on_worker_failed(self, message: str) -> None:
        self._set_job_state("failed")
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(0)
        self._set_progress_message("Failed")
        self._health_timer.stop()
        self._stop_button.setEnabled(False)
        self._open_output_button.setEnabled(bool(self._active_output_dir))
        self._log_line(f"Compare job failed: {message}")
        QMessageBox.critical(self, "Compare failed", message)

    def _on_worker_finished(self) -> None:
        self._health_timer.stop()
        self._worker = None
        self._update_run_state()
        if self._close_after_worker:
            self._close_after_worker = False
            self.close()

    def _open_output_folder(self) -> None:
        target = self._active_output_dir or self._output_root
        if not target:
            QMessageBox.information(self, "No output yet", "Run a compare job first to create an output folder.")
            return
        _open_in_file_manager(target)

    def dragEnterEvent(self, event: QDragEnterEvent) -> None:
        if self._worker is not None:
            event.ignore()
            return
        if _paths_from_mime(event.mimeData()):
            event.acceptProposedAction()

    def dropEvent(self, event: QDropEvent) -> None:
        if self._worker is not None:
            return
        folders = _paths_from_mime(event.mimeData())
        if not folders:
            return

        if self._mode == "mixed":
            if len(folders) == 1:
                self._mixed_folder = folders[0]
            else:
                self.set_mode("paired")
                self._before_folder = folders[0]
                self._after_folder = folders[1]
        else:
            if len(folders) >= 2:
                self._before_folder = folders[0]
                self._after_folder = folders[1]
            elif len(folders) == 1:
                if not self._before_folder:
                    self._before_folder = folders[0]
                elif not self._after_folder:
                    self._after_folder = folders[0]
                else:
                    self._before_folder = folders[0]

        self._ensure_output_root()
        self._refresh_input_view()
        self._log_line(f"Dropped folder(s): {', '.join(folders)}")
        self._update_run_state()
        event.acceptProposedAction()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if self._worker is not None:
            response = QMessageBox.question(
                self,
                "Compare in progress",
                "A compare job is still running. Request stop and close this window after the worker exits?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes,
            )
            if response == QMessageBox.Yes:
                self._close_after_worker = True
                self._request_stop()
            event.ignore()
            return
        super().closeEvent(event)


def build_stylesheet() -> str:
    return """
        QWidget#root {
            background: #f4f7fb;
            color: #1f2937;
            font-family: "Segoe UI Variable Text", "Segoe UI", sans-serif;
            font-size: 10.5pt;
        }
        QFrame#heroCard, QFrame#statusCard, QFrame#folderCard {
            background: #ffffff;
            border: 1px solid #d7e0eb;
            border-radius: 18px;
        }
        QFrame#heroCard {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ffffff, stop:1 #eef4ff);
        }
        QLabel#heroTitle {
            font-size: 25px;
            font-weight: 700;
            color: #0f172a;
        }
        QLabel#heroSubtitle {
            font-size: 10.5pt;
            color: #475569;
        }
        QLabel#modeHint, QLabel#footerNote {
            color: #64748b;
            font-size: 9.5pt;
        }
        QLabel#runBadge {
            padding: 7px 12px;
            border-radius: 999px;
            font-size: 9pt;
            font-weight: 700;
            background: #e2e8f0;
            color: #334155;
        }
        QLabel#runBadge[state="idle"] {
            background: #e2e8f0;
            color: #334155;
        }
        QLabel#runBadge[state="running"] {
            background: #dcfce7;
            color: #166534;
        }
        QLabel#runBadge[state="stopping"] {
            background: #fef3c7;
            color: #92400e;
        }
        QLabel#runBadge[state="broken"] {
            background: #fee2e2;
            color: #991b1b;
        }
        QLabel#runBadge[state="complete"] {
            background: #dbeafe;
            color: #1d4ed8;
        }
        QLabel#runBadge[state="failed"] {
            background: #fee2e2;
            color: #991b1b;
        }
        QLabel#runBadge[state="cancelled"] {
            background: #e2e8f0;
            color: #475569;
        }
        QFrame#statusCard {
            min-height: 94px;
        }
        QLabel#statusTitle {
            color: #64748b;
            font-size: 9.5pt;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        QLabel#statusValue {
            color: #0f172a;
            font-size: 11pt;
            font-weight: 600;
        }
        QLabel#progressLabel {
            font-weight: 600;
            color: #0f172a;
        }
        QLabel#progressTitle {
            color: #475569;
            font-size: 9.5pt;
            font-weight: 600;
            min-width: 150px;
        }
        QLabel#runtimeLabel, QLabel#currentFileLabel {
            color: #475569;
            font-size: 9.5pt;
        }
        QLabel#cardTitle {
            color: #0f172a;
            font-size: 12pt;
            font-weight: 700;
        }
        QLabel#cardDescription, QLabel#cardPath, QLabel#cardCount {
            color: #475569;
        }
        QLabel#cardPath {
            font-size: 9.5pt;
        }
        QLabel#cardCount {
            font-size: 10pt;
            font-weight: 600;
            color: #1d4ed8;
        }
        QFrame#folderCard[selected="true"] {
            border: 1px solid #7aa7ff;
            background: #f8fbff;
        }
        QPushButton, QToolButton {
            background: #ffffff;
            color: #0f172a;
            border: 1px solid #c9d5e3;
            border-radius: 12px;
            padding: 10px 14px;
            font-weight: 600;
        }
        QPushButton:hover, QToolButton:hover {
            background: #f8fbff;
            border-color: #8ab1ff;
        }
        QPushButton:pressed, QToolButton:pressed {
            background: #e8f0ff;
        }
        QPushButton:disabled, QToolButton:disabled {
            background: #eef2f7;
            color: #94a3b8;
            border-color: #e2e8f0;
        }
        QPushButton:checked {
            background: #dbeafe;
            border-color: #7aa7ff;
            color: #1d4ed8;
        }
        QPushButton#primaryButton {
            background: #2563eb;
            color: white;
            border-color: #1d4ed8;
        }
        QPushButton#primaryButton:hover {
            background: #1d4ed8;
        }
        QPushButton#primaryButton:pressed {
            background: #1e40af;
        }
        QPlainTextEdit#logPanel {
            background: #0b1220;
            color: #dbeafe;
            border: 1px solid #16243a;
            border-radius: 16px;
            padding: 14px;
            selection-background-color: #1d4ed8;
            font-family: "Cascadia Mono", "Consolas", monospace;
            font-size: 9.5pt;
        }
        QProgressBar {
            border: 1px solid #c9d5e3;
            border-radius: 10px;
            background: #ffffff;
            height: 18px;
            text-align: center;
            color: #0f172a;
        }
        QProgressBar::chunk {
            border-radius: 10px;
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #2c6bed, stop:1 #5b9dff);
        }
    """


def apply_application_defaults(app: QApplication) -> None:
    app.setStyle("Fusion")
    palette = QPalette()
    palette.setColor(QPalette.Window, Qt.white)
    palette.setColor(QPalette.WindowText, Qt.black)
    palette.setColor(QPalette.Base, Qt.white)
    palette.setColor(QPalette.AlternateBase, Qt.white)
    palette.setColor(QPalette.ToolTipBase, Qt.white)
    palette.setColor(QPalette.ToolTipText, Qt.black)
    palette.setColor(QPalette.Text, Qt.black)
    palette.setColor(QPalette.Button, Qt.white)
    palette.setColor(QPalette.ButtonText, Qt.black)
    palette.setColor(QPalette.Highlight, Qt.blue)
    palette.setColor(QPalette.HighlightedText, Qt.white)
    app.setPalette(palette)
    app.setStyleSheet(build_stylesheet())


def create_window(
    initial_mode: str = "paired",
    mixed_folder: str | None = None,
    before_folder: str | None = None,
    after_folder: str | None = None,
    output_root: str | None = None,
    max_workers: int | None = None,
) -> UniversalPdfCompareWindow:
    window = UniversalPdfCompareWindow(
        initial_mode=initial_mode,
        mixed_folder=mixed_folder,
        before_folder=before_folder,
        after_folder=after_folder,
        output_root=output_root,
        max_workers=max_workers,
    )
    window.setWindowIcon(window.style().standardIcon(QStyle.SP_FileDialogInfoView))
    return window
