from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
import json
from multiprocessing import Manager
import os
from pathlib import Path
import re
import shutil
import threading
import time
from typing import Any, Callable

import fitz

from .analysis import AnalysisCancelledError
from .compare import CompareCancelledError, compare_pair, is_scope_region_diff, is_scope_row_diff
from .pairing import PairDecision, PairingCancelledError, PdfRecord, scan_and_pair
from .reporting import write_report_bundle

ProgressCallback = Callable[..., None] | None
LogCallback = Callable[[str], None] | None
CancelCallback = Callable[[], bool] | None
_WORKER_CANCEL_EVENT: Any = None


class JobCancelledError(RuntimeError):
    def __init__(self, message: str, result: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.result = result


@dataclass(slots=True)
class CompareJobConfig:
    mode: str
    input_paths: list[Path]
    output_root: Path
    run_id: str
    highlight_mode: str = "balanced"
    max_workers: int = 1

    @property
    def output_dir(self) -> Path:
        return self.output_root / self.run_id

    @classmethod
    def from_request(cls, request: dict[str, Any]) -> "CompareJobConfig":
        mode = "mixed" if request.get("mode") == "mixed" else "paired"
        output_root = Path(request.get("output_root") or Path.cwd() / "IFB_GMP_Compare_Output").resolve()
        run_id = request.get("run_id") or datetime.now().strftime("%Y%m%d_%H%M%S")
        performance = request.get("performance", {})
        requested_workers = request.get("max_workers") or performance.get("max_workers")
        cpu_count = max(1, os.cpu_count() or 1)
        default_workers = min(4, cpu_count)
        try:
            max_workers = int(requested_workers) if requested_workers is not None else default_workers
        except (TypeError, ValueError):
            max_workers = default_workers
        max_workers = max(1, min(min(16, cpu_count), max_workers))
        inputs = request.get("inputs", {})
        if mode == "mixed":
            mixed_folder = inputs.get("mixed_folder")
            if not mixed_folder:
                raise ValueError("Mixed mode requires inputs.mixed_folder")
            input_paths = [Path(mixed_folder).resolve()]
        else:
            before_folder = inputs.get("ifb_folder") or inputs.get("before_folder")
            after_folder = inputs.get("gmp_folder") or inputs.get("after_folder")
            if not before_folder or not after_folder:
                raise ValueError("IFB/GMP mode requires inputs.ifb_folder and inputs.gmp_folder")
            input_paths = [Path(before_folder).resolve(), Path(after_folder).resolve()]
        return cls(mode=mode, input_paths=input_paths, output_root=output_root, run_id=run_id, max_workers=max_workers)


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    return slug[:120] or "document"


def _ensure_dirs(config: CompareJobConfig) -> dict[str, Path]:
    output_dir = config.output_dir
    paths = {
        "output_dir": output_dir,
        "changed_pairs": output_dir / "changed_pairs",
        "added": output_dir / "added",
        "removed": output_dir / "removed",
        "review_needed": output_dir / "review_needed",
        "logs": output_dir / "logs",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def _emit_progress(callback: ProgressCallback, percent: int, message: str, **extra: Any) -> None:
    if callback is not None:
        callback(percent=percent, message=message, **extra)


def _check_cancel(cancel_requested: CancelCallback) -> None:
    if cancel_requested is not None and cancel_requested():
        raise JobCancelledError("Compare job stopped by user.")


def _fraction(current: Any, total: Any) -> float:
    try:
        current_value = float(current)
        total_value = float(total)
    except (TypeError, ValueError):
        return 0.0
    if total_value <= 0:
        return 0.0
    return max(0.0, min(1.0, current_value / total_value))


def _count_input_pdfs(input_paths: list[Path]) -> int:
    return sum(1 for path in input_paths for _entry in path.rglob("*.pdf"))


def _decision_work_weight(decision: PairDecision) -> float:
    if decision.status == "matched" and decision.before and decision.after:
        before_pages = max(decision.before.page_count, 1)
        after_pages = max(decision.after.page_count, 1)
        compare_pages = max(before_pages, after_pages)
        render_pages = before_pages + after_pages
        return float(before_pages + after_pages + (compare_pages * 0.8) + (render_pages * 0.45))
    return 1.0


@dataclass(slots=True)
class _WholeProgramProgressTracker:
    scan_total: int = 1
    pairing_total: int = 1
    decision_weights: dict[int, float] = field(default_factory=dict)
    total_decision_weight: float = 1.0
    completed_decision_weight: float = 0.0
    completed_decisions: set[int] = field(default_factory=set)
    last_percent: int = 0
    scan_share: float = 0.12
    pairing_share: float = 0.08
    processing_share: float = 0.75
    report_share: float = 0.05

    def configure_scan(self, total: int) -> None:
        self.scan_total = max(int(total), 1)

    def configure_pairing(self, total: int) -> None:
        self.pairing_total = max(int(total), 1)

    def configure_decisions(self, decisions: list[PairDecision]) -> None:
        self.decision_weights = {index + 1: _decision_work_weight(decision) for index, decision in enumerate(decisions)}
        self.total_decision_weight = sum(self.decision_weights.values()) or 1.0
        self.completed_decision_weight = 0.0
        self.completed_decisions.clear()

    def mark_decision_complete(self, decision_index: int | None) -> None:
        if decision_index is None:
            return
        if decision_index in self.completed_decisions:
            return
        self.completed_decisions.add(decision_index)
        self.completed_decision_weight = min(
            self.total_decision_weight,
            self.completed_decision_weight + self.decision_weights.get(decision_index, 1.0),
        )

    def _decision_stage_fraction(self, payload: dict[str, Any]) -> float:
        stage = str(payload.get("stage") or "")
        if stage == "compare-queue":
            return 0.01
        if stage == "compare-pair":
            return 0.03
        if stage == "analyzing-before":
            return 0.03 + (_fraction(payload.get("current"), payload.get("total")) * 0.34)
        if stage == "analyzing-after":
            return 0.37 + (_fraction(payload.get("current"), payload.get("total")) * 0.31)
        if stage == "comparing-pages":
            return 0.68 + (_fraction(payload.get("current"), payload.get("total")) * 0.20)
        if stage == "rendering-before":
            return 0.88 + (_fraction(payload.get("current"), payload.get("total")) * 0.05)
        if stage == "rendering-after":
            return 0.93 + (_fraction(payload.get("current"), payload.get("total")) * 0.05)
        if stage == "pair-complete":
            return 1.0
        if stage == "processing-unmatched-start":
            return 0.20
        if stage == "processing-unmatched-complete":
            return 1.0
        return 0.0

    def percent_for_payload(self, payload: dict[str, Any]) -> int:
        stage = str(payload.get("stage") or "")
        if stage == "starting":
            return self._monotonic(0)
        if stage == "scan":
            return self._monotonic(round(self.scan_share * _fraction(payload.get("current"), payload.get("total")) * 100))
        if stage == "pairing":
            overall = self.scan_share + (self.pairing_share * _fraction(payload.get("current"), payload.get("total")))
            return self._monotonic(round(overall * 100))
        if stage == "pairing-summary":
            return self._monotonic(round((self.scan_share + self.pairing_share) * 100))
        if stage in {
            "compare-queue",
            "compare-pair",
            "analyzing-before",
            "analyzing-after",
            "comparing-pages",
            "rendering-before",
            "rendering-after",
            "pair-complete",
            "processing-unmatched-start",
            "processing-unmatched-complete",
        }:
            decision_index = int(payload.get("decision_index") or 0)
            decision_weight = self.decision_weights.get(decision_index, 1.0)
            current_weight = self.completed_decision_weight + (decision_weight * self._decision_stage_fraction(payload))
            process_fraction = current_weight / max(self.total_decision_weight, 1.0)
            overall = self.scan_share + self.pairing_share + (self.processing_share * process_fraction)
            return self._monotonic(round(overall * 100))
        if stage == "writing-report":
            overall = self.scan_share + self.pairing_share + self.processing_share + (self.report_share * 0.65)
            return self._monotonic(round(overall * 100))
        if stage == "complete":
            return self._monotonic(100)
        if stage == "cancelled":
            return self.last_percent
        return self.last_percent

    def _monotonic(self, percent: int) -> int:
        clamped = max(0, min(100, int(percent)))
        self.last_percent = max(self.last_percent, clamped)
        return self.last_percent


def _message_for_stage(payload: dict[str, Any]) -> str:
    stage = str(payload.get("stage") or "")
    current = payload.get("current") or 0
    total = payload.get("total") or 0
    current_file = payload.get("current_file") or ""
    pair_index = payload.get("pair_index")
    pair_total = payload.get("pair_total")
    decision_index = payload.get("decision_index")
    decision_total = payload.get("decision_total")
    page_index = payload.get("page_index")
    page_total = payload.get("page_total")

    if stage == "scan":
        message = f"Scanning PDFs... {int(current)}/{int(total) if total else 0}"
        if current_file:
            message = f"{message} | {Path(str(current_file)).name}"
        return message

    if stage == "pairing":
        message = f"Pairing sheets... {int(current)}/{int(total) if total else 0}"
        return message

    if stage.startswith("analyzing") or stage.startswith("comparing") or stage.startswith("rendering") or stage in {
        "compare-pair",
        "compare-queue",
        "pair-complete",
    }:
        pair_total_value = max(int(pair_total or 1), 1)
        pair_position = max(int(pair_index or 1), 1)
        if stage == "analyzing-before":
            action = "Analyzing before"
        elif stage == "analyzing-after":
            action = "Analyzing after"
        elif stage == "comparing-pages":
            action = "Comparing pages"
        elif stage == "rendering-before":
            action = "Rendering before highlights"
        elif stage == "rendering-after":
            action = "Rendering after highlights"
        elif stage == "pair-complete":
            action = "Finished pair"
        elif stage == "compare-queue":
            action = "Starting pair"
        else:
            action = "Preparing pair"
        message = f"{action} {pair_position}/{pair_total_value}"
        if current_file:
            message = f"{message} | {Path(str(current_file)).name}"
        if page_index is not None and page_total:
            message = f"{message} | page {int(page_index) + 1}/{int(page_total)}"
        return message

    if stage == "processing-unmatched-start":
        action = "Packaging document"
        message = f"{action} {int(decision_index or 0)}/{int(decision_total or 0)}"
        if current_file:
            message = f"{message} | {Path(str(current_file)).name}"
        return message

    if stage == "processing-unmatched-complete":
        status = str(payload.get("document_status") or "document")
        message = f"Finished {status} document {int(decision_index or 0)}/{int(decision_total or 0)}"
        if current_file:
            message = f"{message} | {Path(str(current_file)).name}"
        return message

    if stage == "pairing-summary":
        return "Pairing complete. Preparing compare queue..."

    if stage == "writing-report":
        return "Writing Excel report and manifest..."

    if stage == "parallel-active":
        active = payload.get("active_workers")
        pending = payload.get("pending_pairs")
        if active is not None and pending is not None:
            return f"Comparing matched sheets... {active} active, {pending} queued/running"
        return "Comparing matched sheets..."

    return str(payload.get("message") or "Working...")


def _make_logger(log_path: Path, callback: LogCallback) -> Callable[[str], None]:
    lines: list[str] = []
    lock = threading.Lock()

    def log(message: str) -> None:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{stamp}] {message}"
        with lock:
            lines.append(entry)
            log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            if callback is not None:
                callback(message)

    return log


def _copy_original(record: PdfRecord | None, destination_dir: Path, prefix: str = "") -> str | None:
    if record is None:
        return None
    destination_dir.mkdir(parents=True, exist_ok=True)
    target_name = f"{prefix}{record.path.name}" if prefix else record.path.name
    target_path = destination_dir / target_name
    shutil.copy2(record.path, target_path)
    return str(target_path)


def _append_pdf_to_review_set(
    review_doc: fitz.Document,
    source: Any,
    title: str,
    toc_entries: list[list[Any]],
    level: int,
    log: LogCallback,
) -> bool:
    if not source:
        return False
    source_path = Path(str(source))
    if not source_path.exists() or not source_path.is_file():
        return False
    try:
        with fitz.open(source_path) as source_doc:
            if source_doc.page_count <= 0:
                return False
            start_page = review_doc.page_count + 1
            review_doc.insert_pdf(source_doc)
            toc_entries.append([level, title, start_page])
            return True
    except Exception as exc:  # noqa: BLE001
        if log is not None:
            log(f"Skipped Bluebeam PDF source {source_path}: {type(exc).__name__}: {exc}")
        return False


def _bluebeam_sheet_title(order: int, row: dict[str, Any], suffix: str | None = None) -> str:
    sheet_id = str(row.get("sheet_id") or f"sheet_{order:03d}")
    title = str(row.get("sheet_title") or "").strip()
    parts = [f"{order:03d}", sheet_id]
    if suffix:
        parts.append(suffix)
    if title:
        parts.append(title)
    return " - ".join(parts)


def _build_bluebeam_review_package(output_dir: Path, document_rows: list[dict[str, Any]], log: LogCallback) -> dict[str, Any]:
    stale_review_dir = output_dir / "bluebeam_review"
    if stale_review_dir.exists():
        shutil.rmtree(stale_review_dir, ignore_errors=True)

    review_pdf = output_dir / "bluebeam_review.pdf"
    if review_pdf.exists():
        review_pdf.unlink()

    review_doc = fitz.open()
    toc_entries: list[list[Any]] = []
    included_documents = 0

    changed_rows = [
        row for row in document_rows
        if row.get("status") == "changed" and row.get("highlighted_before_pdf") and row.get("highlighted_after_pdf")
    ]
    changed_rows.sort(key=lambda row: (str(row.get("sheet_id") or ""), str(row.get("sheet_title") or "")))
    for order, row in enumerate(changed_rows, start=1):
        sheet_toc: list[list[Any]] = []
        before_added = _append_pdf_to_review_set(
            review_doc,
            row.get("highlighted_before_pdf"),
            "IFB before",
            sheet_toc,
            2,
            log,
        )
        after_added = _append_pdf_to_review_set(
            review_doc,
            row.get("highlighted_after_pdf"),
            "GMP after",
            sheet_toc,
            2,
            log,
        )
        if before_added or after_added:
            included_documents += 1
            toc_entries.append([1, _bluebeam_sheet_title(order, row), sheet_toc[0][2]])
            toc_entries.extend(sheet_toc)

    review_only_rows = [
        row for row in document_rows
        if row.get("status") in {"added", "removed", "review_needed"} and row.get("packaged_path")
    ]
    review_only_rows.sort(key=lambda row: (str(row.get("sheet_id") or ""), str(row.get("sheet_title") or ""), str(row.get("status") or "")))
    for order, row in enumerate(review_only_rows, start=len(changed_rows) + 1):
        status = str(row.get("status") or "review").replace("_", " ").upper()
        title = _bluebeam_sheet_title(order, row, status)
        source = Path(str(row.get("packaged_path")))
        sources = sorted(source.glob("*.pdf")) if source.exists() and source.is_dir() else [source]
        row_toc: list[list[Any]] = []
        for source_pdf in sources:
            _append_pdf_to_review_set(review_doc, source_pdf, source_pdf.name, row_toc, 2, log)
        if row_toc:
            included_documents += 1
            toc_entries.append([1, title, row_toc[0][2]])
            toc_entries.extend(row_toc)

    if review_doc.page_count == 0:
        page = review_doc.new_page(width=612, height=792)
        page.insert_text((72, 72), "No review-worthy changed, added, or removed sheets were found.", fontsize=14)
        toc_entries.append([1, "No review-worthy sheets", 1])

    if toc_entries:
        review_doc.set_toc(toc_entries)
    review_doc.save(review_pdf, garbage=4, deflate=True)
    page_count = review_doc.page_count
    review_doc.close()

    if log is not None:
        log(f"Bluebeam review PDF written to {review_pdf}")
    return {
        "bluebeam_review_pdf": str(review_pdf),
        "bluebeam_review_sheet_count": included_documents,
        "bluebeam_review_page_count": page_count,
    }


def _zip_changed_pairs(output_dir: Path, log: LogCallback) -> Path:
    changed_pairs_dir = output_dir / "changed_pairs"
    zip_path = output_dir / "changed_pairs.zip"
    if zip_path.exists():
        zip_path.unlink()
    shutil.make_archive(str(zip_path.with_suffix("")), "zip", root_dir=output_dir, base_dir=changed_pairs_dir.name)
    if log is not None:
        log(f"Changed pairs ZIP written to {zip_path}")
    return zip_path


def _prune_public_output(output_dir: Path, keep_names: set[str]) -> None:
    for child in output_dir.iterdir():
        if child.name in keep_names:
            continue
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)


def _pair_id(decision: PairDecision, index: int) -> str:
    sheet_id = decision.sheet_id or (decision.before.sheet_id if decision.before else None) or (decision.after.sheet_id if decision.after else None)
    title = (
        decision.after.sheet_title
        if decision.after and decision.after.sheet_title
        else decision.before.sheet_title
        if decision.before and decision.before.sheet_title
        else "review_needed"
    )
    if sheet_id:
        if decision.status == "review_needed":
            return _safe_slug(f"{index + 1:03d}_{sheet_id}__{title}")[:100]
        return _safe_slug(f"{sheet_id}__{title}")[:90]
    before = decision.before.filename_stem if decision.before else "none"
    after = decision.after.filename_stem if decision.after else "none"
    return _safe_slug(f"{index + 1:03d}_{before}__{after}")


def _record_manifest_row(record: PdfRecord) -> dict[str, Any]:
    return {
        "path": str(record.path),
        "source_group": record.source_group,
        "page_count": record.page_count,
        "page_sizes": [f"{width:.1f}x{height:.1f}" for width, height in record.page_sizes],
        "file_size": record.file_size,
        "modified_at": datetime.fromtimestamp(record.modified_at, timezone.utc).isoformat(),
        "filename_stem": record.filename_stem,
        "sheet_id": record.sheet_id,
        "revision": record.revision,
        "sheet_title": record.sheet_title,
        "family": record.family,
        "phase": record.phase,
        "revision_status": record.revision_status,
        "normalized_stem": record.normalized_stem,
        "title_hint": record.title_hint,
        "revision_hint": record.revision_hint,
        "version_hint": record.version_hint,
        "date_hints": record.date_hints,
        "native_text_char_count": record.native_text_char_count,
        "warnings": record.warnings,
    }


def _decision_row(decision: PairDecision, pair_id: str) -> dict[str, Any]:
    return {
        "pair_id": pair_id,
        "sheet_id": decision.sheet_id,
        "sheet_title": decision.after.sheet_title if decision.after else decision.before.sheet_title if decision.before else None,
        "family": decision.family,
        "status": decision.status,
        "revision_status": decision.revision_status,
        "confidence": round(decision.confidence, 3),
        "pairing_method": decision.pairing_method,
        "ifb_path": str(decision.before.path) if decision.before else None,
        "gmp_path": str(decision.after.path) if decision.after else None,
        "ifb_name": decision.before.path.name if decision.before else None,
        "gmp_name": decision.after.path.name if decision.after else None,
        "ifb_pages": decision.before.page_count if decision.before else None,
        "gmp_pages": decision.after.page_count if decision.after else None,
        "ifb_revision": decision.before.revision if decision.before else None,
        "gmp_revision": decision.after.revision if decision.after else None,
        "reasons": decision.reasons,
        "competing_candidates": decision.competing_candidates,
    }


def _summary_rows(
    documents: list[dict[str, Any]],
    decisions: list[PairDecision],
    exception_count: int,
    records: list[PdfRecord] | None = None,
    runtime_seconds: float | None = None,
) -> list[dict[str, Any]]:
    changed = sum(1 for document in documents if document.get("status") == "changed")
    unchanged = sum(1 for document in documents if document.get("status") == "unchanged")
    added = sum(1 for decision in decisions if decision.status == "added")
    removed = sum(1 for decision in decisions if decision.status == "removed")
    review_needed = (
        sum(1 for decision in decisions if decision.status == "review_needed")
        + sum(1 for document in documents if document.get("status") == "review_needed")
    )
    matched = sum(1 for decision in decisions if decision.status == "matched")
    title_only = sum(1 for document in documents if document.get("compare_status") == "title_revision_only")
    schedule_changed = sum(1 for document in documents if document.get("family") in {"schedule", "switch_schedule", "fiber_schedule"} and document.get("status") == "changed")
    plan_changed = sum(1 for document in documents if document.get("family") in {"overall_plan", "area_plan", "detail", "mdf_idf_layout"} and document.get("status") == "changed")
    ocr_used = sum(1 for document in documents if document.get("ocr_used"))
    records = records or []
    rows = [
        {"metric": "ifb_pdf_count", "value": sum(1 for record in records if record.source_group == "IFB")},
        {"metric": "gmp_pdf_count", "value": sum(1 for record in records if record.source_group == "GMP")},
        {"metric": "scanned_pdf_count", "value": len(records)},
        {"metric": "matched_pairs", "value": matched},
        {"metric": "changed_documents", "value": changed},
        {"metric": "unchanged_documents", "value": unchanged},
        {"metric": "added_documents", "value": added},
        {"metric": "removed_documents", "value": removed},
        {"metric": "review_needed", "value": review_needed},
        {"metric": "title_revision_only", "value": title_only},
        {"metric": "schedule_bom_changed", "value": schedule_changed},
        {"metric": "plan_or_detail_changed", "value": plan_changed},
        {"metric": "ocr_used_documents", "value": ocr_used},
        {"metric": "exceptions", "value": exception_count},
    ]
    if runtime_seconds is not None:
        rows.append({"metric": "runtime_seconds", "value": round(runtime_seconds, 2)})
    return rows


def _document_rows_for_unmatched(
    decision: PairDecision,
    pair_id: str,
    packaged_path: str | None,
) -> dict[str, Any]:
    return {
        "pair_id": pair_id,
        "sheet_id": decision.sheet_id,
        "sheet_title": decision.after.sheet_title if decision.after else decision.before.sheet_title if decision.before else None,
        "family": decision.family,
        "status": decision.status,
        "compare_status": decision.status,
        "revision_status": decision.revision_status,
        "changed": decision.status in {"added", "removed"},
        "document_confidence": round(decision.confidence, 3),
        "ifb_path": str(decision.before.path) if decision.before else None,
        "gmp_path": str(decision.after.path) if decision.after else None,
        "ifb_name": decision.before.path.name if decision.before else None,
        "gmp_name": decision.after.path.name if decision.after else None,
        "ifb_revision": decision.before.revision if decision.before else None,
        "gmp_revision": decision.after.revision if decision.after else None,
        "ifb_pages": decision.before.page_count if decision.before else None,
        "gmp_pages": decision.after.page_count if decision.after else None,
        "changed_pages": 0,
        "schedule_row_diff_count": 0,
        "title_block_diff_count": 0,
        "visual_diff_count": 0,
        "structured_region_pages": 0,
        "highlighted_before_pdf": None,
        "highlighted_after_pdf": None,
        "packaged_path": packaged_path,
        "ocr_used": False,
        "warning_count": 0,
        "exception_count": 0,
    }


def _result_counts(result: Any) -> dict[str, int]:
    schedule_rows = 0
    title_regions = 0
    visual_boxes = 0
    for page in result.page_diffs:
        if page.change_type != "unchanged":
            visual_boxes += len(page.global_boxes)
        for region in page.region_diffs:
            if region.region_kind in {"metadata-like", "title-block-like"}:
                title_regions += 1
            for row_diff in region.row_diffs:
                if is_scope_row_diff(row_diff):
                    schedule_rows += 1
            if not region.row_diffs and is_scope_region_diff(region):
                schedule_rows += 1
    return {
        "schedule_row_diff_count": schedule_rows,
        "title_block_diff_count": title_regions,
        "visual_diff_count": visual_boxes,
    }


def _compare_status(result: Any, family: str) -> str:
    if getattr(result, "status", None) == "review_needed":
        return "review_needed"
    if not result.changed:
        return "unchanged"
    counts = _result_counts(result)
    if counts["schedule_row_diff_count"]:
        return "schedule_bom_changed"
    region_kinds = {
        region.region_kind
        for page in result.page_diffs
        for region in page.region_diffs
        if region.change_type != "unchanged"
    }
    visual_boxes = counts["visual_diff_count"]
    if region_kinds and region_kinds <= {"metadata-like", "title-block-like"} and visual_boxes <= 3:
        return "title_revision_only"
    if visual_boxes and family in {"overall_plan", "area_plan", "detail", "mdf_idf_layout"}:
        return "drawing_scope_changed"
    return "scope_changed"


def _rows_for_matched_result(
    decision: PairDecision,
    pair_id: str,
    result: Any,
    packaged_path: str | None,
) -> dict[str, Any]:
    sheet_title = decision.after.sheet_title if decision.after else decision.before.sheet_title if decision.before else None
    ocr_used = any(page.text_source_before == "ocr" or page.text_source_after == "ocr" for page in result.page_diffs)
    counts = _result_counts(result)
    document_row = {
        "pair_id": pair_id,
        "sheet_id": decision.sheet_id,
        "sheet_title": sheet_title,
        "family": decision.family,
        "status": result.status,
        "compare_status": _compare_status(result, decision.family),
        "revision_status": decision.revision_status,
        "changed": result.changed,
        "scope_changed": result.changed,
        "document_confidence": result.document_confidence,
        "ifb_path": str(decision.before.path) if decision.before else None,
        "gmp_path": str(decision.after.path) if decision.after else None,
        "ifb_name": decision.before.path.name if decision.before else None,
        "gmp_name": decision.after.path.name if decision.after else None,
        "ifb_revision": decision.before.revision if decision.before else None,
        "gmp_revision": decision.after.revision if decision.after else None,
        "ifb_pages": decision.before.page_count if decision.before else None,
        "gmp_pages": decision.after.page_count if decision.after else None,
        "changed_pages": sum(1 for page in result.page_diffs if page.change_type != "unchanged"),
        **counts,
        "structured_region_pages": sum(1 for page in result.page_diffs if page.structured_regions_detected),
        "highlighted_before_pdf": str(result.highlighted_before_pdf) if result.highlighted_before_pdf else None,
        "highlighted_after_pdf": str(result.highlighted_after_pdf) if result.highlighted_after_pdf else None,
        "packaged_path": packaged_path,
        "ocr_used": ocr_used,
        "warning_count": len(result.warnings),
        "exception_count": len(result.exceptions),
    }
    page_rows: list[dict[str, Any]] = []
    region_rows: list[dict[str, Any]] = []
    row_rows: list[dict[str, Any]] = []
    for page in result.page_diffs:
        page_rows.append(
            {
                "pair_id": pair_id,
                "sheet_id": decision.sheet_id,
                "sheet_title": sheet_title,
                "family": decision.family,
                "page_label": page.page_label,
                "before_page_index": page.page_before_index,
                "after_page_index": page.page_after_index,
                "change_type": page.change_type,
                "confidence": page.confidence,
                "global_box_count": len(page.global_boxes),
                "emphasized_box_count": len(page.emphasized_boxes),
                "structured_regions_detected": page.structured_regions_detected,
                "text_source_before": page.text_source_before,
                "text_source_after": page.text_source_after,
                "notes": page.notes,
                "highlighted_before_pdf": str(result.highlighted_before_pdf) if result.highlighted_before_pdf else None,
                "highlighted_after_pdf": str(result.highlighted_after_pdf) if result.highlighted_after_pdf else None,
            }
        )
        for region in page.region_diffs:
            region_rows.append(
                {
                    "pair_id": pair_id,
                    "sheet_id": decision.sheet_id,
                    "sheet_title": sheet_title,
                    "family": decision.family,
                    "page_label": page.page_label,
                    "region_kind": region.region_kind,
                    "change_type": region.change_type,
                    "confidence": region.confidence,
                    "before_bbox": region.before_bbox,
                    "after_bbox": region.after_bbox,
                    "before_text": region.before_text,
                    "after_text": region.after_text,
                    "emphasized": region.emphasized,
                    "before_region_id": region.region_id_before,
                    "after_region_id": region.region_id_after,
                    "highlighted_before_pdf": str(result.highlighted_before_pdf) if result.highlighted_before_pdf else None,
                    "highlighted_after_pdf": str(result.highlighted_after_pdf) if result.highlighted_after_pdf else None,
                }
            )
            for row_diff in region.row_diffs:
                is_scope_diff = is_scope_row_diff(row_diff)
                row_rows.append(
                    {
                        "pair_id": pair_id,
                        "sheet_id": decision.sheet_id,
                        "sheet_title": sheet_title,
                        "family": decision.family,
                        "page_index": row_diff.page_index,
                        "region_id": row_diff.region_id,
                        "region_kind": row_diff.region_kind,
                        "change_type": row_diff.change_type,
                        "row_key": row_diff.row_key,
                        "before_text": row_diff.before_text,
                        "after_text": row_diff.after_text,
                        "before_cells": row_diff.before_cells,
                        "after_cells": row_diff.after_cells,
                        "bbox": row_diff.bbox,
                        "before_bbox": row_diff.before_bbox,
                        "after_bbox": row_diff.after_bbox,
                        "before_changed_bboxes": row_diff.before_changed_bboxes,
                        "after_changed_bboxes": row_diff.after_changed_bboxes,
                        "confidence": row_diff.confidence,
                        "severity": row_diff.severity,
                        "is_scope_diff": is_scope_diff,
                        "highlighted_before_pdf": str(result.highlighted_before_pdf) if result.highlighted_before_pdf else None,
                        "highlighted_after_pdf": str(result.highlighted_after_pdf) if result.highlighted_after_pdf else None,
                    }
                )
    exceptions = [{"scope": "document-warning", "pair_id": pair_id, "message": warning} for warning in result.warnings]
    exceptions.extend({"scope": "document-exception", "pair_id": pair_id, "message": error} for error in result.exceptions)
    return {
        "document_row": document_row,
        "page_rows": page_rows,
        "region_rows": region_rows,
        "row_rows": row_rows,
        "exceptions": exceptions,
    }


def _compare_pair_worker(
    before_record: PdfRecord,
    after_record: PdfRecord,
    highlighted_before_path: Path,
    highlighted_after_path: Path,
) -> Any:
    return compare_pair(
        before_record,
        after_record,
        highlighted_before_path,
        highlighted_after_path,
        cancel_requested=_worker_cancel_requested,
    )


def _init_compare_worker(cancel_event: Any) -> None:
    global _WORKER_CANCEL_EVENT
    _WORKER_CANCEL_EVENT = cancel_event


def _worker_cancel_requested() -> bool:
    return bool(_WORKER_CANCEL_EVENT is not None and _WORKER_CANCEL_EVENT.is_set())


def run_compare_job(
    request_or_config: dict[str, Any] | CompareJobConfig,
    *,
    progress_callback: ProgressCallback = None,
    log_callback: LogCallback = None,
    cancel_requested: CancelCallback = None,
) -> dict[str, Any]:
    config = request_or_config if isinstance(request_or_config, CompareJobConfig) else CompareJobConfig.from_request(request_or_config)
    started_at = datetime.now(timezone.utc)
    started_monotonic = time.monotonic()
    dirs = _ensure_dirs(config)
    log_path = dirs["logs"] / "run.log"
    log = _make_logger(log_path, log_callback)
    cancelled = False
    cancel_message: str | None = None
    tracker = _WholeProgramProgressTracker()
    tracker.configure_scan(_count_input_pdfs(config.input_paths))
    progress_lock = threading.Lock()

    def emit_payload(payload: dict[str, Any]) -> None:
        with progress_lock:
            if payload.get("stage") == "pairing":
                tracker.configure_pairing(int(payload.get("total") or 1))
            percent = tracker.percent_for_payload(payload)
        message = _message_for_stage(payload)
        _emit_progress(progress_callback, percent, message, **payload, overall_percent=percent)

    def relay_progress(**payload: Any) -> None:
        emit_payload(dict(payload))

    def mark_decision_complete(decision_index: int | None) -> None:
        with progress_lock:
            tracker.mark_decision_complete(decision_index)

    log(f"Run {config.run_id} started in {config.mode} mode.")
    log(f"Inputs: {', '.join(str(path) for path in config.input_paths)}")
    _emit_progress(progress_callback, 0, "Starting compare job...", stage="starting", overall_percent=0)

    try:
        _check_cancel(cancel_requested)
        scan_result = scan_and_pair(
            config.input_paths,
            progress_callback=relay_progress,
            log_callback=log,
            cancel_requested=cancel_requested,
        )
        records = scan_result.records
        decisions = scan_result.decisions
        exceptions: list[dict[str, Any]] = [{"scope": "scan", "message": message} for message in scan_result.exceptions]
    except (JobCancelledError, PairingCancelledError) as exc:
        cancelled = True
        cancel_message = str(exc)
        log(cancel_message)
        records = []
        decisions = []
        exceptions = [{"scope": "cancelled", "message": cancel_message}]
    tracker.configure_decisions(decisions)

    log(f"Scanned {len(records)} PDFs.")
    log(
        "Pairing results: "
        f"{sum(1 for decision in decisions if decision.status == 'matched')} matched, "
        f"{sum(1 for decision in decisions if decision.status == 'review_needed')} review-needed, "
        f"{sum(1 for decision in decisions if decision.status == 'added')} added, "
        f"{sum(1 for decision in decisions if decision.status == 'removed')} removed."
    )
    relay_progress(stage="pairing-summary", current=1, total=1)

    pairing_rows: list[dict[str, Any]] = []
    document_rows: list[dict[str, Any]] = []
    page_rows: list[dict[str, Any]] = []
    region_rows: list[dict[str, Any]] = []
    row_rows: list[dict[str, Any]] = []

    decision_context: dict[int, tuple[PairDecision, str]] = {}
    matched_tasks: list[tuple[int, int, PairDecision, str]] = []
    unmatched_tasks: list[tuple[int, PairDecision, str]] = []
    matched_index = 0
    for decision_index, decision in enumerate(decisions):
        pair_id = _pair_id(decision, decision_index)
        decision_context[decision_index] = (decision, pair_id)
        pairing_rows.append(_decision_row(decision, pair_id))
        if decision.status == "matched" and decision.before and decision.after:
            matched_index += 1
            matched_tasks.append((decision_index, matched_index, decision, pair_id))
        else:
            unmatched_tasks.append((decision_index, decision, pair_id))

    total_matched = max(len(matched_tasks), 1)
    processed_by_index: dict[int, dict[str, Any]] = {}

    def rows_for_compare_exception(decision_index: int, decision: PairDecision, pair_id: str, exc: Exception) -> dict[str, Any]:
        message = f"{type(exc).__name__}: {exc}"
        review_dir = dirs["review_needed"] / pair_id
        review_dir.mkdir(parents=True, exist_ok=True)
        packaged_before = _copy_original(decision.before, review_dir, "IFB__")
        packaged_after = _copy_original(decision.after, review_dir, "GMP__")
        pair_dir = dirs["changed_pairs"] / pair_id
        if pair_dir.exists():
            shutil.rmtree(pair_dir, ignore_errors=True)
        return {
            "document_rows": [
                {
                    **_document_rows_for_unmatched(decision, pair_id, str(review_dir)),
                    "status": "review_needed",
                    "compare_status": "compare_failed",
                    "packaged_path": str(review_dir),
                    "highlighted_before_pdf": packaged_before,
                    "highlighted_after_pdf": packaged_after,
                    "exception_count": 1,
                }
            ],
            "page_rows": [],
            "region_rows": [],
            "row_rows": [],
            "exceptions": [{"scope": "compare-exception", "pair_id": pair_id, "sheet_id": decision.sheet_id, "message": message}],
        }

    def cleanup_pair_dir(pair_id: str) -> None:
        pair_dir = dirs["changed_pairs"] / pair_id
        if pair_dir.exists():
            shutil.rmtree(pair_dir, ignore_errors=True)

    def finalize_matched_result(decision_index: int, decision: PairDecision, pair_id: str, result: Any) -> dict[str, Any]:
        pair_dir = dirs["changed_pairs"] / pair_id
        before_highlight = pair_dir / "before_highlighted.pdf"
        after_highlight = pair_dir / "after_highlighted.pdf"
        extra_exceptions: list[dict[str, Any]] = []
        if result.status == "review_needed":
            review_dir = dirs["review_needed"] / pair_id
            review_dir.mkdir(parents=True, exist_ok=True)
            _copy_original(decision.before, review_dir, "IFB__")
            _copy_original(decision.after, review_dir, "GMP__")
            if before_highlight.exists():
                target = review_dir / before_highlight.name
                shutil.copy2(before_highlight, target)
                result.highlighted_before_pdf = target
                before_highlight.unlink()
            if after_highlight.exists():
                target = review_dir / after_highlight.name
                shutil.copy2(after_highlight, target)
                result.highlighted_after_pdf = target
                after_highlight.unlink()
            if pair_dir.exists() and not any(pair_dir.iterdir()):
                pair_dir.rmdir()
            packaged_path = str(review_dir)
            extra_exceptions.append(
                {
                    "scope": "compare-review-needed",
                    "pair_id": pair_id,
                    "sheet_id": decision.sheet_id,
                    "message": "Page analysis failed on at least one page; copied matched sheet to review_needed.",
                }
            )
        else:
            packaged_path = str(pair_dir) if result.changed else None

        if not result.changed:
            if before_highlight.exists():
                before_highlight.unlink()
            if after_highlight.exists():
                after_highlight.unlink()
            if pair_dir.exists() and not any(pair_dir.iterdir()):
                pair_dir.rmdir()

        rows = _rows_for_matched_result(decision, pair_id, result, packaged_path)
        rows["document_rows"] = [rows.pop("document_row")]
        rows["exceptions"] = [*extra_exceptions, *rows["exceptions"]]
        return rows

    if matched_tasks and not cancelled:
        worker_count = min(config.max_workers, len(matched_tasks))
        log(f"Comparing {len(matched_tasks)} matched pairs with {worker_count} worker process(es).")
        manager = Manager()
        cancel_event = manager.Event()
        executor = ProcessPoolExecutor(max_workers=worker_count, initializer=_init_compare_worker, initargs=(cancel_event,))
        future_map: dict[Future[Any], tuple[int, int, PairDecision, str]] = {}
        next_task = 0

        def submit_next_task() -> bool:
            nonlocal next_task
            if next_task >= len(matched_tasks):
                return False
            if cancel_requested is not None and cancel_requested():
                raise JobCancelledError("Compare job stopped by user.")
            task = matched_tasks[next_task]
            next_task += 1
            decision_index, pair_index, decision, pair_id = task
            current_pair_label = f"{decision.sheet_id or pair_id}: {decision.before.path.name} -> {decision.after.path.name}"
            emit_payload(
                {
                    "stage": "compare-queue",
                    "pair_index": pair_index,
                    "pair_total": total_matched,
                    "decision_index": decision_index + 1,
                    "decision_total": len(decisions),
                    "current_file": current_pair_label,
                    "sheet_id": decision.sheet_id,
                }
            )
            pair_dir = dirs["changed_pairs"] / pair_id
            pair_dir.mkdir(parents=True, exist_ok=True)
            before_highlight = pair_dir / "before_highlighted.pdf"
            after_highlight = pair_dir / "after_highlighted.pdf"
            log(f"Queued pair {decision.before.path.name} -> {decision.after.path.name}")
            future_map[executor.submit(_compare_pair_worker, decision.before, decision.after, before_highlight, after_highlight)] = task
            return True

        try:
            for _ in range(worker_count):
                if not submit_next_task():
                    break
            pending = set(future_map)
            while pending:
                if cancel_requested is not None and cancel_requested():
                    cancel_event.set()
                    raise JobCancelledError("Compare job stopped by user.")
                done, pending = wait(pending, timeout=0.75, return_when=FIRST_COMPLETED)
                if not done:
                    emit_payload(
                        {
                            "stage": "parallel-active",
                            "active_workers": min(worker_count, len(pending)),
                            "pending_pairs": len(pending),
                            "current_file": f"{len(pending)} matched pair(s) still running",
                        }
                    )
                    continue
                for future in done:
                    decision_index, pair_index, decision, pair_id = future_map[future]
                    try:
                        _check_cancel(cancel_requested)
                        result = future.result()
                        result.decision = decision
                        processed_by_index[decision_index] = finalize_matched_result(decision_index, decision, pair_id, result)
                        relay_progress(
                            stage="pair-complete",
                            current=1,
                            total=1,
                            current_file=f"{decision.sheet_id or pair_id}: {decision.before.path.name} -> {decision.after.path.name}",
                            sheet_id=decision.sheet_id,
                            pair_index=pair_index,
                            pair_total=total_matched,
                            decision_index=decision_index + 1,
                            decision_total=len(decisions),
                        )
                        mark_decision_complete(decision_index + 1)
                        if submit_next_task():
                            pending.add(next(reversed(future_map)))
                    except (JobCancelledError, PairingCancelledError, AnalysisCancelledError, CompareCancelledError) as exc:
                        cancelled = True
                        cancel_message = str(exc)
                        cancel_event.set()
                        exceptions.append({"scope": "cancelled", "pair_id": pair_id, "message": cancel_message})
                        log(f"Cancelled while processing {pair_id}: {cancel_message}")
                        for pending_future in pending:
                            pending_future.cancel()
                        pending.clear()
                        break
                    except Exception as exc:  # noqa: BLE001
                        message = f"{type(exc).__name__}: {exc}"
                        log(f"Compare failed for {pair_id}; continuing: {message}")
                        processed_by_index[decision_index] = rows_for_compare_exception(decision_index, decision, pair_id, exc)
                        mark_decision_complete(decision_index + 1)
        except (JobCancelledError, PairingCancelledError) as exc:
            cancelled = True
            cancel_message = str(exc)
            cancel_event.set()
            exceptions.append({"scope": "cancelled", "message": cancel_message})
            log(cancel_message)
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
            manager.shutdown()
            if cancelled:
                for decision_index, _pair_index, _decision, pair_id in matched_tasks:
                    if decision_index not in processed_by_index:
                        cleanup_pair_dir(pair_id)

    if not cancelled:
        for decision_index, decision, pair_id in unmatched_tasks:
            current_name = decision.after.path.name if decision.after else decision.before.path.name if decision.before else pair_id
            relay_progress(
                stage="processing-unmatched-start",
                current=1,
                total=1,
                current_file=current_name,
                document_status=decision.status,
                decision_index=decision_index + 1,
                decision_total=len(decisions),
            )
            if decision.status == "added":
                packaged = _copy_original(decision.after, dirs["added"])
            elif decision.status == "removed":
                packaged = _copy_original(decision.before, dirs["removed"])
            else:
                review_dir = dirs["review_needed"] / pair_id
                review_dir.mkdir(parents=True, exist_ok=True)
                first = _copy_original(decision.before, review_dir, "before__")
                second = _copy_original(decision.after, review_dir, "after__")
                packaged = second or first or str(review_dir)
                if decision.status == "review_needed":
                    log(f"Pair sent to review_needed: {pair_id}")
            processed_by_index[decision_index] = {
                "document_rows": [_document_rows_for_unmatched(decision, pair_id, packaged)],
                "page_rows": [],
                "region_rows": [],
                "row_rows": [],
                "exceptions": [],
            }
            relay_progress(
                stage="processing-unmatched-complete",
                current=1,
                total=1,
                current_file=current_name,
                document_status=decision.status,
                decision_index=decision_index + 1,
                decision_total=len(decisions),
            )
            mark_decision_complete(decision_index + 1)

    for decision_index in range(len(decisions)):
        rows = processed_by_index.get(decision_index)
        if not rows:
            continue
        document_rows.extend(rows["document_rows"])
        page_rows.extend(rows["page_rows"])
        region_rows.extend(rows["region_rows"])
        row_rows.extend(rows["row_rows"])
        exceptions.extend(rows["exceptions"])

    relay_progress(stage="writing-report", current=1, total=1)
    manifest_rows = [_record_manifest_row(record) for record in records]
    runtime_seconds = time.monotonic() - started_monotonic
    job_manifest_path = dirs["output_dir"] / "job_manifest.json"
    expected_report_path = dirs["output_dir"] / "report.xlsx"
    expected_manifest_path = dirs["output_dir"] / "manifest.json"
    expected_manifest_csv_path = dirs["output_dir"] / "manifest.csv"
    expected_changed_pairs_zip_path = dirs["output_dir"] / "changed_pairs.zip"
    run_status = "cancelled" if cancelled else "complete"
    bluebeam_bundle = _build_bluebeam_review_package(dirs["output_dir"], document_rows, log)
    summary_rows = [
        {"metric": "run_status", "value": run_status},
        {"metric": "is_partial", "value": cancelled},
        {"metric": "output_dir", "value": str(dirs["output_dir"])},
        {"metric": "report_path", "value": str(expected_report_path)},
        {"metric": "bluebeam_review_pdf", "value": bluebeam_bundle["bluebeam_review_pdf"]},
        {"metric": "changed_pairs_zip_path", "value": str(expected_changed_pairs_zip_path)},
        *_summary_rows(document_rows, decisions, len(exceptions), records, runtime_seconds),
    ]

    compare_results = {
        "summary": summary_rows,
        "pairing_index": pairing_rows,
        "document_summary": document_rows,
        "page_diffs": page_rows,
        "region_diffs": region_rows,
        "structured_row_diffs": row_rows,
        "exceptions": exceptions,
    }

    report_bundle = write_report_bundle(
        dirs["output_dir"],
        compare_results=compare_results,
        manifests=manifest_rows,
        exceptions=exceptions,
    )

    manifest_payload = {
        "run_id": config.run_id,
        "mode": config.mode,
        "input_paths": [str(path) for path in config.input_paths],
        "output_dir": str(dirs["output_dir"]),
        "started_at": started_at.replace(microsecond=0).isoformat(),
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "run_status": run_status,
        "is_partial": cancelled,
        "report_path": report_bundle["workbook_path"],
        "manifest_path": report_bundle["manifest_json_path"],
        "manifest_csv_path": report_bundle["manifest_csv_path"],
        "log_path": str(log_path),
        "artifacts": {
            "changed_pairs_dir": str(dirs["changed_pairs"]),
            "added_dir": str(dirs["added"]),
            "removed_dir": str(dirs["removed"]),
            "review_needed_dir": str(dirs["review_needed"]),
            "logs_dir": str(dirs["logs"]),
            "changed_pairs_zip_path": str(expected_changed_pairs_zip_path),
            **bluebeam_bundle,
        },
        "counts": {
            "scanned": len(records),
            "ifb": sum(1 for record in records if record.source_group == "IFB"),
            "gmp": sum(1 for record in records if record.source_group == "GMP"),
            "decisions": len(decisions),
            "matched": sum(1 for decision in decisions if decision.status == "matched"),
            "added": sum(1 for decision in decisions if decision.status == "added"),
            "removed": sum(1 for decision in decisions if decision.status == "removed"),
            "changed_documents": sum(1 for row in document_rows if row.get("status") == "changed"),
            "unchanged_documents": sum(1 for row in document_rows if row.get("status") == "unchanged"),
            "review_needed": (
                sum(1 for decision in decisions if decision.status == "review_needed")
                + sum(1 for row in document_rows if row.get("status") == "review_needed")
            ),
            "bluebeam_review_sheets": bluebeam_bundle["bluebeam_review_sheet_count"],
            "bluebeam_review_pages": bluebeam_bundle["bluebeam_review_page_count"],
        },
    }
    job_manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    changed_pairs_zip_path = _zip_changed_pairs(dirs["output_dir"], log)

    completion_stage = "cancelled" if cancelled else "complete"
    completion_percent = tracker.percent_for_payload({"stage": completion_stage})
    _emit_progress(
        progress_callback,
        completion_percent,
        "Stopped" if cancelled else "Complete",
        stage=completion_stage,
        output_dir=str(dirs["output_dir"]),
        overall_percent=completion_percent,
    )
    log(f"Report written to {report_bundle['workbook_path']}")
    log(f"Bluebeam review PDF written to {bluebeam_bundle['bluebeam_review_pdf']}")
    log(f"Downloadable run package ready at {dirs['output_dir']}")
    if cancelled:
        log("Run was stopped before the full compare queue completed.")

    public_keep = {"bluebeam_review.pdf", "report.xlsx", "changed_pairs.zip"}
    _prune_public_output(dirs["output_dir"], public_keep)

    response = {
        "output_dir": str(dirs["output_dir"]),
        "report_path": report_bundle["workbook_path"],
        "manifest_path": None,
        "manifest_csv_path": None,
        "job_manifest_path": None,
        "log_path": None,
        "changed_pairs_zip_path": str(changed_pairs_zip_path),
        "bluebeam_review_pdf": bluebeam_bundle["bluebeam_review_pdf"],
        "summary": summary_rows,
        "pairing_index": pairing_rows,
        "document_summary": document_rows,
        "page_diffs": page_rows,
        "region_diffs": region_rows,
        "structured_row_diffs": row_rows,
        "exceptions": exceptions,
        "manifests": manifest_rows,
        "bluebeam_review": bluebeam_bundle,
    }
    if cancelled:
        raise JobCancelledError(cancel_message or "Compare job stopped by user.", result=response)
    return response
