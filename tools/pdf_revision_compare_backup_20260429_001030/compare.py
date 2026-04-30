from __future__ import annotations

import io
import math
import re
import statistics
from typing import Callable
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

import fitz
import numpy
from PIL import Image, ImageChops, ImageDraw

try:  # OpenCV is a runtime dependency, but keep imports/testable without it.
    from cv2 import CHAIN_APPROX_SIMPLE, RETR_EXTERNAL, THRESH_BINARY, boundingRect, findContours, threshold
except Exception:  # noqa: BLE001
    CHAIN_APPROX_SIMPLE = RETR_EXTERNAL = THRESH_BINARY = None
    boundingRect = findContours = threshold = None

from .analysis import analyze_document, bbox_overlap_ratio, normalize_text
from .models import BBox, DocumentResult, PageDiff, Region, RegionDiff, RowDiff
from .pairing import PdfRecord, average_hash, hamming_similarity


@dataclass(slots=True)
class HighlightAnnotation:
    bbox: BBox
    outline: tuple[int, int, int, int]
    style: str
    label: str = ""
    change_type: str = "modified"  # "added", "removed", or "modified"


ProgressCallback = Callable[..., None] | None
CancelCallback = Callable[[], bool] | None


class CompareCancelledError(RuntimeError):
    pass


MAX_VISUAL_RENDER_PIXELS = 2_750_000
MAX_VISUAL_BOXES_PER_PAGE = 72
MAX_HIGHLIGHT_MARKERS_PER_PAGE = 120
DENSE_VISUAL_DIFF_RATIO = 0.18
CELL_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")
DRAWING_INDEX_RE = re.compile(r"^\s*DT\d{4}\b", re.IGNORECASE)
MODEL_TOKEN_RE = re.compile(r"\b(?=[a-z0-9_.-]*[a-z])(?=[a-z0-9_.-]*\d)[a-z0-9][a-z0-9_.-]{2,}\b", re.IGNORECASE)
DATE_TOKEN_RE = re.compile(r"\b(?:\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}|\d{4}[/.-]\d{1,2}[/.-]\d{1,2})\b")
REVISION_ONLY_RE = re.compile(r"^\s*(?:rev(?:ision)?\.?\s*)?[a-z0-9]{1,3}\s*$", re.IGNORECASE)
REVISION_LOG_RE = re.compile(r"\b(?:revised|revision|issued?|gmp issue|for bid|bid issue|addendum)\b", re.IGNORECASE)

SCOPE_REGION_KINDS = {"bom-like", "table-like"}
IGNORED_REGION_KINDS = {"metadata-like", "title-block-like"}
SCOPE_CONTEXT_TERMS = {
    "access",
    "ap",
    "badge",
    "bom",
    "cable",
    "camera",
    "card",
    "cat6",
    "circuit",
    "control",
    "controller",
    "device",
    "door",
    "equipment",
    "fiber",
    "function",
    "idf",
    "intercom",
    "location",
    "make",
    "manufacturer",
    "mdf",
    "model",
    "mount",
    "mounting",
    "network",
    "panel",
    "part",
    "patch",
    "port",
    "qty",
    "quantity",
    "reader",
    "router",
    "schedule",
    "server",
    "switch",
    "tag",
    "type",
    "wap",
    "wireless",
}
NON_SCOPE_TERMS = {
    "approved",
    "checked",
    "client",
    "consultant",
    "copyright",
    "date",
    "drawn",
    "drawing",
    "engineer",
    "issue",
    "issued",
    "legal",
    "owner",
    "professional",
    "project",
    "revision",
    "revisions",
    "rev",
    "scale",
    "seal",
    "sheet",
    "signature",
    "title",
}
LEGEND_REFERENCE_TERMS = {
    "abbreviation",
    "abbreviations",
    "autodesk docs",
    "construction note",
    "designator",
    "device legend",
    "drawing index",
    "equipment number",
    "legend",
    "note #",
    "refer to drawing",
    "sequential",
    "sheet index",
    "sheet type",
    "symbols",
}
HEADER_ONLY_TERMS = {
    "item",
    "qty",
    "quantity",
    "description",
    "remarks",
    "comment",
    "material",
    "part",
    "schedule",
    "bom",
    "bill",
    "length",
    "width",
    "height",
    "port",
    "tag",
    "device",
    "model",
    "manufacturer",
    "type",
    "make",
    "mounting",
    "function",
}


def _check_cancel(cancel_requested: CancelCallback) -> None:
    if cancel_requested is not None and cancel_requested():
        raise CompareCancelledError("Compare job stopped by user.")


def _page_fingerprint(page: fitz.Page) -> str:
    return average_hash(page, dpi=24)


def _page_preview(page: fitz.Page) -> str:
    return normalize_text(page.get_text("text")[:1500])


def align_pages(
    before_doc: fitz.Document,
    after_doc: fitz.Document,
    cancel_requested: CancelCallback = None,
) -> list[tuple[int | None, int | None]]:
    before_hashes: list[str] = []
    before_previews: list[str] = []
    for page in before_doc:
        _check_cancel(cancel_requested)
        before_hashes.append(_page_fingerprint(page))
        before_previews.append(_page_preview(page))
    after_hashes: list[str] = []
    after_previews: list[str] = []
    for page in after_doc:
        _check_cancel(cancel_requested)
        after_hashes.append(_page_fingerprint(page))
        after_previews.append(_page_preview(page))

    candidates: list[tuple[float, int, int]] = []
    max_pages = max(before_doc.page_count, after_doc.page_count)
    for before_index, before_hash in enumerate(before_hashes):
        _check_cancel(cancel_requested)
        for after_index, after_hash in enumerate(after_hashes):
            similarity = hamming_similarity(before_hash, after_hash)
            text_similarity = SequenceMatcher(None, before_previews[before_index], after_previews[after_index]).ratio()
            before_rect = before_doc[before_index].rect
            after_rect = after_doc[after_index].rect
            size_delta = min(
                abs(before_rect.width - after_rect.width) + abs(before_rect.height - after_rect.height),
                abs(before_rect.width - after_rect.height) + abs(before_rect.height - after_rect.width),
            )
            size_score = max(0.0, 1.0 - (size_delta / max(before_rect.width + before_rect.height + after_rect.width + after_rect.height, 1.0)))
            index_penalty = abs(before_index - after_index) / max(max_pages, 1)
            score = (similarity * 0.55) + (text_similarity * 0.35) + (size_score * 0.10) - (index_penalty * 0.03)
            if score >= 0.45:
                candidates.append((score, before_index, after_index))
    candidates.sort(reverse=True)

    used_before: set[int] = set()
    used_after: set[int] = set()
    matches: list[tuple[int | None, int | None]] = []
    for _score, before_index, after_index in candidates:
        if before_index in used_before or after_index in used_after:
            continue
        used_before.add(before_index)
        used_after.add(after_index)
        matches.append((before_index, after_index))

    for before_index in range(before_doc.page_count):
        if before_index not in used_before:
            matches.append((before_index, None))
    for after_index in range(after_doc.page_count):
        if after_index not in used_after:
            matches.append((None, after_index))

    def sort_key(item: tuple[int | None, int | None]) -> tuple[int, int]:
        left = item[0] if item[0] is not None else 10_000 + (item[1] or 0)
        right = item[1] if item[1] is not None else 10_000 + (item[0] or 0)
        return (left, right)

    return sorted(matches, key=sort_key)


def _relative_bbox(bbox: BBox, width: float, height: float) -> tuple[float, float, float, float]:
    return (
        bbox[0] / max(width, 1.0),
        bbox[1] / max(height, 1.0),
        bbox[2] / max(width, 1.0),
        bbox[3] / max(height, 1.0),
    )


def _match_region_score(
    before_region: Region,
    after_region: Region,
    before_size: tuple[float, float],
    after_size: tuple[float, float],
) -> float:
    if before_region.kind != after_region.kind:
        return 0.0
    before_rel = _relative_bbox(before_region.bbox, *before_size)
    after_rel = _relative_bbox(after_region.bbox, *after_size)
    overlap = bbox_overlap_ratio(before_rel, after_rel)
    text_similarity = SequenceMatcher(None, before_region.normalized_text, after_region.normalized_text).ratio()
    row_similarity = 0.0
    if before_region.rows or after_region.rows:
        row_similarity = 1.0 - (
            abs(len(before_region.rows) - len(after_region.rows)) / max(len(before_region.rows), len(after_region.rows), 1)
        )
    return (overlap * 0.45) + (text_similarity * 0.40) + (row_similarity * 0.15)


def _normalized_cell(value: str) -> str:
    return CELL_NORMALIZE_RE.sub("", value.lower())


def _word_tokens(value: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", value.lower()))


def _model_like_tokens(value: str) -> list[str]:
    tokens = []
    for match in MODEL_TOKEN_RE.finditer(value):
        token = match.group(0).strip("._-").lower()
        if len(token) < 4:
            continue
        if re.fullmatch(r"(?:dt|ee|et|el|e)\d{3,5}", token):
            continue
        if DATE_TOKEN_RE.fullmatch(token):
            continue
        tokens.append(token)
    return tokens


def _has_scope_signal(value: str) -> bool:
    normalized = normalize_text(value)
    tokens = _word_tokens(normalized)
    if tokens & SCOPE_CONTEXT_TERMS:
        return True
    if re.search(r"\b(?:idf|mdf|sw|switch|port|cam|camera|wap|ap|reader|fiber)[-_ ]?[a-z0-9]*\d+[a-z0-9_.-]*\b", normalized):
        return True
    return False


def _has_specific_scope_signal(value: str) -> bool:
    normalized = normalize_text(value)
    tokens = _word_tokens(normalized)
    if re.search(r"\b(?:cam|camera|idf|mdf|sw|switch|port|wap|ap|reader|fiber)[-_ ]?[a-z0-9]*\d+[a-z0-9_.-]*\b", normalized):
        return True
    if (tokens & {"model", "make", "manufacturer", "part", "device", "switch", "port", "equipment"} or _has_scope_signal(normalized)) and _model_like_tokens(normalized):
        return True
    return False


def _looks_like_header_only(value: str) -> bool:
    tokens = _word_tokens(value)
    if len(tokens) < 2:
        return False
    if "schedule" in tokens and len(tokens) <= 3 and not _model_like_tokens(value):
        return True
    return len(tokens & HEADER_ONLY_TERMS) >= 2 and not any(token.isdigit() for token in tokens)


def _is_revision_date_or_metadata_noise(value: str) -> bool:
    normalized = normalize_text(value)
    if not normalized:
        return True
    if DRAWING_INDEX_RE.match(normalized):
        return True
    if DATE_TOKEN_RE.search(normalized):
        stripped = DATE_TOKEN_RE.sub(" ", normalized)
        if not _has_scope_signal(stripped):
            return True
    if REVISION_ONLY_RE.fullmatch(normalized):
        return True
    tokens = _word_tokens(normalized)
    if not tokens:
        return True
    if tokens <= NON_SCOPE_TERMS:
        return True
    if len(tokens & NON_SCOPE_TERMS) >= 2 and not _has_scope_signal(normalized):
        return True
    if re.search(r"\bdt\d{4}\b", normalized) and not _has_scope_signal(normalized):
        return True
    return False


def _looks_like_sheet_index_or_title_row(value: str) -> bool:
    normalized = normalize_text(value)
    if not normalized:
        return False
    if DRAWING_INDEX_RE.match(normalized):
        return True
    return "sheet index" in normalized or "project information" in normalized


def _looks_like_revision_log_row(value: str) -> bool:
    normalized = normalize_text(value)
    if not normalized:
        return False
    if not REVISION_LOG_RE.search(normalized):
        return False
    return bool(DATE_TOKEN_RE.search(normalized) or re.search(r"\brev(?:ision)?\b", normalized))


def _looks_like_legend_or_reference_text(value: str) -> bool:
    normalized = normalize_text(value)
    return any(term in normalized for term in LEGEND_REFERENCE_TERMS)


def _cell_change_is_significant(before_value: str, after_value: str) -> bool:
    return _normalized_cell(before_value) != _normalized_cell(after_value)


def _changed_cell_values(before_cells: list[str], after_cells: list[str]) -> list[tuple[str, str]]:
    if len(before_cells) == len(after_cells):
        return [
            (before_cell, after_cell)
            for before_cell, after_cell in zip(before_cells, after_cells)
            if _cell_change_is_significant(before_cell, after_cell)
        ]
    matcher = SequenceMatcher(None, [_normalized_cell(cell) for cell in before_cells], [_normalized_cell(cell) for cell in after_cells])
    changed: list[tuple[str, str]] = []
    for opcode, i1, i2, j1, j2 in matcher.get_opcodes():
        if opcode == "equal":
            continue
        before_text = " ".join(before_cells[i1:i2])
        after_text = " ".join(after_cells[j1:j2])
        changed.append((before_text, after_text))
    return changed


def _value_change_is_scope_worthy(before_value: str, after_value: str, row_context: str) -> bool:
    if not _cell_change_is_significant(before_value, after_value):
        return False
    combined_change = f"{before_value} {after_value}"
    if _is_revision_date_or_metadata_noise(combined_change):
        return False
    if _has_specific_scope_signal(combined_change):
        return True
    if _has_specific_scope_signal(row_context) and not _is_revision_date_or_metadata_noise(combined_change):
        return True
    return False


def is_scope_row_diff(row_diff: RowDiff) -> bool:
    if row_diff.region_kind not in SCOPE_REGION_KINDS:
        return False
    before_text = row_diff.before_text or ""
    after_text = row_diff.after_text or ""
    row_context = f"{before_text} {after_text}"
    if _looks_like_sheet_index_or_title_row(before_text) or _looks_like_sheet_index_or_title_row(after_text):
        return False
    if _looks_like_revision_log_row(row_context):
        return False
    if _looks_like_legend_or_reference_text(row_context):
        return False
    if _looks_like_header_only(row_context):
        return False
    if _is_revision_date_or_metadata_noise(row_context) and not _has_scope_signal(row_context):
        return False
    if row_diff.change_type == "modified":
        changed_values = _changed_cell_values(row_diff.before_cells, row_diff.after_cells)
        if not changed_values:
            changed_values = [(before_text, after_text)]
        return any(_value_change_is_scope_worthy(before_value, after_value, row_context) for before_value, after_value in changed_values)
    changed_text = after_text if row_diff.change_type == "added" else before_text
    return _has_specific_scope_signal(changed_text) and not _looks_like_header_only(changed_text) and not _is_revision_date_or_metadata_noise(changed_text)


def _changed_cell_bboxes(before_row, after_row) -> tuple[list[BBox], list[BBox]]:
    before_cells = list(before_row.cells)
    after_cells = list(after_row.cells)
    before_boxes = list(getattr(before_row, "cell_bboxes", []) or [])
    after_boxes = list(getattr(after_row, "cell_bboxes", []) or [])
    if not before_cells and not after_cells:
        return [before_row.bbox], [after_row.bbox]

    changed_before: list[BBox] = []
    changed_after: list[BBox] = []
    if len(before_cells) == len(after_cells):
        for index, (before_cell, after_cell) in enumerate(zip(before_cells, after_cells)):
            if not _cell_change_is_significant(before_cell, after_cell):
                continue
            if index < len(before_boxes):
                changed_before.append(before_boxes[index])
            if index < len(after_boxes):
                changed_after.append(after_boxes[index])
    else:
        matcher = SequenceMatcher(None, [_normalized_cell(cell) for cell in before_cells], [_normalized_cell(cell) for cell in after_cells])
        for opcode, i1, i2, j1, j2 in matcher.get_opcodes():
            if opcode == "equal":
                continue
            changed_before.extend(before_boxes[index] for index in range(i1, min(i2, len(before_boxes))))
            changed_after.extend(after_boxes[index] for index in range(j1, min(j2, len(after_boxes))))

    if not changed_before and before_row.normalized_text != after_row.normalized_text:
        changed_before = [before_row.bbox]
    if not changed_after and before_row.normalized_text != after_row.normalized_text:
        changed_after = [after_row.bbox]
    return changed_before, changed_after


def _row_anchor(row) -> str | None:
    cells = [cell for cell in getattr(row, "cells", []) if _normalized_cell(cell)]
    if not cells:
        return None
    first = _normalized_cell(cells[0])
    if first:
        return first
    for cell in cells[:4]:
        normalized = _normalized_cell(cell)
        if len(normalized) >= 3 and any(char.isalpha() for char in normalized) and any(char.isdigit() for char in normalized):
            return normalized
    return None


def _removed_row_diff(row, page_index: int, region_id: str, region_kind: str, confidence: float = 0.88) -> RowDiff:
    return RowDiff(
        page_index=page_index,
        region_id=region_id,
        region_kind=region_kind,
        change_type="removed",
        row_key=row.key,
        before_text=row.text,
        after_text="",
        before_cells=row.cells,
        bbox=row.bbox,
        before_bbox=row.bbox,
        before_changed_bboxes=[row.bbox],
        confidence=confidence,
        severity="high" if region_kind in {"table-like", "bom-like"} else "medium",
    )


def _added_row_diff(row, page_index: int, region_id: str, region_kind: str, confidence: float = 0.88) -> RowDiff:
    return RowDiff(
        page_index=page_index,
        region_id=region_id,
        region_kind=region_kind,
        change_type="added",
        row_key=row.key,
        before_text="",
        after_text=row.text,
        after_cells=row.cells,
        bbox=row.bbox,
        after_bbox=row.bbox,
        after_changed_bboxes=[row.bbox],
        confidence=confidence,
        severity="high" if region_kind in {"table-like", "bom-like"} else "medium",
    )


def _row_diff_from_replace(
    before_rows: list,
    after_rows: list,
    page_index: int,
    region_id: str,
    region_kind: str,
) -> list[RowDiff]:
    row_diffs: list[RowDiff] = []
    overlap_count = min(len(before_rows), len(after_rows))
    for index in range(overlap_count):
        before_row = before_rows[index]
        after_row = after_rows[index]
        if before_row.normalized_text == after_row.normalized_text:
            continue
        before_changed_bboxes, after_changed_bboxes = _changed_cell_bboxes(before_row, after_row)
        row_diffs.append(
            RowDiff(
                page_index=page_index,
                region_id=region_id,
                region_kind=region_kind,
                change_type="modified",
                row_key=f"{before_row.key}:{after_row.key}",
                before_text=before_row.text,
                after_text=after_row.text,
                before_cells=before_row.cells,
                after_cells=after_row.cells,
                bbox=before_row.bbox,
                before_bbox=before_row.bbox,
                after_bbox=after_row.bbox,
                before_changed_bboxes=before_changed_bboxes,
                after_changed_bboxes=after_changed_bboxes,
                confidence=0.82,
                severity="high" if region_kind in {"table-like", "bom-like"} else "medium",
            )
        )
    for before_row in before_rows[overlap_count:]:
        row_diffs.append(_removed_row_diff(before_row, page_index, region_id, region_kind, confidence=0.85))
    for after_row in after_rows[overlap_count:]:
        row_diffs.append(_added_row_diff(after_row, page_index, region_id, region_kind, confidence=0.85))
    return row_diffs


def compare_rows(before_region: Region, after_region: Region, page_index: int) -> list[RowDiff]:
    before_rows = before_region.rows
    after_rows = after_region.rows
    if not before_rows and not after_rows:
        return []
    if len(before_rows) == len(after_rows):
        before_joined = [row.normalized_text for row in before_rows]
        after_joined = [row.normalized_text for row in after_rows]
        if before_joined == after_joined:
            return []

    before_anchor_map: dict[str, object] = {}
    after_anchor_map: dict[str, object] = {}
    duplicate_anchors: set[str] = set()
    for row in before_rows:
        anchor = _row_anchor(row)
        if not anchor:
            continue
        if anchor in before_anchor_map:
            duplicate_anchors.add(anchor)
        before_anchor_map[anchor] = row
    for row in after_rows:
        anchor = _row_anchor(row)
        if not anchor:
            continue
        if anchor in after_anchor_map:
            duplicate_anchors.add(anchor)
        after_anchor_map[anchor] = row
    common_anchors = (set(before_anchor_map) & set(after_anchor_map)) - duplicate_anchors
    if common_anchors and (len(common_anchors) >= 2 or min(len(before_rows), len(after_rows)) <= 3):
        row_diffs: list[RowDiff] = []
        used_before = set()
        used_after = set()
        for before_row in before_rows:
            anchor = _row_anchor(before_row)
            if anchor not in common_anchors:
                continue
            after_row = after_anchor_map[anchor]
            used_before.add(before_row.row_index)
            used_after.add(after_row.row_index)
            if before_row.normalized_text != after_row.normalized_text:
                row_diffs.extend(
                    _row_diff_from_replace(
                        [before_row],
                        [after_row],
                        page_index,
                        before_region.region_id,
                        before_region.kind,
                    )
                )
        for before_row in before_rows:
            if before_row.row_index not in used_before:
                row_diffs.append(_removed_row_diff(before_row, page_index, before_region.region_id, before_region.kind))
        for after_row in after_rows:
            if after_row.row_index not in used_after:
                row_diffs.append(_added_row_diff(after_row, page_index, after_region.region_id, after_region.kind))
        return row_diffs

    before_lines = [row.normalized_text for row in before_rows]
    after_lines = [row.normalized_text for row in after_rows]
    matcher = SequenceMatcher(None, before_lines, after_lines)
    row_diffs: list[RowDiff] = []

    for opcode, i1, i2, j1, j2 in matcher.get_opcodes():
        if opcode == "equal":
            continue
        if opcode == "replace":
            row_diffs.extend(
                _row_diff_from_replace(
                    before_rows[i1:i2],
                    after_rows[j1:j2],
                    page_index,
                    before_region.region_id,
                    before_region.kind,
                )
            )
            continue
        if opcode == "delete":
            for before_row in before_rows[i1:i2]:
                row_diffs.append(
                    RowDiff(
                        page_index=page_index,
                        region_id=before_region.region_id,
                        region_kind=before_region.kind,
                        change_type="removed",
                        row_key=before_row.key,
                        before_text=before_row.text,
                        after_text="",
                        before_cells=before_row.cells,
                        bbox=before_row.bbox,
                        before_bbox=before_row.bbox,
                        before_changed_bboxes=[before_row.bbox],
                        confidence=0.88,
                        severity="high" if before_region.kind in {"table-like", "bom-like"} else "medium",
                    )
                )
            continue
        if opcode == "insert":
            for after_row in after_rows[j1:j2]:
                row_diffs.append(
                    RowDiff(
                        page_index=page_index,
                        region_id=after_region.region_id,
                        region_kind=after_region.kind,
                        change_type="added",
                        row_key=after_row.key,
                        before_text="",
                        after_text=after_row.text,
                        after_cells=after_row.cells,
                        bbox=after_row.bbox,
                        after_bbox=after_row.bbox,
                        after_changed_bboxes=[after_row.bbox],
                        confidence=0.88,
                        severity="high" if after_region.kind in {"table-like", "bom-like"} else "medium",
                    )
                )
    return row_diffs


def _bbox_area(bbox: BBox) -> float:
    return max(0.0, bbox[2] - bbox[0]) * max(0.0, bbox[3] - bbox[1])


def _bbox_area_ratio(bbox: BBox, page_size: tuple[float, float] | None) -> float:
    if page_size is None:
        return 0.0
    return _bbox_area(bbox) / max(page_size[0] * page_size[1], 1.0)


def _region_text_scope_worthy(region_diff: RegionDiff, page_size: tuple[float, float] | None = None) -> bool:
    if region_diff.region_kind in IGNORED_REGION_KINDS:
        return False
    if region_diff.region_kind == "general":
        return False
    bbox = region_diff.before_bbox or region_diff.after_bbox
    if region_diff.region_kind == "general" and bbox is not None and _bbox_area_ratio(bbox, page_size) > 0.035:
        return False
    combined = f"{region_diff.before_text} {region_diff.after_text}"
    if _looks_like_legend_or_reference_text(combined):
        return False
    if _looks_like_header_only(combined):
        return False
    if _is_revision_date_or_metadata_noise(combined) and not _has_scope_signal(combined):
        return False
    return _has_specific_scope_signal(combined)


def _structured_row_has_scope_content(row) -> bool:
    text = getattr(row, "text", "") or ""
    if _looks_like_sheet_index_or_title_row(text) or _looks_like_header_only(text):
        return False
    if _looks_like_legend_or_reference_text(text):
        return False
    return _has_specific_scope_signal(text) and not _is_revision_date_or_metadata_noise(text)


def _region_has_scope_context(region: Region) -> bool:
    if region.kind in IGNORED_REGION_KINDS:
        return False
    if region.rows:
        return any(_structured_row_has_scope_content(row) for row in region.rows)
    text = region.text or ""
    if _looks_like_sheet_index_or_title_row(text) or _looks_like_header_only(text):
        return False
    if _looks_like_legend_or_reference_text(text):
        return False
    return _has_specific_scope_signal(text) and not _is_revision_date_or_metadata_noise(text)


def is_scope_region_diff(region_diff: RegionDiff, page_size: tuple[float, float] | None = None) -> bool:
    if region_diff.change_type == "unchanged":
        return False
    if region_diff.region_kind in IGNORED_REGION_KINDS:
        return False
    if region_diff.row_diffs:
        return any(is_scope_row_diff(row_diff) for row_diff in region_diff.row_diffs)
    if region_diff.region_kind in SCOPE_REGION_KINDS:
        return False
    return _region_text_scope_worthy(region_diff, page_size)


def _box_intersects_noise_region(box: BBox, regions: list[Region]) -> bool:
    for region in regions:
        if region.kind not in IGNORED_REGION_KINDS:
            continue
        if bbox_overlap_ratio(box, region.bbox) > 0.15:
            return True
    return False


def _box_near_scope_region(box: BBox, regions: list[Region], page_size: tuple[float, float]) -> bool:
    margin = max(18.0, min(page_size) * 0.025)
    expanded = (
        max(0.0, box[0] - margin),
        max(0.0, box[1] - margin),
        min(page_size[0], box[2] + margin),
        min(page_size[1], box[3] + margin),
    )
    for region in regions:
        if region.kind in IGNORED_REGION_KINDS:
            continue
        if _region_has_scope_context(region):
            if bbox_overlap_ratio(expanded, region.bbox) > 0.0:
                return True
    return False


def _word_bbox(word: tuple) -> BBox:
    return (float(word[0]), float(word[1]), float(word[2]), float(word[3]))


def _box_near_scope_words(box: BBox, pages: list[fitz.Page], page_size: tuple[float, float]) -> bool:
    margin = max(24.0, min(page_size) * 0.03)
    expanded = (
        max(0.0, box[0] - margin),
        max(0.0, box[1] - margin),
        min(page_size[0], box[2] + margin),
        min(page_size[1], box[3] + margin),
    )
    for page in pages:
        for word in page.get_text("words"):
            text = str(word[4])
            if not _has_specific_scope_signal(text) or _is_revision_date_or_metadata_noise(text):
                continue
            if bbox_overlap_ratio(expanded, _word_bbox(word)) > 0.0:
                return True
    return False


def _scope_visual_boxes(
    boxes: list[BBox],
    before_analysis,
    after_analysis,
    page_size: tuple[float, float],
    before_page: fitz.Page | None = None,
    after_page: fitz.Page | None = None,
) -> list[BBox]:
    regions = []
    if before_analysis is not None:
        regions.extend(before_analysis.regions)
    if after_analysis is not None:
        regions.extend(after_analysis.regions)
    scoped: list[BBox] = []
    for box in boxes:
        area_ratio = _bbox_area_ratio(box, page_size)
        if area_ratio <= 0.0 or area_ratio > 0.035:
            continue
        center_x = (box[0] + box[2]) / 2
        center_y = (box[1] + box[3]) / 2
        in_title_zone = center_x > page_size[0] * 0.64 and center_y > page_size[1] * 0.45
        in_footer_zone = center_y > page_size[1] * 0.84
        if in_title_zone or in_footer_zone:
            continue
        if _box_intersects_noise_region(box, regions):
            continue
        pages = [page for page in (before_page, after_page) if page is not None]
        if _box_near_scope_region(box, regions, page_size) or _box_near_scope_words(box, pages, page_size):
            scoped.append(box)
    return scoped


def compare_regions(
    before_regions: list[Region],
    after_regions: list[Region],
    page_index: int,
    before_size: tuple[float, float],
    after_size: tuple[float, float],
    cancel_requested: CancelCallback = None,
) -> list[RegionDiff]:
    candidates: list[tuple[float, Region, Region]] = []
    for before_region in before_regions:
        _check_cancel(cancel_requested)
        for after_region in after_regions:
            score = _match_region_score(before_region, after_region, before_size, after_size)
            if score >= 0.45:
                candidates.append((score, before_region, after_region))
    candidates.sort(key=lambda item: item[0], reverse=True)

    used_before: set[str] = set()
    used_after: set[str] = set()
    diffs: list[RegionDiff] = []

    for score, before_region, after_region in candidates:
        _check_cancel(cancel_requested)
        if before_region.region_id in used_before or after_region.region_id in used_after:
            continue
        used_before.add(before_region.region_id)
        used_after.add(after_region.region_id)
        text_similarity = SequenceMatcher(None, before_region.normalized_text, after_region.normalized_text).ratio()
        row_diffs = [] if text_similarity > 0.995 else compare_rows(before_region, after_region, page_index)
        change_type = "unchanged" if text_similarity > 0.995 and not row_diffs else "modified"
        emphasized = change_type != "unchanged" and before_region.kind in {"table-like", "bom-like", "metadata-like", "title-block-like"}
        if change_type != "unchanged":
            diff = RegionDiff(
                page_index=page_index,
                region_id_before=before_region.region_id,
                region_id_after=after_region.region_id,
                region_kind=before_region.kind,
                change_type=change_type,
                confidence=min(0.99, max(score, text_similarity)),
                before_bbox=before_region.bbox,
                after_bbox=after_region.bbox,
                before_text=before_region.text[:2000],
                after_text=after_region.text[:2000],
                emphasized=emphasized,
                row_diffs=row_diffs,
            )
            diff.emphasized = is_scope_region_diff(diff, before_size)
            diffs.append(diff)

    for before_region in before_regions:
        if before_region.region_id in used_before:
            continue
        diff = RegionDiff(
            page_index=page_index,
            region_id_before=before_region.region_id,
            region_id_after=None,
            region_kind=before_region.kind,
            change_type="removed",
            confidence=0.80,
            before_bbox=before_region.bbox,
            after_bbox=None,
            before_text=before_region.text[:2000],
            after_text="",
            emphasized=before_region.kind != "general",
            row_diffs=[
                RowDiff(
                    page_index=page_index,
                    region_id=before_region.region_id,
                    region_kind=before_region.kind,
                    change_type="removed",
                    row_key=row.key,
                    before_text=row.text,
                    after_text="",
                    before_cells=row.cells,
                    bbox=row.bbox,
                    before_bbox=row.bbox,
                    before_changed_bboxes=[row.bbox],
                    confidence=0.80,
                    severity="high" if before_region.kind in {"table-like", "bom-like"} else "medium",
                )
                for row in before_region.rows
            ],
            )
        diff.emphasized = is_scope_region_diff(diff, before_size)
        diffs.append(diff)

    for after_region in after_regions:
        if after_region.region_id in used_after:
            continue
        diff = RegionDiff(
            page_index=page_index,
            region_id_before=None,
            region_id_after=after_region.region_id,
            region_kind=after_region.kind,
            change_type="added",
            confidence=0.80,
            before_bbox=None,
            after_bbox=after_region.bbox,
            before_text="",
            after_text=after_region.text[:2000],
            emphasized=after_region.kind != "general",
            row_diffs=[
                RowDiff(
                    page_index=page_index,
                    region_id=after_region.region_id,
                    region_kind=after_region.kind,
                    change_type="added",
                    row_key=row.key,
                    before_text="",
                    after_text=row.text,
                    after_cells=row.cells,
                    bbox=row.bbox,
                    after_bbox=row.bbox,
                    after_changed_bboxes=[row.bbox],
                    confidence=0.80,
                    severity="high" if after_region.kind in {"table-like", "bom-like"} else "medium",
                )
                for row in after_region.rows
            ],
            )
        diff.emphasized = is_scope_region_diff(diff, after_size)
        diffs.append(diff)

    return sorted(diffs, key=lambda item: (item.page_index, item.region_kind, item.change_type))


def _render_page_image(page: fitz.Page, dpi: int) -> Image.Image:
    pix = page.get_pixmap(dpi=dpi, alpha=False)
    return Image.frombytes("RGB", [pix.width, pix.height], pix.samples)


def _adaptive_visual_dpi(before_page: fitz.Page, after_page: fitz.Page, requested_dpi: int) -> int:
    page_area = max(
        float(before_page.rect.width * before_page.rect.height),
        float(after_page.rect.width * after_page.rect.height),
        1.0,
    )
    estimated_pixels = page_area * ((requested_dpi / 72.0) ** 2)
    if estimated_pixels <= MAX_VISUAL_RENDER_PIXELS:
        return requested_dpi
    capped_dpi = int(72.0 * math.sqrt(MAX_VISUAL_RENDER_PIXELS / page_area))
    return max(24, min(requested_dpi, capped_dpi))


def _adaptive_tile_size(width: int, height: int) -> int:
    return max(10, int(math.ceil(max(width, height) / 180)))


def _mask_bounds_box(mask: numpy.ndarray) -> tuple[int, int, int, int] | None:
    y_indexes, x_indexes = numpy.where(mask)
    if len(x_indexes) == 0:
        return None
    return (
        int(x_indexes.min()),
        int(y_indexes.min()),
        int(x_indexes.max()) + 1,
        int(y_indexes.max()) + 1,
    )


def _component_boxes_from_mask(
    mask: numpy.ndarray,
    tile_size: int = 10,
    max_boxes: int = MAX_VISUAL_BOXES_PER_PAGE,
) -> list[tuple[int, int, int, int]]:
    height, width = mask.shape
    if height <= 0 or width <= 0:
        return []
    tile_rows = math.ceil(height / tile_size)
    tile_cols = math.ceil(width / tile_size)
    padded_height = tile_rows * tile_size
    padded_width = tile_cols * tile_size
    padded = numpy.zeros((padded_height, padded_width), dtype=bool)
    padded[:height, :width] = mask
    active = padded.reshape(tile_rows, tile_size, tile_cols, tile_size).any(axis=(1, 3))

    visited = numpy.zeros_like(active, dtype=bool)
    boxes: list[tuple[int, int, int, int]] = []
    for row in range(tile_rows):
        for col in range(tile_cols):
            if visited[row, col] or not active[row, col]:
                continue
            stack = [(row, col)]
            visited[row, col] = True
            rows: list[int] = []
            cols: list[int] = []
            while stack:
                current_row, current_col = stack.pop()
                rows.append(current_row)
                cols.append(current_col)
                for next_row in range(max(0, current_row - 1), min(tile_rows, current_row + 2)):
                    for next_col in range(max(0, current_col - 1), min(tile_cols, current_col + 2)):
                        if visited[next_row, next_col] or not active[next_row, next_col]:
                            continue
                        visited[next_row, next_col] = True
                        stack.append((next_row, next_col))
            boxes.append(
                (
                    min(cols) * tile_size,
                    min(rows) * tile_size,
                    min(width, (max(cols) + 1) * tile_size),
                    min(height, (max(rows) + 1) * tile_size),
                )
            )
            if len(boxes) > max_boxes:
                bounds = _mask_bounds_box(mask)
                return [bounds] if bounds is not None else []
    return boxes


def _coarse_boxes_from_mask(mask: numpy.ndarray, max_boxes: int = MAX_VISUAL_BOXES_PER_PAGE) -> list[tuple[int, int, int, int]]:
    height, width = mask.shape
    if height <= 0 or width <= 0 or not bool(mask.any()):
        return []
    full_box = _mask_bounds_box(mask)
    if full_box is None:
        return []
    active_ratio = float(mask.sum()) / max(mask.size, 1)
    if active_ratio >= DENSE_VISUAL_DIFF_RATIO:
        return [full_box]

    tile_size = max(_adaptive_tile_size(width, height), int(math.ceil(math.sqrt(mask.size / max(max_boxes, 1)))))
    return _component_boxes_from_mask(mask, tile_size=tile_size, max_boxes=max_boxes)


def _pixel_diff_boxes(
    diff_array: numpy.ndarray,
    threshold_value: int,
    max_boxes: int = MAX_VISUAL_BOXES_PER_PAGE,
) -> list[tuple[int, int, int, int]]:
    mask = diff_array > threshold_value
    if not bool(mask.any()):
        return []
    active_ratio = float(mask.sum()) / max(mask.size, 1)
    if active_ratio >= DENSE_VISUAL_DIFF_RATIO:
        return _coarse_boxes_from_mask(mask, max_boxes=max_boxes)
    if (
        findContours is not None
        and threshold is not None
        and boundingRect is not None
        and diff_array.size <= MAX_VISUAL_RENDER_PIXELS
    ):
        contours, _hierarchy = findContours(
            threshold(diff_array, threshold_value, 255, THRESH_BINARY)[1],
            RETR_EXTERNAL,
            CHAIN_APPROX_SIMPLE,
        )
        boxes = [boundingRect(contour) for contour in contours]
        if len(boxes) <= max_boxes:
            return boxes
        return _coarse_boxes_from_mask(mask, max_boxes=max_boxes)
    return [
        (x0, y0, x1 - x0, y1 - y0)
        for x0, y0, x1, y1 in _component_boxes_from_mask(
            mask,
            tile_size=_adaptive_tile_size(diff_array.shape[1], diff_array.shape[0]),
            max_boxes=max_boxes,
        )
    ]


def _visual_diff_boxes(
    before_page: fitz.Page,
    after_page: fitz.Page,
    dpi: int = 72,
    threshold_value: int = 28,
    cancel_requested: CancelCallback = None,
) -> list[BBox]:
    _check_cancel(cancel_requested)
    dpi = _adaptive_visual_dpi(before_page, after_page, dpi)
    before_image = _render_page_image(before_page, dpi).convert("RGB")
    _check_cancel(cancel_requested)
    after_image = _render_page_image(after_page, dpi).convert("RGB")
    candidates = [after_image]
    before_ratio = before_image.width / max(before_image.height, 1)
    after_ratio = after_image.width / max(after_image.height, 1)
    if abs(before_ratio - after_ratio) > 0.15 or before_image.size != after_image.size:
        candidates.extend(after_image.rotate(angle, expand=True) for angle in (90, 180, 270))
    best_diff_array: numpy.ndarray | None = None
    best_score: float | None = None
    best_mean = 0.0
    best_active_ratio = 0.0
    for candidate in candidates:
        _check_cancel(cancel_requested)
        if candidate.size != before_image.size:
            candidate = candidate.resize(before_image.size)
        diff = ImageChops.difference(before_image, candidate)
        diff_array = numpy.array(diff).max(axis=2)
        active_ratio = float((diff_array > threshold_value).sum()) / max(diff_array.size, 1)
        score = float(diff_array.mean()) + (active_ratio * 255.0)
        if best_score is None or score < best_score:
            best_score = score
            best_diff_array = diff_array
            best_mean = float(diff_array.mean())
            best_active_ratio = active_ratio
    diff_array = best_diff_array if best_diff_array is not None else numpy.zeros((before_image.height, before_image.width), dtype=numpy.uint8)
    if best_active_ratio <= 0.0:
        return []
    if best_mean < 0.15 and best_active_ratio < 0.00002:
        return []

    boxes: list[BBox] = []
    x_scale = before_page.rect.width / max(before_image.width, 1)
    y_scale = before_page.rect.height / max(before_image.height, 1)
    pixel_boxes = _pixel_diff_boxes(diff_array, threshold_value, max_boxes=MAX_VISUAL_BOXES_PER_PAGE)
    pixel_boxes.sort(key=lambda item: item[2] * item[3], reverse=True)
    for x, y, width, height in pixel_boxes[:MAX_VISUAL_BOXES_PER_PAGE]:
        if (width * height) < 30:
            continue
        boxes.append(
            (
                round(x * x_scale, 2),
                round(y * y_scale, 2),
                round((x + width) * x_scale, 2),
                round((y + height) * y_scale, 2),
            )
        )
    return boxes


def compare_page_pair(
    before_page: fitz.Page | None,
    after_page: fitz.Page | None,
    before_analysis,
    after_analysis,
    page_label: str,
    cancel_requested: CancelCallback = None,
) -> PageDiff:
    if before_page is None and after_page is not None:
        page_box = (0.0, 0.0, float(after_page.rect.width), float(after_page.rect.height))
        return PageDiff(
            page_before_index=None,
            page_after_index=after_analysis.page_index if after_analysis is not None else None,
            page_label=page_label,
            change_type="added",
            confidence=1.0,
            global_boxes=[page_box],
            emphasized_boxes=[page_box],
            structured_regions_detected=bool(after_analysis and after_analysis.regions),
            text_source_before="none",
            text_source_after=after_analysis.text_source if after_analysis is not None else "analysis_failed",
            notes=["page exists only in after document"],
        )
    if after_page is None and before_page is not None:
        page_box = (0.0, 0.0, float(before_page.rect.width), float(before_page.rect.height))
        return PageDiff(
            page_before_index=before_analysis.page_index if before_analysis is not None else None,
            page_after_index=None,
            page_label=page_label,
            change_type="removed",
            confidence=1.0,
            global_boxes=[page_box],
            emphasized_boxes=[page_box],
            structured_regions_detected=bool(before_analysis and before_analysis.regions),
            text_source_before=before_analysis.text_source if before_analysis is not None else "analysis_failed",
            text_source_after="none",
            notes=["page exists only in before document"],
        )

    assert before_page is not None and after_page is not None
    if before_analysis is None or after_analysis is None:
        page_box = (0.0, 0.0, float(before_page.rect.width), float(before_page.rect.height))
        return PageDiff(
            page_before_index=before_analysis.page_index if before_analysis is not None else before_page.number,
            page_after_index=after_analysis.page_index if after_analysis is not None else after_page.number,
            page_label=page_label,
            change_type="review_needed",
            confidence=0.0,
            global_boxes=[page_box],
            emphasized_boxes=[page_box],
            structured_regions_detected=False,
            text_source_before=before_analysis.text_source if before_analysis is not None else "analysis_failed",
            text_source_after=after_analysis.text_source if after_analysis is not None else "analysis_failed",
            notes=["page analysis failed; manual review needed"],
        )

    raw_global_boxes = _visual_diff_boxes(before_page, after_page, cancel_requested=cancel_requested)
    region_diffs = compare_regions(
        before_analysis.regions,
        after_analysis.regions,
        before_analysis.page_index,
        (before_analysis.width, before_analysis.height),
        (after_analysis.width, after_analysis.height),
        cancel_requested=cancel_requested,
    )
    page_size = (before_analysis.width, before_analysis.height)
    scope_region_diffs = [region for region in region_diffs if is_scope_region_diff(region, page_size)]
    global_boxes = _scope_visual_boxes(raw_global_boxes, before_analysis, after_analysis, page_size, before_page, after_page)
    emphasized_boxes = [
        region.before_bbox or region.after_bbox
        for region in scope_region_diffs
        if region.emphasized and (region.before_bbox or region.after_bbox)
    ]
    change_type = "unchanged"
    if scope_region_diffs:
        change_type = "modified"
    elif global_boxes:
        change_type = "modified"

    confidence_points = [region.confidence for region in scope_region_diffs] or [0.7 if global_boxes else 0.95]
    return PageDiff(
        page_before_index=before_analysis.page_index,
        page_after_index=after_analysis.page_index,
        page_label=page_label,
        change_type=change_type,
        confidence=round(statistics.mean(confidence_points), 3),
        global_boxes=global_boxes,
        emphasized_boxes=[box for box in emphasized_boxes if box is not None],
        structured_regions_detected=bool(before_analysis.regions or after_analysis.regions),
        text_source_before=before_analysis.text_source,
        text_source_after=after_analysis.text_source,
        region_diffs=region_diffs,
        notes=[*before_analysis.warnings, *after_analysis.warnings],
    )


def _scale_bbox_for_page(bbox: BBox, source_size: tuple[float, float], target_size: tuple[float, float]) -> BBox:
    x_scale = target_size[0] / max(source_size[0], 1.0)
    y_scale = target_size[1] / max(source_size[1], 1.0)
    return (
        bbox[0] * x_scale,
        bbox[1] * y_scale,
        bbox[2] * x_scale,
        bbox[3] * y_scale,
    )


def _expand_bbox(bbox: BBox, padding: float, page_size: tuple[float, float]) -> BBox:
    width, height = page_size
    return (
        max(0.0, bbox[0] - padding),
        max(0.0, bbox[1] - padding),
        min(width, bbox[2] + padding),
        min(height, bbox[3] + padding),
    )


def _bbox_area_ratio_for_page(bbox: BBox, page_size: tuple[float, float]) -> float:
    page_area = max(page_size[0] * page_size[1], 1.0)
    return max(0.0, (bbox[2] - bbox[0]) * (bbox[3] - bbox[1])) / page_area


def _outline_for_kind(region_kind: str, change_type: str) -> tuple[int, int, int, int]:
    if change_type == "added":
        return (34, 160, 90, 255)  # green for added items
    if change_type == "removed":
        return (220, 50, 50, 255)  # red for removed items
    return (255, 193, 7, 255)  # yellow for modified/moved/changed items


def _annotation_style(region_kind: str, area_ratio: float, has_row_boxes: bool) -> str:
    if has_row_boxes:
        return "row"
    if area_ratio >= 0.12:
        return "corners"
    if region_kind in {"metadata-like", "title-block-like"}:
        return "corners"
    return "outline"


def _dedupe_annotations(
    annotations: list[HighlightAnnotation],
    page_size: tuple[float, float],
) -> list[HighlightAnnotation]:
    deduped: list[HighlightAnnotation] = []
    for annotation in annotations:
        area_ratio = _bbox_area_ratio_for_page(annotation.bbox, page_size)
        if area_ratio > 0.45 and annotation.style != "corners":
            annotation = HighlightAnnotation(
                bbox=annotation.bbox,
                outline=annotation.outline,
                style="corners",
                label=annotation.label,
                change_type=annotation.change_type,
            )
        duplicate = False
        for existing in deduped:
            if existing.outline == annotation.outline and bbox_overlap_ratio(existing.bbox, annotation.bbox) > 0.92:
                duplicate = True
                break
        if not duplicate:
            deduped.append(annotation)
    return deduped


def _page_annotations_for_side(
    page_diff: PageDiff,
    side: str,
    page_size: tuple[float, float],
) -> list[HighlightAnnotation]:
    annotations: list[HighlightAnnotation] = []
    has_structured_row_diffs = any(
        is_scope_row_diff(row_diff)
        for region in page_diff.region_diffs
        for row_diff in region.row_diffs
    )
    page_width = page_size[0]
    # Row highlights wider than this fraction of the page are suppressed to avoid page-wide bands.
    max_row_highlight_width = page_width * 0.45

    for region_diff in page_diff.region_diffs:
        if not is_scope_region_diff(region_diff, page_size):
            continue
        bbox = region_diff.before_bbox if side == "before" else region_diff.after_bbox
        region_outline = _outline_for_kind(region_diff.region_kind, region_diff.change_type)
        has_row_annotations = False

        for row_diff in region_diff.row_diffs:
            if not is_scope_row_diff(row_diff):
                continue
            # Removed rows belong only on the before (IFB) PDF.
            # Added rows belong only on the after (GMP) PDF.
            # Drawing them on the wrong side produces highlights over empty space.
            if row_diff.change_type == "removed" and side == "after":
                continue
            if row_diff.change_type == "added" and side == "before":
                continue

            row_outline = _outline_for_kind(region_diff.region_kind, row_diff.change_type)

            if side == "before":
                scoped = row_diff.before_changed_bboxes or ([row_diff.before_bbox] if row_diff.before_bbox else [])
            else:
                scoped = row_diff.after_changed_bboxes or ([row_diff.after_bbox] if row_diff.after_bbox else [])
            # Only fall back to row_diff.bbox (the before-side position) for modified rows where
            # both sides have real content.  Never use it for removed/added rows on the wrong side.
            if not scoped and row_diff.change_type == "modified" and row_diff.bbox is not None:
                scoped = [row_diff.bbox]

            # Suppress bboxes that span an unreasonable fraction of the page width — these are
            # artifacts of y-level word clustering across far-apart columns, not real cell regions.
            scoped = [box for box in scoped if (box[2] - box[0]) <= max_row_highlight_width]

            for row_box in scoped:
                has_row_annotations = True
                annotations.append(
                    HighlightAnnotation(
                        bbox=_expand_bbox(row_box, 1.5, page_size),
                        outline=row_outline,
                        style="highlight",
                        label=region_diff.region_kind,
                        change_type=row_diff.change_type,
                    )
                )

        if has_row_annotations:
            if bbox is not None and region_diff.region_kind in {"metadata-like", "title-block-like"}:
                annotations.append(
                    HighlightAnnotation(
                        bbox=_expand_bbox(bbox, 6.0, page_size),
                        outline=region_outline,
                        style="corners",
                        label=region_diff.region_kind,
                        change_type=region_diff.change_type,
                    )
                )
            continue

        if bbox is None:
            continue
        area_ratio = _bbox_area_ratio_for_page(bbox, page_size)
        if region_diff.region_kind == "general" and area_ratio > 0.08:
            continue
        expanded_bbox = _expand_bbox(bbox, 6.0, page_size)
        # Don't draw a wide filled band when no specific row cells were identified.
        if (expanded_bbox[2] - expanded_bbox[0]) > max_row_highlight_width:
            continue
        annotations.append(
            HighlightAnnotation(
                bbox=expanded_bbox,
                outline=region_outline,
                style=_annotation_style(region_diff.region_kind, area_ratio, has_row_boxes=False),
                label=region_diff.region_kind,
                change_type=region_diff.change_type,
            )
        )

    visual_boxes = [] if has_structured_row_diffs else page_diff.global_boxes[:MAX_VISUAL_BOXES_PER_PAGE]
    for bbox in visual_boxes:
        visual_bbox = _expand_bbox(bbox, 4.0, page_size)
        visual_area_ratio = _bbox_area_ratio_for_page(visual_bbox, page_size)
        annotations.append(
            HighlightAnnotation(
                bbox=visual_bbox,
                outline=(255, 193, 7, 255),
                style="corners" if visual_area_ratio >= 0.35 else "outline",
                label="visual-diff",
                change_type="modified",
            )
        )
    return _dedupe_annotations(annotations, page_size)[:MAX_HIGHLIGHT_MARKERS_PER_PAGE]


def _fill_rgba_for_change_type(change_type: str) -> tuple[int, int, int, int]:
    if change_type == "added":
        return (119, 236, 245, 60)  # aqua blue for additions
    if change_type == "removed":
        return (180, 245, 200, 60)  # light green for removals
    return (255, 200, 200, 60)  # light red for modifications/moves/changes


def _draw_double_outline(
    draw: ImageDraw.ImageDraw,
    bbox: BBox,
    outline: tuple[int, int, int, int],
    width: int,
    fill_rgba: tuple[int, int, int, int] = (255, 232, 64, 45),
) -> None:
    x0, y0, x1, y1 = [int(round(value)) for value in bbox]
    shadow = (255, 255, 255, 220)
    draw.rectangle((x0, y0, x1, y1), fill=fill_rgba)
    draw.rectangle((x0, y0, x1, y1), outline=shadow, width=width + 2)
    draw.rectangle((x0, y0, x1, y1), outline=outline, width=width)


def _draw_corner_brackets(
    draw: ImageDraw.ImageDraw,
    bbox: BBox,
    outline: tuple[int, int, int, int],
    width: int,
) -> None:
    x0, y0, x1, y1 = [int(round(value)) for value in bbox]
    corner = max(14, min((x1 - x0) // 5, (y1 - y0) // 5, 40))
    shadow = (255, 255, 255, 220)
    lines = [
        ((x0, y0), (x0 + corner, y0)),
        ((x0, y0), (x0, y0 + corner)),
        ((x1, y0), (x1 - corner, y0)),
        ((x1, y0), (x1, y0 + corner)),
        ((x0, y1), (x0 + corner, y1)),
        ((x0, y1), (x0, y1 - corner)),
        ((x1, y1), (x1 - corner, y1)),
        ((x1, y1), (x1, y1 - corner)),
    ]
    for start, end in lines:
        draw.line((start, end), fill=shadow, width=width + 2)
    for start, end in lines:
        draw.line((start, end), fill=outline, width=width)


def _draw_row_marker(
    draw: ImageDraw.ImageDraw,
    bbox: BBox,
    outline: tuple[int, int, int, int],
    width: int,
    fill_rgba: tuple[int, int, int, int] = (255, 232, 64, 45),
) -> None:
    _draw_double_outline(draw, bbox, outline, width, fill_rgba)
    x0, y0, _x1, y1 = [int(round(value)) for value in bbox]
    shadow = (255, 255, 255, 220)
    draw.line(((x0 - 10, y0), (x0 - 10, y1)), fill=shadow, width=width + 2)
    draw.line(((x0 - 10, y0), (x0 - 10, y1)), fill=outline, width=width)


def _draw_vector_markers(page: fitz.Page, annotations: list[HighlightAnnotation]) -> None:
    if not annotations:
        return
    stroke_blue = (0.24, 0.84, 0.90)
    fill_blue = (0.47, 0.93, 0.96)
    stroke_green = (0.13, 0.58, 0.34)
    fill_green = (0.70, 0.95, 0.78)
    stroke_red = (0.86, 0.20, 0.20)
    fill_red = (1.0, 0.75, 0.75)
    page_size = (float(page.rect.width), float(page.rect.height))
    blue_text_shape = page.new_shape()
    blue_broad_shape = page.new_shape()
    green_text_shape = page.new_shape()
    green_broad_shape = page.new_shape()
    red_text_shape = page.new_shape()
    red_broad_shape = page.new_shape()
    has_bt = has_bb = has_gt = has_gb = has_rt = has_rb = False
    for annotation in annotations:
        rect = fitz.Rect(*annotation.bbox)
        area_ratio = _bbox_area_ratio_for_page(annotation.bbox, page_size)
        is_broad = annotation.style == "corners" or area_ratio >= 0.18
        if annotation.change_type == "added":
            if is_broad:
                blue_broad_shape.draw_rect(rect)
                has_bb = True
            else:
                blue_text_shape.draw_rect(rect)
                has_bt = True
        elif annotation.change_type == "removed":
            if is_broad:
                green_broad_shape.draw_rect(rect)
                has_gb = True
            else:
                green_text_shape.draw_rect(rect)
                has_gt = True
        else:
            if is_broad:
                red_broad_shape.draw_rect(rect)
                has_rb = True
            else:
                red_text_shape.draw_rect(rect)
                has_rt = True
    if has_bt:
        blue_text_shape.finish(width=0.0, color=stroke_blue, fill=fill_blue, stroke_opacity=0.0, fill_opacity=0.28)
        blue_text_shape.commit(overlay=True)
    if has_bb:
        blue_broad_shape.finish(width=0.0, color=stroke_blue, fill=fill_blue, stroke_opacity=0.0, fill_opacity=0.10)
        blue_broad_shape.commit(overlay=True)
    if has_gt:
        green_text_shape.finish(width=0.0, color=stroke_green, fill=fill_green, stroke_opacity=0.0, fill_opacity=0.28)
        green_text_shape.commit(overlay=True)
    if has_gb:
        green_broad_shape.finish(width=0.0, color=stroke_green, fill=fill_green, stroke_opacity=0.0, fill_opacity=0.10)
        green_broad_shape.commit(overlay=True)
    if has_rt:
        red_text_shape.finish(width=0.0, color=stroke_red, fill=fill_red, stroke_opacity=0.0, fill_opacity=0.28)
        red_text_shape.commit(overlay=True)
    if has_rb:
        red_broad_shape.finish(width=0.0, color=stroke_red, fill=fill_red, stroke_opacity=0.0, fill_opacity=0.10)
        red_broad_shape.commit(overlay=True)


def render_highlight_pdf(
    source_path: Path,
    page_diffs: list[PageDiff],
    side: str,
    output_path: Path,
    dpi: int = 96,
    *,
    progress_callback: ProgressCallback = None,
    cancel_requested: CancelCallback = None,
) -> str | None:
    if not page_diffs:
        return None

    vector_error: Exception | None = None
    try:
        with fitz.open(source_path) as source_doc:
            output_doc = fitz.open()
            output_doc.insert_pdf(source_doc)
            total_pages = max(len(page_diffs), 1)
            for page_offset, page_diff in enumerate(page_diffs, start=1):
                _check_cancel(cancel_requested)
                if progress_callback is not None:
                    progress_callback(
                        stage=f"rendering-{side}",
                        current=page_offset,
                        total=total_pages,
                        current_file=source_path.name,
                        side=side,
                    )
                source_index = page_diff.page_before_index if side == "before" else page_diff.page_after_index
                if source_index is None or source_index >= output_doc.page_count:
                    continue
                page = output_doc[source_index]
                page_size = (float(page.rect.width), float(page.rect.height))
                annotations = _page_annotations_for_side(page_diff, side, page_size)
                _draw_vector_markers(page, annotations)
            output_doc.save(output_path, garbage=4, deflate=True)
            output_doc.close()
        return "vector"
    except Exception as exc:  # noqa: BLE001
        # Raster fallback keeps output generation robust when annotations fail on unusual PDFs.
        vector_error = exc

    with fitz.open(source_path) as source_doc:
        output_doc = fitz.open()
        total_pages = max(len(page_diffs), 1)
        for page_offset, page_diff in enumerate(page_diffs, start=1):
            _check_cancel(cancel_requested)
            if progress_callback is not None:
                progress_callback(
                    stage=f"rendering-{side}",
                    current=page_offset,
                    total=total_pages,
                    current_file=source_path.name,
                    side=side,
                )
            source_index = page_diff.page_before_index if side == "before" else page_diff.page_after_index
            if source_index is None:
                continue
            page = source_doc[source_index]
            image = _render_page_image(page, dpi).convert("RGBA")
            draw = ImageDraw.Draw(image, "RGBA")
            image_size = (image.width, image.height)
            page_size = (float(page.rect.width), float(page.rect.height))
            annotations = _page_annotations_for_side(page_diff, side, page_size)
            line_width = max(2, dpi // 72)
            for annotation in annotations:
                scaled = _scale_bbox_for_page(annotation.bbox, page_size, image_size)
                fill_rgba = _fill_rgba_for_change_type(annotation.change_type)
                if annotation.style == "corners":
                    _draw_corner_brackets(draw, scaled, annotation.outline, line_width)
                elif annotation.style == "row":
                    _draw_row_marker(draw, scaled, annotation.outline, line_width, fill_rgba)
                else:
                    _draw_double_outline(draw, scaled, annotation.outline, line_width, fill_rgba)

            note = f"{side.upper()} | {page_diff.page_label} | {page_diff.change_type.upper()} | {len(annotations)} MARKERS"
            draw.rectangle((20, 18, min(image.width - 20, 760), 64), fill=(20, 24, 28, 200))
            draw.text((32, 28), note, fill=(245, 245, 245, 255))

            page_pdf = output_doc.new_page(width=page.rect.width, height=page.rect.height)
            buffer = io.BytesIO()
            image.convert("RGB").save(buffer, format="PNG", optimize=True)
            page_pdf.insert_image(page_pdf.rect, stream=buffer.getvalue())

        output_doc.save(output_path)
        output_doc.close()
    return f"raster:{type(vector_error).__name__}" if vector_error is not None else "raster"


def compare_pair(
    before_record: PdfRecord,
    after_record: PdfRecord,
    highlighted_before_path: Path | None = None,
    highlighted_after_path: Path | None = None,
    *,
    progress_callback: ProgressCallback = None,
    cancel_requested: CancelCallback = None,
    pair_index: int | None = None,
    pair_total: int | None = None,
    decision_index: int | None = None,
    decision_total: int | None = None,
) -> DocumentResult:
    page_diffs: list[PageDiff] = []
    exceptions: list[str] = []
    warnings: list[str] = []

    def emit(stage: str, current: int, total: int, current_file: str, **extra: object) -> None:
        if progress_callback is not None:
            progress_callback(
                stage=stage,
                current=current,
                total=total,
                current_file=current_file,
                pair_index=pair_index,
                pair_total=pair_total,
                decision_index=decision_index,
                decision_total=decision_total,
                **extra,
            )

    def relay_nested_progress(**payload: object) -> None:
        stage = str(payload.get("stage") or "working")
        current = int(payload.get("current") or 0)
        total = int(payload.get("total") or 0)
        current_file = str(payload.get("current_file") or "")
        nested_extra = dict(payload)
        nested_extra.pop("stage", None)
        nested_extra.pop("current", None)
        nested_extra.pop("total", None)
        nested_extra.pop("current_file", None)
        emit(stage, current, total, current_file, **nested_extra)

    _check_cancel(cancel_requested)
    emit(
        "compare-pair",
        0,
        1,
        f"{before_record.path.name} -> {after_record.path.name}",
        status="starting",
    )

    before_analyses, before_exceptions = analyze_document(
        before_record.path,
        progress_callback=relay_nested_progress,
        cancel_requested=cancel_requested,
        document_label=before_record.path.name,
        stage_label="analyzing-before",
        sheet_family=before_record.family,
    )
    _check_cancel(cancel_requested)
    after_analyses, after_exceptions = analyze_document(
        after_record.path,
        progress_callback=relay_nested_progress,
        cancel_requested=cancel_requested,
        document_label=after_record.path.name,
        stage_label="analyzing-after",
        sheet_family=after_record.family,
    )
    exceptions.extend(before_exceptions)
    exceptions.extend(after_exceptions)

    with fitz.open(before_record.path) as before_doc, fitz.open(after_record.path) as after_doc:
        page_map = align_pages(before_doc, after_doc, cancel_requested=cancel_requested)
        total_page_pairs = max(len(page_map), 1)
        for page_pair_index, (before_index, after_index) in enumerate(page_map):
            _check_cancel(cancel_requested)
            emit(
                "comparing-pages",
                page_pair_index + 1,
                total_page_pairs,
                f"{before_record.path.name} -> {after_record.path.name}",
                page_index=page_pair_index,
                page_total=total_page_pairs,
                before_page_index=before_index,
                after_page_index=after_index,
            )
            before_page = before_doc[before_index] if before_index is not None else None
            after_page = after_doc[after_index] if after_index is not None else None
            before_analysis = before_analyses[before_index] if before_index is not None and before_index < len(before_analyses) else None
            after_analysis = after_analyses[after_index] if after_index is not None and after_index < len(after_analyses) else None
            page_diffs.append(
                compare_page_pair(
                    before_page,
                    after_page,
                    before_analysis,
                    after_analysis,
                    page_label=f"Page {page_pair_index + 1}",
                    cancel_requested=cancel_requested,
                )
            )

    changed = any(page.change_type != "unchanged" for page in page_diffs)
    document_confidence = round(statistics.mean(page.confidence for page in page_diffs), 3) if page_diffs else 0.0

    review_needed = any(page.change_type == "review_needed" for page in page_diffs)

    if changed and highlighted_before_path is not None:
        render_mode = render_highlight_pdf(
            before_record.path,
            page_diffs,
            "before",
            highlighted_before_path,
            progress_callback=relay_nested_progress,
            cancel_requested=cancel_requested,
        )
        if render_mode and render_mode.startswith("raster"):
            warnings.append(f"Before highlighted PDF used raster fallback ({render_mode}).")
    if changed and highlighted_after_path is not None:
        render_mode = render_highlight_pdf(
            after_record.path,
            page_diffs,
            "after",
            highlighted_after_path,
            progress_callback=relay_nested_progress,
            cancel_requested=cancel_requested,
        )
        if render_mode and render_mode.startswith("raster"):
            warnings.append(f"After highlighted PDF used raster fallback ({render_mode}).")

    if any(page.text_source_before == "ocr" or page.text_source_after == "ocr" for page in page_diffs):
        warnings.append("OCR fallback was used on at least one page")

    return DocumentResult(
        decision=None,  # type: ignore[arg-type]
        status="review_needed" if review_needed else "changed" if changed else "unchanged",
        changed=changed,
        document_confidence=document_confidence,
        page_diffs=page_diffs,
        highlighted_before_pdf=highlighted_before_path if changed else None,
        highlighted_after_pdf=highlighted_after_path if changed else None,
        warnings=warnings,
        exceptions=exceptions,
    )
