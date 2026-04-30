from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

BBox = tuple[float, float, float, float]


@dataclass(slots=True)
class PdfRecord:
    path: Path
    source_group: str
    page_count: int
    page_sizes: list[tuple[float, float]]
    file_size: int
    modified_at: float
    filename_stem: str
    normalized_stem: str
    filename_tokens: list[str]
    title_hint: str
    title_tokens: list[str]
    revision_hint: str | None
    version_hint: int | None
    date_hints: list[str]
    first_page_fingerprint: str
    first_page_text_preview: str
    native_text_char_count: int
    sheet_id: str | None = None
    revision: str | None = None
    sheet_title: str = ""
    family: str = "unknown"
    phase: str = ""
    revision_status: str = "unknown"
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PairCandidate:
    before: PdfRecord
    after: PdfRecord
    score: float
    confidence: float
    reasons: list[str]
    pairing_method: str


@dataclass(slots=True)
class PairDecision:
    status: str
    confidence: float
    pairing_method: str
    before: PdfRecord | None = None
    after: PdfRecord | None = None
    sheet_id: str | None = None
    family: str = "unknown"
    revision_status: str = "unknown"
    reasons: list[str] = field(default_factory=list)
    competing_candidates: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class StructuredRow:
    row_index: int
    bbox: BBox
    cells: list[str]
    text: str
    normalized_text: str
    key: str
    cell_bboxes: list[BBox] = field(default_factory=list)


@dataclass(slots=True)
class Region:
    region_id: str
    page_index: int
    kind: str
    bbox: BBox
    text: str
    normalized_text: str
    confidence: float
    source: str
    rows: list[StructuredRow] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PageAnalysis:
    page_index: int
    width: float
    height: float
    text_source: str
    native_text_char_count: int
    ocr_attempted: bool
    regions: list[Region]
    plain_lines: list[str]
    raw_text_preview: str
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RowDiff:
    page_index: int
    region_id: str
    region_kind: str
    change_type: str
    row_key: str
    before_text: str
    after_text: str
    before_cells: list[str] = field(default_factory=list)
    after_cells: list[str] = field(default_factory=list)
    bbox: BBox | None = None
    before_bbox: BBox | None = None
    after_bbox: BBox | None = None
    before_changed_bboxes: list[BBox] = field(default_factory=list)
    after_changed_bboxes: list[BBox] = field(default_factory=list)
    confidence: float = 0.0
    severity: str = "medium"


@dataclass(slots=True)
class RegionDiff:
    page_index: int
    region_id_before: str | None
    region_id_after: str | None
    region_kind: str
    change_type: str
    confidence: float
    before_bbox: BBox | None
    after_bbox: BBox | None
    before_text: str
    after_text: str
    emphasized: bool
    row_diffs: list[RowDiff] = field(default_factory=list)


@dataclass(slots=True)
class PageDiff:
    page_before_index: int | None
    page_after_index: int | None
    page_label: str
    change_type: str
    confidence: float
    global_boxes: list[BBox]
    emphasized_boxes: list[BBox]
    structured_regions_detected: bool
    text_source_before: str
    text_source_after: str
    region_diffs: list[RegionDiff] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DocumentResult:
    decision: PairDecision | None
    status: str
    changed: bool
    document_confidence: float
    page_diffs: list[PageDiff] = field(default_factory=list)
    highlighted_before_pdf: Path | None = None
    highlighted_after_pdf: Path | None = None
    warnings: list[str] = field(default_factory=list)
    exceptions: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RunManifest:
    run_id: str
    created_at: datetime
    mode: str
    input_paths: list[str]
    output_dir: Path
    changed_pairs_dir: Path
    added_dir: Path
    removed_dir: Path
    review_needed_dir: Path
    logs_dir: Path


@dataclass(slots=True)
class RunResult:
    manifest: RunManifest
    scanned_records: list[PdfRecord]
    decisions: list[PairDecision]
    documents: list[DocumentResult]
    exceptions: list[str] = field(default_factory=list)
    log_path: Path | None = None
    report_path: Path | None = None
    manifest_path: Path | None = None
