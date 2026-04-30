from __future__ import annotations

import hashlib
import statistics
from pathlib import Path
from typing import Callable

import fitz
from PIL import Image

from .models import BBox, PageAnalysis, Region, StructuredRow

try:
    import pytesseract  # type: ignore
except Exception:  # noqa: BLE001
    pytesseract = None

METADATA_KEYWORDS = {
    "revision",
    "revisions",
    "rev",
    "title",
    "sheet",
    "drawing",
    "drawn",
    "checked",
    "project",
    "date",
    "issue",
    "issued",
    "scale",
    "client",
    "owner",
    "approved",
    "number",
}

TABLE_HINT_KEYWORDS = {
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
}

SCOPE_TABLE_HEADER_KEYWORDS = {
    "item",
    "qty",
    "quantity",
    "description",
    "material",
    "part",
    "device",
    "model",
    "manufacturer",
    "make",
    "type",
    "function",
    "location",
    "mounting",
    "port",
    "switch",
    "tag",
    "equipment",
    "equip",
    "idf",
    "mdf",
}

NOISE_TABLE_HEADER_KEYWORDS = {
    "revision",
    "revisions",
    "rev",
    "date",
    "drawing",
    "sheet",
    "title",
    "project",
    "scale",
    "drawn",
    "checked",
    "approved",
    "issue",
    "issued",
}

STRONG_TABLE_HINT_KEYWORDS = {
    "item",
    "qty",
    "quantity",
    "description",
    "material",
    "part",
    "schedule",
    "bom",
    "bill",
    "manufacturer",
    "model",
}

SCHEDULE_FAMILIES = {"schedule", "switch_schedule", "fiber_schedule"}
LARGE_DRAWING_PAGE_AREA = 1_200_000

ProgressCallback = Callable[..., None] | None
CancelCallback = Callable[[], bool] | None


class AnalysisCancelledError(RuntimeError):
    pass


def normalize_text(text: str) -> str:
    return " ".join(text.lower().split())


def _check_cancel(cancel_requested: CancelCallback) -> None:
    if cancel_requested is not None and cancel_requested():
        raise AnalysisCancelledError("Compare job stopped by user.")


def bbox_area(bbox: BBox) -> float:
    return max(0.0, bbox[2] - bbox[0]) * max(0.0, bbox[3] - bbox[1])


def bbox_contains(outer: BBox, inner: BBox) -> bool:
    return outer[0] <= inner[0] and outer[1] <= inner[1] and outer[2] >= inner[2] and outer[3] >= inner[3]


def bbox_union(boxes: list[BBox]) -> BBox:
    return (
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    )


def bbox_overlap_ratio(left: BBox, right: BBox) -> float:
    x0 = max(left[0], right[0])
    y0 = max(left[1], right[1])
    x1 = min(left[2], right[2])
    y1 = min(left[3], right[3])
    if x1 <= x0 or y1 <= y0:
        return 0.0
    intersection = (x1 - x0) * (y1 - y0)
    return intersection / max(min(bbox_area(left), bbox_area(right)), 1e-9)


def _cluster_words_to_rows(words: list[tuple]) -> list[list[tuple]]:
    if not words:
        return []
    heights = [word[3] - word[1] for word in words]
    tolerance = max(3.0, statistics.median(heights) * 0.75)
    sorted_words = sorted(words, key=lambda item: (((item[1] + item[3]) / 2), item[0]))
    rows: list[list[tuple]] = []
    row_centers: list[float] = []

    for word in sorted_words:
        center = (word[1] + word[3]) / 2
        if not rows or abs(center - row_centers[-1]) > tolerance:
            rows.append([word])
            row_centers.append(center)
            continue
        rows[-1].append(word)
        row_centers[-1] = statistics.mean(((entry[1] + entry[3]) / 2) for entry in rows[-1])
    return rows


def _words_to_cell_parts(row_words: list[tuple]) -> list[tuple[str, BBox]]:
    if not row_words:
        return []
    ordered = sorted(row_words, key=lambda item: item[0])
    heights = [word[3] - word[1] for word in ordered]
    tolerance = max(18.0, statistics.median(heights) * 1.5)
    groups: list[list[tuple]] = []
    current: list[tuple] = [ordered[0]]
    last_x1 = ordered[0][2]
    for word in ordered[1:]:
        gap = word[0] - last_x1
        if gap > tolerance:
            groups.append(current)
            current = [word]
        else:
            current.append(word)
        last_x1 = word[2]
    groups.append(current)
    parts: list[tuple[str, BBox]] = []
    for group in groups:
        text = " ".join(str(word[4]) for word in group).strip()
        if not text:
            continue
        parts.append((text, bbox_union([_word_bbox(word) for word in group])))
    return parts


def _words_to_cells(row_words: list[tuple]) -> list[str]:
    return [text for text, _bbox in _words_to_cell_parts(row_words)]


def _row_from_words(row_words: list[tuple], row_index: int) -> StructuredRow:
    cell_parts = _words_to_cell_parts(row_words)
    cells = [text for text, _bbox in cell_parts]
    cell_bboxes = [bbox for _text, bbox in cell_parts]
    text = " ".join(word[4] for word in sorted(row_words, key=lambda item: item[0])).strip()
    normalized = normalize_text(text)
    bbox: BBox = (
        min(word[0] for word in row_words),
        min(word[1] for word in row_words),
        max(word[2] for word in row_words),
        max(word[3] for word in row_words),
    )
    key_seed = "|".join(cells[:3]) if cells else normalized
    key = hashlib.sha1(normalize_text(key_seed).encode("utf-8")).hexdigest()[:12]
    return StructuredRow(
        row_index=row_index,
        bbox=bbox,
        cells=cells,
        text=text,
        normalized_text=normalized,
        key=key,
        cell_bboxes=cell_bboxes,
    )


def _clip_row_to_x(row: StructuredRow, x0: float, x1: float) -> StructuredRow | None:
    if not row.cell_bboxes:
        center = (row.bbox[0] + row.bbox[2]) / 2
        return row if x0 <= center <= x1 else None
    kept = [
        (cell, bbox)
        for cell, bbox in zip(row.cells, row.cell_bboxes)
        if x0 <= ((bbox[0] + bbox[2]) / 2) <= x1
    ]
    if not kept:
        return None
    cells = [cell for cell, _bbox in kept]
    bboxes = [bbox for _cell, bbox in kept]
    text = " ".join(cells).strip()
    if not text:
        return None
    key_seed = "|".join(cells[:3]) if cells else normalize_text(text)
    return StructuredRow(
        row_index=row.row_index,
        bbox=bbox_union(bboxes),
        cells=cells,
        text=text,
        normalized_text=normalize_text(text),
        key=hashlib.sha1(normalize_text(key_seed).encode("utf-8")).hexdigest()[:12],
        cell_bboxes=bboxes,
    )


def _rows_in_bbox(words: list[tuple], bbox: BBox) -> list[StructuredRow]:
    scoped = [
        word for word in words
        if bbox[0] - 2 <= word[0] <= bbox[2] + 2 and bbox[1] - 2 <= word[1] <= bbox[3] + 2
    ]
    rows = _cluster_words_to_rows(scoped)
    structured: list[StructuredRow] = []
    for row_index, row_words in enumerate(rows):
        text = " ".join(word[4] for word in row_words).strip()
        if len(text) < 2:
            continue
        structured.append(_row_from_words(row_words, row_index))
    return structured


def _scope_table_score(rows: list[StructuredRow], text: str) -> tuple[bool, int, int]:
    if not rows:
        return False, 0, 0
    header_candidates = rows[: min(3, len(rows))]
    header_text = " ".join(row.text for row in header_candidates)
    header_tokens = set(normalize_text(header_text).split())
    scope_header_hits = len(header_tokens & SCOPE_TABLE_HEADER_KEYWORDS)
    noise_header_hits = len(header_tokens & NOISE_TABLE_HEADER_KEYWORDS)
    normalized_text = normalize_text(text)
    scope_text_hits = sum(1 for keyword in SCOPE_TABLE_HEADER_KEYWORDS if keyword in normalized_text)
    data_rows = rows[1:] if len(rows) > 1 else rows
    data_scope_rows = 0
    for row in data_rows:
        normalized_row = normalize_text(row.text)
        row_tokens = set(normalized_row.split())
        has_scope_word = bool(row_tokens & SCOPE_TABLE_HEADER_KEYWORDS)
        has_model_like_value = any(any(char.isalpha() for char in cell) and any(char.isdigit() for char in cell) for cell in row.cells)
        if has_scope_word or has_model_like_value:
            data_scope_rows += 1
    mostly_sheet_index = len(rows) >= 3 and sum(1 for row in rows if row.text.strip().upper().startswith("DT")) >= max(2, len(rows) // 2)
    if mostly_sheet_index:
        return False, scope_header_hits, noise_header_hits
    if noise_header_hits >= 2 and scope_header_hits < 3:
        return False, scope_header_hits, noise_header_hits
    is_scope = scope_header_hits >= 2 and data_scope_rows >= 1 and scope_text_hits >= 2
    return is_scope, scope_header_hits, noise_header_hits


def _word_bbox(word: tuple) -> BBox:
    return (float(word[0]), float(word[1]), float(word[2]), float(word[3]))


def _is_probably_page_border(table: fitz.table.Table, page_rect: fitz.Rect) -> bool:
    area_ratio = bbox_area(tuple(table.bbox)) / max(page_rect.width * page_rect.height, 1.0)
    return area_ratio > 0.60 and table.row_count <= 6


def _should_run_table_detection(
    page: fitz.Page,
    words: list[tuple],
    normalized_page_text: str,
    hint_hits: int,
    sheet_family: str | None,
) -> bool:
    if hint_hits == 0:
        return False
    if sheet_family in SCHEDULE_FAMILIES:
        return True
    strong_hits = sum(1 for keyword in STRONG_TABLE_HINT_KEYWORDS if keyword in normalized_page_text)
    page_area = float(page.rect.width * page.rect.height)
    if page_area >= LARGE_DRAWING_PAGE_AREA:
        return strong_hits >= 4 and len(words) <= 1200
    if len(words) > 1800 and strong_hits < 3:
        return False
    return True


def _table_regions(
    page: fitz.Page,
    words: list[tuple],
    page_index: int,
    page_text: str,
    sheet_family: str | None = None,
) -> list[Region]:
    if not hasattr(page, "find_tables"):
        return []
    normalized_page_text = normalize_text(page_text[:5000])
    hint_hits = sum(1 for keyword in TABLE_HINT_KEYWORDS if keyword in normalized_page_text)
    if hint_hits == 0 and len(words) > 900:
        return []
    if hint_hits == 0 and len(words) < 80:
        return []
    if not _should_run_table_detection(page, words, normalized_page_text, hint_hits, sheet_family):
        return []
    try:
        finder = page.find_tables() if hint_hits > 0 else page.find_tables(strategy="lines_strict")
    except Exception:  # noqa: BLE001
        return []

    regions: list[Region] = []
    for table_index, table in enumerate(finder.tables):
        bbox = tuple(float(value) for value in table.bbox)
        if _is_probably_page_border(table, page.rect):
            continue
        try:
            extracted = table.extract()
        except Exception:  # noqa: BLE001
            extracted = []

        flat_cells = [
            " ".join(part.split())
            for row in extracted
            for part in row
            if isinstance(part, str) and " ".join(part.split())
        ]
        region_text = "\n".join(flat_cells).strip()
        if not region_text:
            rows = _rows_in_bbox(words, bbox)
            region_text = "\n".join(row.text for row in rows)
        else:
            rows = _rows_in_bbox(words, bbox)

        if len(region_text) < 10:
            continue
        normalized = normalize_text(region_text)
        region_hint_hits = sum(1 for keyword in TABLE_HINT_KEYWORDS if keyword in normalized)
        is_scope_table, scope_header_hits, noise_header_hits = _scope_table_score(rows, region_text)
        kind = "bom-like" if is_scope_table else "table-like"
        confidence = min(0.98, 0.55 + (0.05 * table.row_count) + (0.03 * region_hint_hits))
        regions.append(
            Region(
                region_id=f"page-{page_index}-table-{table_index}",
                page_index=page_index,
                kind=kind,
                bbox=bbox,
                text=region_text[:5000],
                normalized_text=normalized[:5000],
                confidence=confidence,
                source="find_tables",
                rows=rows,
                metadata={
                    "row_count": table.row_count,
                    "col_count": table.col_count,
                    "table_hint_hits": region_hint_hits,
                    "scope_header_hits": scope_header_hits,
                    "noise_header_hits": noise_header_hits,
                    "table_role": "scope_table" if is_scope_table else "generic_table",
                },
            )
        )
    return regions


def _top_right_bom_regions(
    page: fitz.Page,
    words: list[tuple],
    existing_regions: list[Region],
    page_index: int,
) -> list[Region]:
    table_boxes = [region.bbox for region in existing_regions]
    scoped = []
    for word in words:
        bbox = _word_bbox(word)
        if any(bbox_contains(table_box, bbox) for table_box in table_boxes):
            continue
        scoped.append(word)
    if not scoped:
        return []

    all_rows = [_row_from_words(row_words, row_index) for row_index, row_words in enumerate(_cluster_words_to_rows(scoped))]
    all_rows = [row for row in all_rows if len(row.text) >= 2]
    if not all_rows:
        return []

    def header_score(row: StructuredRow) -> int:
        normalized_row = normalize_text(row.text)
        tokens = set(normalized_row.split())
        scope_hits = len(tokens & SCOPE_TABLE_HEADER_KEYWORDS)
        noise_hits = len(tokens & NOISE_TABLE_HEADER_KEYWORDS)
        if noise_hits >= 2 and scope_hits < 3:
            return 0
        return scope_hits

    regions: list[Region] = []
    used_row_indexes: set[int] = set()
    for header_index, header in enumerate(all_rows):
        if header.row_index in used_row_indexes:
            continue
        if header_score(header) < 2:
            continue
        header = all_rows[header_index]
        header_height = max(4.0, header.bbox[3] - header.bbox[1])
        clip_x0 = header.bbox[0] - 20.0
        clip_x1 = header.bbox[2] + 20.0
        rows = []
        last_bottom = header.bbox[3]
        for row in all_rows[header_index:]:
            clipped = _clip_row_to_x(row, clip_x0, clip_x1)
            if clipped is None:
                continue
            vertical_gap = row.bbox[1] - last_bottom
            horizontal_overlap = bbox_overlap_ratio(
                (header.bbox[0], 0.0, header.bbox[2], 100.0),
                (clipped.bbox[0], 0.0, clipped.bbox[2], 100.0),
            )
            if rows and vertical_gap > header_height * 2.8:
                break
            if horizontal_overlap < 0.20 and len(clipped.cells) < 3:
                break
            rows.append(clipped)
            last_bottom = row.bbox[3]

        text = " ".join(row.text for row in rows)
        normalized = normalize_text(text)
        hint_hits = sum(1 for keyword in TABLE_HINT_KEYWORDS if keyword in normalized)
        is_scope_table, scope_header_hits, noise_header_hits = _scope_table_score(rows, text)
        if not is_scope_table or hint_hits < 2 or len(rows) < 2 or len(normalized) < 12:
            continue
        bbox = bbox_union([row.bbox for row in rows])
        regions.append(
            Region(
                region_id=f"page-{page_index}-word-bom-{len(regions)}",
                page_index=page_index,
                kind="bom-like",
                bbox=bbox,
                text="\n".join(row.text for row in rows)[:5000],
                normalized_text=normalize_text("\n".join(row.text for row in rows))[:5000],
                confidence=min(0.94, 0.62 + (0.04 * hint_hits) + (0.02 * min(len(rows), 6))),
                source="word-scope-table",
                rows=rows,
                metadata={
                    "table_hint_hits": hint_hits,
                    "scope_header_hits": scope_header_hits,
                    "noise_header_hits": noise_header_hits,
                    "row_count": len(rows),
                    "table_role": "scope_table",
                },
            )
        )
        used_row_indexes.update(row.row_index for row in rows)
    return regions


def _classify_block_kind(bbox: BBox, text: str, page_rect: fitz.Rect) -> tuple[str, float]:
    normalized = normalize_text(text)
    keyword_hits = sum(1 for keyword in METADATA_KEYWORDS if keyword in normalized)
    bom_hits = sum(1 for keyword in TABLE_HINT_KEYWORDS if keyword in normalized)
    near_right_edge = bbox[0] > page_rect.width * 0.60
    near_top_edge = bbox[1] < page_rect.height * 0.45
    near_bottom_edge = bbox[1] > page_rect.height * 0.60
    near_edge = near_right_edge or near_bottom_edge
    if bom_hits >= 2 and near_right_edge and near_top_edge:
        return "metadata-like", min(0.88, 0.52 + (0.04 * bom_hits))
    if keyword_hits and near_edge:
        return "title-block-like", min(0.95, 0.58 + (0.06 * keyword_hits))
    if keyword_hits:
        return "metadata-like", min(0.88, 0.50 + (0.05 * keyword_hits))
    return "general", 0.40


def _merge_close_regions(regions: list[Region], page_index: int) -> list[Region]:
    if not regions:
        return []
    merged: list[Region] = []
    pending = sorted(regions, key=lambda item: (item.bbox[1], item.bbox[0]))
    while pending:
        current = pending.pop(0)
        current_boxes = [current.bbox]
        current_texts = [current.text]
        matches = [current]
        index = 0
        while index < len(pending):
            candidate = pending[index]
            horizontally_close = abs(candidate.bbox[0] - current.bbox[0]) < 80 or bbox_overlap_ratio(current.bbox, candidate.bbox) > 0
            vertically_close = abs(candidate.bbox[1] - current.bbox[3]) < 80 or abs(candidate.bbox[3] - current.bbox[1]) < 80
            if current.kind == candidate.kind and (horizontally_close or vertically_close):
                matches.append(candidate)
                current_boxes.append(candidate.bbox)
                current_texts.append(candidate.text)
                pending.pop(index)
                continue
            index += 1
        merged.append(
            Region(
                region_id=f"page-{page_index}-{current.kind}-{len(merged)}",
                page_index=page_index,
                kind=current.kind,
                bbox=bbox_union(current_boxes),
                text="\n".join(text for text in current_texts if text).strip(),
                normalized_text=normalize_text("\n".join(current_texts)),
                confidence=max(region.confidence for region in matches),
                source="merged-blocks",
                rows=[row for region in matches for row in region.rows] if current.kind == "bom-like" else [],
                metadata={"merged_count": len(matches)},
            )
        )
    return merged


def _text_regions(page: fitz.Page, blocks: list[tuple], words: list[tuple], excluded_regions: list[Region], page_index: int) -> list[Region]:
    table_boxes = [region.bbox for region in excluded_regions]
    candidate_regions: list[Region] = []
    for block_index, block in enumerate(blocks):
        bbox = (float(block[0]), float(block[1]), float(block[2]), float(block[3]))
        text = " ".join(part for part in block[4].splitlines() if part.strip()).strip()
        if len(text) < 20:
            continue
        if any(bbox_contains(table_box, bbox) for table_box in table_boxes):
            continue
        kind, confidence = _classify_block_kind(bbox, text, page.rect)
        rows = _rows_in_bbox(words, bbox) if kind == "bom-like" else []
        candidate_regions.append(
            Region(
                region_id=f"page-{page_index}-block-{block_index}",
                page_index=page_index,
                kind=kind,
                bbox=bbox,
                text=text,
                normalized_text=normalize_text(text),
                confidence=confidence,
                source="text-block",
                rows=rows,
                metadata={},
            )
        )

    grouped: list[Region] = []
    metadata_like = [region for region in candidate_regions if region.kind != "general"]
    general = [region for region in candidate_regions if region.kind == "general"]
    grouped.extend(_merge_close_regions(metadata_like, page_index))
    grouped.extend(sorted(general, key=lambda item: len(item.text), reverse=True)[:8])
    return grouped


def ocr_page_text(page: fitz.Page) -> tuple[str, bool, str | None]:
    if pytesseract is None:
        return "", False, "pytesseract not installed"
    try:
        pix = page.get_pixmap(dpi=150, alpha=False)
        image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        text = pytesseract.image_to_string(image)
        return text, True, None
    except Exception as exc:  # noqa: BLE001
        return "", True, str(exc)


def analyze_page(
    page: fitz.Page,
    page_index: int,
    *,
    cancel_requested: CancelCallback = None,
    sheet_family: str | None = None,
) -> PageAnalysis:
    _check_cancel(cancel_requested)
    native_text = page.get_text("text")
    blocks = page.get_text("blocks")
    words = page.get_text("words")
    warnings: list[str] = []
    ocr_attempted = False
    text_source = "native"
    text_for_lines = native_text

    if len(native_text.strip()) < 20:
        ocr_text, attempted, error = ocr_page_text(page)
        ocr_attempted = attempted
        if ocr_text.strip():
            text_source = "ocr"
            text_for_lines = ocr_text
        elif error:
            warnings.append(f"OCR unavailable or failed: {error}")

    table_regions = _table_regions(page, words, page_index, native_text, sheet_family=sheet_family)
    _check_cancel(cancel_requested)
    bom_word_regions = _top_right_bom_regions(page, words, table_regions, page_index)
    _check_cancel(cancel_requested)
    text_regions = _text_regions(page, blocks, words, [*table_regions, *bom_word_regions], page_index)
    lines = [line for line in (" ".join(entry.split()) for entry in text_for_lines.splitlines()) if line]

    return PageAnalysis(
        page_index=page_index,
        width=float(page.rect.width),
        height=float(page.rect.height),
        text_source=text_source,
        native_text_char_count=len(native_text.strip()),
        ocr_attempted=ocr_attempted,
        regions=[*table_regions, *bom_word_regions, *text_regions],
        plain_lines=lines[:500],
        raw_text_preview=text_for_lines[:2000],
        warnings=warnings,
    )


def analyze_document(
    pdf_path: Path,
    *,
    progress_callback: ProgressCallback = None,
    cancel_requested: CancelCallback = None,
    document_label: str | None = None,
    stage_label: str = "analyzing",
    sheet_family: str | None = None,
) -> tuple[list[PageAnalysis], list[str]]:
    page_analyses: list[PageAnalysis] = []
    exceptions: list[str] = []
    with fitz.open(pdf_path) as doc:
        for page_index, page in enumerate(doc):
            try:
                _check_cancel(cancel_requested)
                if progress_callback is not None:
                    progress_callback(
                        stage=stage_label,
                        current=page_index + 1,
                        total=doc.page_count,
                        current_file=document_label or str(pdf_path),
                        page_index=page_index,
                        page_total=doc.page_count,
                    )
                page_analyses.append(analyze_page(page, page_index, cancel_requested=cancel_requested, sheet_family=sheet_family))
            except Exception as exc:  # noqa: BLE001
                if isinstance(exc, AnalysisCancelledError):
                    raise
                exceptions.append(f"Failed to analyze page {page_index + 1} of {pdf_path.name}: {exc}")
    return page_analyses, exceptions
