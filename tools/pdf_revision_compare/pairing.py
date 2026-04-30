from __future__ import annotations

import hashlib
import itertools
import re
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Callable, Iterable

import fitz
from PIL import Image

from .models import PairCandidate, PairDecision, PdfRecord

TOKEN_RE = re.compile(r"[a-z0-9]+")
SHEET_ID_RE = re.compile(r"\b(DT\d{4})\b", re.I)
REV_RE = re.compile(r"(?:^|[\s._-])rev(?:ision)?[\s._-]*([a-z0-9]+)(?:$|[\s._-])", re.I)
VERSION_RE = re.compile(r"(?:^|[\s._-])v(?:er(?:sion)?)?[\s._-]*(\d+)(?:$|[\s._-])", re.I)
DATE_RE = re.compile(
    r"(?:(20\d{2})[-_]?([01]\d)[-_]?([0-3]\d)|([01]?\d)[-_]([0-3]?\d)[-_](20\d{2}|\d{2}))"
)
NOISE_TOKENS = {
    "rev",
    "revision",
    "version",
    "ver",
    "copy",
    "final",
    "latest",
    "new",
    "old",
    "before",
    "after",
    "issue",
}

ProgressCallback = Callable[..., None] | None
LogCallback = Callable[[str], None] | None
CancelCallback = Callable[[], bool] | None


class PairingCancelledError(RuntimeError):
    pass


def extract_sheet_id(text: str) -> str | None:
    match = SHEET_ID_RE.search(text)
    return match.group(1).upper() if match else None


def clean_sheet_title(stem: str, sheet_id: str | None, revision: str | None) -> str:
    text = stem
    if sheet_id:
        text = re.sub(re.escape(sheet_id), " ", text, count=1, flags=re.I)
    text = REV_RE.sub(" ", text)
    text = VERSION_RE.sub(" ", text)
    text = DATE_RE.sub(" ", text)
    text = re.sub(r"[_\-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" ,.-")
    return text


def classify_sheet_family(sheet_id: str | None, title: str) -> str:
    normalized_title = title.upper()
    if "SWITCH-SCHEDULES" in normalized_title or "SWITCH SCHEDULES" in normalized_title:
        return "switch_schedule"
    if "FIBER-SCHEDULES" in normalized_title or "FIBER SCHEDULES" in normalized_title:
        return "fiber_schedule"
    if "MDF" in normalized_title or "IDF" in normalized_title:
        if "SCHEDULE" in normalized_title:
            return "schedule"
        if "LAYOUT" in normalized_title or "DETAIL" in normalized_title:
            return "mdf_idf_layout"
    if not sheet_id:
        return "unknown"
    try:
        number = int(sheet_id[2:])
    except ValueError:
        return "unknown"
    if number < 1000:
        return "general_notes"
    if 1000 <= number < 2000:
        return "overall_plan"
    if 4000 <= number < 5000:
        return "area_plan"
    if 5000 <= number < 6000:
        return "detail"
    if number in {6001, 6002} or 6101 <= number <= 6109:
        return "schedule"
    if 6000 <= number < 7000:
        return "mdf_idf_layout"
    return "unknown"


def revision_status(before_revision: str | None, after_revision: str | None) -> str:
    before_value = normalized_revision_value(before_revision)
    after_value = normalized_revision_value(after_revision)
    if before_revision and after_revision and before_revision.strip().lower() == after_revision.strip().lower():
        return "same_revision"
    if before_value is not None and after_value is not None:
        if after_value > before_value:
            return "revised"
        if after_value < before_value:
            return "revision_regressed"
        return "same_revision"
    if before_revision or after_revision:
        return "revision_unknown"
    return "no_revision"


def tokenize(text: str) -> list[str]:
    return [token for token in TOKEN_RE.findall(text.lower()) if token and token not in NOISE_TOKENS]


def normalized_revision_value(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip().lower()
    if value.isdigit():
        return float(int(value))
    if len(value) == 1 and "a" <= value <= "z":
        return float(ord(value) - ord("a") + 1)
    return None


def parse_date_hints(text: str) -> list[str]:
    hints: list[str] = []
    for match in DATE_RE.finditer(text):
        if match.group(1):
            year, month, day = match.group(1), match.group(2), match.group(3)
        else:
            month, day, year = match.group(4), match.group(5), match.group(6)
            if year and len(year) == 2:
                year = f"20{year}"
        if year and month and day:
            hints.append(f"{int(year):04d}-{int(month):02d}-{int(day):02d}")
    return hints


def strip_version_tokens(stem: str) -> str:
    text = stem
    text = REV_RE.sub(" ", text)
    text = VERSION_RE.sub(" ", text)
    text = DATE_RE.sub(" ", text)
    text = re.sub(r"[_\-.]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def preview_lines(text: str, limit: int = 6) -> list[str]:
    lines: list[str] = []
    for raw in text.splitlines():
        line = " ".join(raw.split())
        if 4 <= len(line) <= 120 and line not in lines:
            lines.append(line)
        if len(lines) >= limit:
            break
    return lines


def average_hash(page: fitz.Page, dpi: int = 24) -> str:
    pix = page.get_pixmap(dpi=dpi, alpha=False)
    image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples).convert("L").resize((8, 8))
    values = list(image.getdata())
    average = sum(values) / max(len(values), 1)
    bits = "".join("1" if value >= average else "0" for value in values)
    return f"{int(bits, 2):016x}"


def hamming_similarity(hash_a: str, hash_b: str) -> float:
    if not hash_a or not hash_b:
        return 0.0
    bits = max(len(hash_a), len(hash_b)) * 4
    distance = bin(int(hash_a, 16) ^ int(hash_b, 16)).count("1")
    return max(0.0, 1.0 - (distance / max(bits, 1)))


def _title_similarity(a: PdfRecord, b: PdfRecord) -> float:
    return SequenceMatcher(None, a.title_hint.lower(), b.title_hint.lower()).ratio()


def _token_similarity(tokens_a: Iterable[str], tokens_b: Iterable[str]) -> float:
    set_a = set(tokens_a)
    set_b = set(tokens_b)
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def _page_size_similarity(a: PdfRecord, b: PdfRecord) -> float:
    if not a.page_sizes or not b.page_sizes:
        return 0.0
    aw, ah = a.page_sizes[0]
    bw, bh = b.page_sizes[0]
    delta = abs(aw - bw) + abs(ah - bh)
    return max(0.0, 1.0 - (delta / max(aw + ah + bw + bh, 1.0)))


def _file_stem_similarity(a: PdfRecord, b: PdfRecord) -> float:
    return SequenceMatcher(None, a.normalized_stem, b.normalized_stem).ratio()


def infer_direction(a: PdfRecord, b: PdfRecord) -> tuple[PdfRecord, PdfRecord, str] | None:
    rev_a = normalized_revision_value(a.revision_hint)
    rev_b = normalized_revision_value(b.revision_hint)
    if rev_a is not None and rev_b is not None and rev_a != rev_b:
        return (a, b, "revision-order") if rev_a < rev_b else (b, a, "revision-order")

    if a.version_hint is not None and b.version_hint is not None and a.version_hint != b.version_hint:
        return (a, b, "version-order") if a.version_hint < b.version_hint else (b, a, "version-order")

    if a.date_hints and b.date_hints:
        latest_a = max(a.date_hints)
        latest_b = max(b.date_hints)
        if latest_a != latest_b:
            return (a, b, "date-order") if latest_a < latest_b else (b, a, "date-order")

    if abs(a.modified_at - b.modified_at) > 3600:
        return (a, b, "modified-time") if a.modified_at < b.modified_at else (b, a, "modified-time")
    return None


def build_pdf_record(pdf_path: Path, source_group: str) -> PdfRecord:
    warnings: list[str] = []
    with fitz.open(pdf_path) as doc:
        page_sizes = [(float(page.rect.width), float(page.rect.height)) for page in doc]
        first_page = doc[0]
        first_page_text = first_page.get_text("text")
        native_text_char_count = sum(len(page.get_text("text").strip()) for page in doc)
        title_hint = " | ".join(preview_lines(first_page_text, limit=5))
        fingerprint = average_hash(first_page)

    stat = pdf_path.stat()
    stem = pdf_path.stem
    normalized_stem = strip_version_tokens(stem).lower()
    revision_match = REV_RE.search(stem)
    revision = revision_match.group(1).upper() if revision_match else None
    version_match = VERSION_RE.search(stem)
    sheet_id = extract_sheet_id(stem)
    sheet_title = clean_sheet_title(stem, sheet_id, revision)
    family = classify_sheet_family(sheet_id, sheet_title)
    combined_text = f"{stem} {title_hint}"

    return PdfRecord(
        path=pdf_path,
        source_group=source_group,
        page_count=len(page_sizes),
        page_sizes=page_sizes,
        file_size=stat.st_size,
        modified_at=stat.st_mtime,
        filename_stem=stem,
        normalized_stem=normalized_stem,
        filename_tokens=tokenize(normalized_stem),
        title_hint=title_hint,
        title_tokens=tokenize(title_hint),
        revision_hint=revision,
        version_hint=int(version_match.group(1)) if version_match else None,
        date_hints=parse_date_hints(combined_text),
        first_page_fingerprint=fingerprint,
        first_page_text_preview=title_hint[:500],
        native_text_char_count=native_text_char_count,
        sheet_id=sheet_id,
        revision=revision,
        sheet_title=sheet_title,
        family=family,
        phase=source_group,
        warnings=warnings,
    )


def build_failed_pdf_record(pdf_path: Path, source_group: str, message: str) -> PdfRecord:
    stat = pdf_path.stat()
    stem = pdf_path.stem
    normalized_stem = strip_version_tokens(stem).lower()
    revision_match = REV_RE.search(stem)
    revision = revision_match.group(1).upper() if revision_match else None
    version_match = VERSION_RE.search(stem)
    sheet_id = extract_sheet_id(stem)
    sheet_title = clean_sheet_title(stem, sheet_id, revision)
    family = classify_sheet_family(sheet_id, sheet_title)
    return PdfRecord(
        path=pdf_path,
        source_group=source_group,
        page_count=0,
        page_sizes=[],
        file_size=stat.st_size,
        modified_at=stat.st_mtime,
        filename_stem=stem,
        normalized_stem=normalized_stem,
        filename_tokens=tokenize(normalized_stem),
        title_hint=sheet_title,
        title_tokens=tokenize(sheet_title),
        revision_hint=revision,
        version_hint=int(version_match.group(1)) if version_match else None,
        date_hints=parse_date_hints(stem),
        first_page_fingerprint="",
        first_page_text_preview="",
        native_text_char_count=0,
        sheet_id=sheet_id,
        revision=revision,
        sheet_title=sheet_title,
        family=family,
        phase=source_group,
        warnings=[f"scan failed: {message}"],
    )


def record_needs_scan_review(record: PdfRecord) -> bool:
    return record.page_count <= 0 or any("scan failed:" in warning for warning in record.warnings)


def _check_cancel(cancel_requested: CancelCallback) -> None:
    if cancel_requested is not None and cancel_requested():
        raise PairingCancelledError("Compare job stopped by user.")


def scan_pdf_records(
    folder: Path,
    source_group: str,
    *,
    progress_callback: ProgressCallback = None,
    log_callback: LogCallback = None,
    cancel_requested: CancelCallback = None,
    progress_offset: int = 0,
    progress_total: int | None = None,
) -> tuple[list[PdfRecord], list[str]]:
    records: list[PdfRecord] = []
    exceptions: list[str] = []
    pdf_paths = sorted(folder.rglob("*.pdf"))
    total = progress_total or len(pdf_paths)
    for index, pdf_path in enumerate(pdf_paths, start=1):
        _check_cancel(cancel_requested)
        try:
            records.append(build_pdf_record(pdf_path, source_group))
            if progress_callback is not None:
                progress_callback(
                    stage="scan",
                    current=progress_offset + index,
                    total=total,
                    current_file=str(pdf_path),
                    source_group=source_group,
                )
            if log_callback is not None and (index == 1 or index == len(pdf_paths) or index % 10 == 0):
                log_callback(f"Scanned {progress_offset + index}/{total}: {pdf_path.name}")
        except Exception as exc:  # noqa: BLE001
            message = f"Failed to read {pdf_path}: {exc}"
            exceptions.append(message)
            records.append(build_failed_pdf_record(pdf_path, source_group, str(exc)))
            if progress_callback is not None:
                progress_callback(
                    stage="scan",
                    current=progress_offset + index,
                    total=total,
                    current_file=str(pdf_path),
                    source_group=source_group,
                )
    return records, exceptions


def _candidate_score(before: PdfRecord, after: PdfRecord) -> PairCandidate:
    reasons: list[str] = []
    score = 0.0

    stem_similarity = _file_stem_similarity(before, after)
    if stem_similarity > 0.98:
        score += 0.40
        reasons.append("normalized stem exact/near-exact")
    else:
        score += stem_similarity * 0.25

    file_token_similarity = _token_similarity(before.filename_tokens, after.filename_tokens)
    title_token_similarity = _token_similarity(before.title_tokens, after.title_tokens)
    token_similarity = max(file_token_similarity, title_token_similarity)
    score += token_similarity * 0.20
    if token_similarity > 0.5:
        reasons.append("filename/title token overlap")

    title_similarity = _title_similarity(before, after)
    score += title_similarity * 0.10

    if before.page_count == after.page_count:
        score += 0.10
        reasons.append("page count match")
    else:
        score += max(0.0, 0.1 - (abs(before.page_count - after.page_count) * 0.03))

    size_similarity = _page_size_similarity(before, after)
    score += size_similarity * 0.05
    if size_similarity > 0.98:
        reasons.append("page size match")

    hash_similarity = hamming_similarity(before.first_page_fingerprint, after.first_page_fingerprint)
    score += hash_similarity * 0.10
    if hash_similarity > 0.75:
        reasons.append("first-page fingerprint similarity")

    text_preview_similarity = SequenceMatcher(
        None,
        before.first_page_text_preview.lower(),
        after.first_page_text_preview.lower(),
    ).ratio()
    score += text_preview_similarity * 0.05

    if before.revision_hint and after.revision_hint and before.revision_hint != after.revision_hint:
        score += 0.05
        reasons.append("revision clues present")
    elif before.version_hint is not None and after.version_hint is not None and before.version_hint != after.version_hint:
        score += 0.05
        reasons.append("version clues present")

    confidence = max(0.0, min(score, 1.0))
    return PairCandidate(
        before=before,
        after=after,
        score=score,
        confidence=confidence,
        reasons=reasons,
        pairing_method="scored-match",
    )


def _top_competitors(candidates: list[PairCandidate], anchor: PdfRecord, mode: str) -> list[PairCandidate]:
    if mode == "before":
        related = [candidate for candidate in candidates if candidate.before.path == anchor.path]
    else:
        related = [candidate for candidate in candidates if candidate.after.path == anchor.path]
    return sorted(related, key=lambda item: item.score, reverse=True)


def _decide_match(
    candidate: PairCandidate,
    before_candidates: list[PairCandidate],
    after_candidates: list[PairCandidate],
    threshold: float = 0.56,
    ambiguity_margin: float = 0.08,
) -> PairDecision:
    reasons = list(candidate.reasons)
    competing: list[dict[str, object]] = []

    top_before = before_candidates[:2]
    top_after = after_candidates[:2]

    for entry in itertools.chain(top_before, top_after):
        if entry is candidate:
            continue
        competing.append(
            {
                "before": str(entry.before.path),
                "after": str(entry.after.path),
                "score": round(entry.score, 3),
            }
        )

    second_best_score = 0.0
    if len(top_before) > 1:
        second_best_score = max(second_best_score, top_before[1].score)
    if len(top_after) > 1:
        second_best_score = max(second_best_score, top_after[1].score)

    if candidate.confidence < threshold:
        reasons.append("confidence below auto-pair threshold")
        return PairDecision(
            status="review_needed",
            confidence=candidate.confidence,
            pairing_method="low-confidence",
            before=candidate.before,
            after=candidate.after,
            reasons=reasons,
            competing_candidates=competing,
        )

    if second_best_score and (candidate.score - second_best_score) < ambiguity_margin:
        reasons.append("competing candidate too close")
        return PairDecision(
            status="review_needed",
            confidence=candidate.confidence,
            pairing_method="ambiguous-match",
            before=candidate.before,
            after=candidate.after,
            reasons=reasons,
            competing_candidates=competing,
        )

    return PairDecision(
        status="matched",
        confidence=candidate.confidence,
        pairing_method=candidate.pairing_method,
        before=candidate.before,
        after=candidate.after,
        reasons=reasons,
        competing_candidates=competing,
    )


def _pair_two_groups_scored(
    before_records: list[PdfRecord],
    after_records: list[PdfRecord],
    *,
    progress_callback: ProgressCallback = None,
    cancel_requested: CancelCallback = None,
) -> list[PairDecision]:
    candidates = [
        _candidate_score(before_record, after_record)
        for before_record in before_records
        for after_record in after_records
        if _token_similarity(before_record.filename_tokens, after_record.filename_tokens) > 0
        or _file_stem_similarity(before_record, after_record) > 0.45
        or hamming_similarity(before_record.first_page_fingerprint, after_record.first_page_fingerprint) > 0.60
    ]
    candidates.sort(key=lambda item: item.score, reverse=True)

    used_before: set[Path] = set()
    used_after: set[Path] = set()
    decisions: list[PairDecision] = []

    for index, candidate in enumerate(candidates, start=1):
        _check_cancel(cancel_requested)
        if progress_callback is not None and (index == 1 or index % 250 == 0 or index == len(candidates)):
            progress_callback(
                stage="pairing",
                current=index,
                total=max(len(candidates), 1),
                current_file=f"{candidate.before.path.name} -> {candidate.after.path.name}",
            )
        if candidate.before.path in used_before or candidate.after.path in used_after:
            continue
        decision = _decide_match(
            candidate,
            _top_competitors(candidates, candidate.before, "before"),
            _top_competitors(candidates, candidate.after, "after"),
        )
        decisions.append(decision)
        used_before.add(candidate.before.path)
        used_after.add(candidate.after.path)

    for record in before_records:
        if record.path not in used_before:
            decisions.append(
                PairDecision(
                    status="removed",
                    confidence=1.0,
                    pairing_method="unmatched-before",
                    before=record,
                    sheet_id=record.sheet_id,
                    family=record.family,
                    revision_status="removed",
                    reasons=["file only found in IFB/before folder"],
                )
            )

    for record in after_records:
        if record.path not in used_after:
            decisions.append(
                PairDecision(
                    status="added",
                    confidence=1.0,
                    pairing_method="unmatched-after",
                    after=record,
                    sheet_id=record.sheet_id,
                    family=record.family,
                    revision_status="added",
                    reasons=["file only found in GMP/after folder"],
                )
            )

    return decisions


def pair_two_groups(
    before_records: list[PdfRecord],
    after_records: list[PdfRecord],
    *,
    progress_callback: ProgressCallback = None,
    log_callback: LogCallback = None,
    cancel_requested: CancelCallback = None,
) -> list[PairDecision]:
    keyed_before: dict[str, list[PdfRecord]] = defaultdict(list)
    keyed_after: dict[str, list[PdfRecord]] = defaultdict(list)
    invalid_before: list[PdfRecord] = []
    invalid_after: list[PdfRecord] = []
    failed_before: list[PdfRecord] = []
    failed_after: list[PdfRecord] = []
    for record in before_records:
        if record_needs_scan_review(record):
            failed_before.append(record)
        elif record.sheet_id:
            keyed_before[record.sheet_id].append(record)
        else:
            invalid_before.append(record)
    for record in after_records:
        if record_needs_scan_review(record):
            failed_after.append(record)
        elif record.sheet_id:
            keyed_after[record.sheet_id].append(record)
        else:
            invalid_after.append(record)

    if not keyed_before and not keyed_after:
        if failed_before or failed_after:
            decisions = _pair_two_groups_scored(
                invalid_before,
                invalid_after,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            for record in [*failed_before, *failed_after]:
                decisions.append(
                    PairDecision(
                        status="review_needed",
                        confidence=0.0,
                        pairing_method="scan-failed",
                        before=record if record in failed_before else None,
                        after=record if record in failed_after else None,
                        sheet_id=record.sheet_id,
                        family=record.family,
                        revision_status="review_needed",
                        reasons=record.warnings or ["PDF could not be read during scan"],
                    )
                )
            return decisions
        if log_callback is not None:
            log_callback("No DT#### sheet IDs found; falling back to scored before/after pairing.")
        return _pair_two_groups_scored(
            before_records,
            after_records,
            progress_callback=progress_callback,
            cancel_requested=cancel_requested,
        )

    sheet_ids = sorted(set(keyed_before) | set(keyed_after))
    total = max(len(sheet_ids) + len(invalid_before) + len(invalid_after) + len(failed_before) + len(failed_after), 1)
    current = 0
    decisions: list[PairDecision] = []

    def emit(current_file: str) -> None:
        if progress_callback is not None:
            progress_callback(stage="pairing", current=current, total=total, current_file=current_file)

    for sheet_id in sheet_ids:
        _check_cancel(cancel_requested)
        current += 1
        before_group = keyed_before.get(sheet_id, [])
        after_group = keyed_after.get(sheet_id, [])
        emit(sheet_id)
        if len(before_group) == 1 and len(after_group) == 1:
            before = before_group[0]
            after = after_group[0]
            rev_status = revision_status(before.revision, after.revision)
            is_regressed = rev_status == "revision_regressed"
            decisions.append(
                PairDecision(
                    status="review_needed" if is_regressed else "matched",
                    confidence=0.4 if is_regressed else 1.0,
                    pairing_method="sheet-id-revision-regressed" if is_regressed else "sheet-id",
                    before=before,
                    after=after,
                    sheet_id=sheet_id,
                    family=after.family or before.family,
                    revision_status=rev_status,
                    reasons=[
                        f"matched deterministic sheet id {sheet_id}",
                        f"revision status: {rev_status}",
                        "GMP revision appears older than IFB; manual review needed",
                    ] if is_regressed else [f"matched deterministic sheet id {sheet_id}", f"revision status: {rev_status}"],
                )
            )
            continue
        if len(before_group) == 0 and len(after_group) == 1:
            after = after_group[0]
            decisions.append(
                PairDecision(
                    status="added",
                    confidence=1.0,
                    pairing_method="sheet-id-added",
                    after=after,
                    sheet_id=sheet_id,
                    family=after.family,
                    revision_status="added",
                    reasons=["sheet only found in GMP folder"],
                )
            )
            continue
        if len(before_group) == 1 and len(after_group) == 0:
            before = before_group[0]
            decisions.append(
                PairDecision(
                    status="removed",
                    confidence=1.0,
                    pairing_method="sheet-id-removed",
                    before=before,
                    sheet_id=sheet_id,
                    family=before.family,
                    revision_status="removed",
                    reasons=["sheet only found in IFB folder"],
                )
            )
            continue

        reasons = [
            f"duplicate or conflicting sheet id {sheet_id}",
            f"IFB records: {len(before_group)}",
            f"GMP records: {len(after_group)}",
        ]
        max_len = max(len(before_group), len(after_group))
        for index in range(max_len):
            before = before_group[index] if index < len(before_group) else None
            after = after_group[index] if index < len(after_group) else None
            decisions.append(
                PairDecision(
                    status="review_needed",
                    confidence=0.0,
                    pairing_method="sheet-id-conflict",
                    before=before,
                    after=after,
                    sheet_id=sheet_id,
                    family=(after.family if after else before.family if before else "unknown"),
                    revision_status="review_needed",
                    reasons=reasons,
                )
            )

    for record in [*invalid_before, *invalid_after, *failed_before, *failed_after]:
        _check_cancel(cancel_requested)
        current += 1
        emit(record.path.name)
        failed = record_needs_scan_review(record)
        decisions.append(
            PairDecision(
                status="review_needed",
                confidence=0.0,
                pairing_method="scan-failed" if failed else "sheet-id-parse-failed",
                before=record if record in [*invalid_before, *failed_before] else None,
                after=record if record in [*invalid_after, *failed_after] else None,
                sheet_id=record.sheet_id if failed else None,
                family=record.family,
                revision_status="review_needed",
                reasons=record.warnings if failed else ["could not extract DT#### sheet id from filename"],
            )
        )

    if log_callback is not None:
        log_callback("Paired IFB/GMP folders by deterministic DT#### sheet id.")
    return decisions


def pair_mixed_group(
    records: list[PdfRecord],
    *,
    progress_callback: ProgressCallback = None,
    log_callback: LogCallback = None,
    cancel_requested: CancelCallback = None,
) -> list[PairDecision]:
    candidates: list[PairCandidate] = []
    combinations = list(itertools.combinations(records, 2))
    for index, (left, right) in enumerate(combinations, start=1):
        _check_cancel(cancel_requested)
        if progress_callback is not None and (index == 1 or index % 250 == 0 or index == len(combinations)):
            progress_callback(
                stage="pairing",
                current=index,
                total=max(len(combinations), 1),
                current_file=f"{left.path.name} <> {right.path.name}",
            )
        if _token_similarity(left.filename_tokens, right.filename_tokens) <= 0 and _file_stem_similarity(left, right) < 0.45:
            continue
        direction = infer_direction(left, right)
        if direction is None:
            continue
        before, after, method = direction
        candidate = _candidate_score(before, after)
        candidate.pairing_method = f"mixed-{method}"
        candidates.append(candidate)

    candidates.sort(key=lambda item: item.score, reverse=True)
    used_paths: set[Path] = set()
    decisions: list[PairDecision] = []

    for candidate in candidates:
        _check_cancel(cancel_requested)
        if candidate.before.path in used_paths or candidate.after.path in used_paths:
            continue
        decision = _decide_match(
            candidate,
            _top_competitors(candidates, candidate.before, "before"),
            _top_competitors(candidates, candidate.after, "after"),
        )
        decisions.append(decision)
        used_paths.add(candidate.before.path)
        used_paths.add(candidate.after.path)

    for record in records:
        if record.path not in used_paths:
            decisions.append(
                PairDecision(
                    status="review_needed",
                    confidence=0.0,
                    pairing_method="mixed-unpaired",
                    before=record if record.revision_hint or record.version_hint else None,
                    after=None if record.revision_hint or record.version_hint else record,
                    reasons=["unable to infer a confident before/after pair from mixed folder"],
                )
            )

    return decisions


@dataclass(slots=True)
class PairingScanResult:
    records: list[PdfRecord]
    decisions: list[PairDecision]
    exceptions: list[str]


def scan_and_pair(
    input_paths: list[Path],
    *,
    progress_callback: ProgressCallback = None,
    log_callback: LogCallback = None,
    cancel_requested: CancelCallback = None,
) -> PairingScanResult:
    if len(input_paths) not in {1, 2}:
        raise ValueError("scan_and_pair expects one mixed folder or exactly two IFB/GMP folders")
    if len(input_paths) == 1:
        paths = sorted(input_paths[0].rglob("*.pdf"))
        records, exceptions = scan_pdf_records(
            input_paths[0],
            "mixed",
            progress_callback=progress_callback,
            log_callback=log_callback,
            cancel_requested=cancel_requested,
            progress_total=len(paths),
        )
        if log_callback is not None:
            log_callback("Scoring mixed-folder candidates...")
        decisions = pair_mixed_group(
            records,
            progress_callback=progress_callback,
            log_callback=log_callback,
            cancel_requested=cancel_requested,
        )
        return PairingScanResult(records=records, decisions=decisions, exceptions=exceptions)

    before_paths = sorted(input_paths[0].rglob("*.pdf"))
    after_paths = sorted(input_paths[1].rglob("*.pdf"))
    total = len(before_paths) + len(after_paths)
    before_records, before_exceptions = scan_pdf_records(
        input_paths[0],
        "IFB",
        progress_callback=progress_callback,
        log_callback=log_callback,
        cancel_requested=cancel_requested,
        progress_offset=0,
        progress_total=total,
    )
    after_records, after_exceptions = scan_pdf_records(
        input_paths[1],
        "GMP",
        progress_callback=progress_callback,
        log_callback=log_callback,
        cancel_requested=cancel_requested,
        progress_offset=len(before_paths),
        progress_total=total,
    )
    if log_callback is not None:
        log_callback("Pairing IFB/GMP sheets by DT####...")
    decisions = pair_two_groups(
        before_records,
        after_records,
        progress_callback=progress_callback,
        log_callback=log_callback,
        cancel_requested=cancel_requested,
    )
    return PairingScanResult(
        records=[*before_records, *after_records],
        decisions=decisions,
        exceptions=[*before_exceptions, *after_exceptions],
    )
