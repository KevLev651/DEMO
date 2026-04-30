"""OCR routing and diagnostics for P&ID PDF pages."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OcrOptions:
    mode: str = "auto"  # auto | always | off
    dpi: int = 300


@dataclass
class PageOcrDecision:
    requested_mode: str
    needs_ocr: bool
    should_ocr: bool
    used_ocr: bool = False
    backend: str = ""
    reason: str = ""
    warning: str = ""
    native_word_count: int = 0
    native_char_count: int = 0
    native_span_count: int = 0
    drawing_count: int = 0
    image_count: int = 0
    image_coverage: float = 0.0
    vector_suspicion: bool = False

    @property
    def page_ocr_mode(self):
        if self.used_ocr:
            return "ocr"
        if self.warning and self.needs_ocr:
            return "ocr-unavailable"
        return "native"


def _image_coverage(page):
    """Approximate image coverage in display coordinates."""
    page_area = max(float(page.rect.width * page.rect.height), 1.0)
    rects = []
    seen = set()
    for image in page.get_images(full=True):
        xref = image[0]
        try:
            img_rects = page.get_image_rects(xref)
        except Exception:
            img_rects = []
        for rect in img_rects:
            r = rect * page.rotation_matrix
            r.normalize()
            key = (round(r.x0), round(r.y0), round(r.x1), round(r.y1))
            if key in seen:
                continue
            seen.add(key)
            rects.append(r)
    area = sum(max(0.0, r.width) * max(0.0, r.height) for r in rects)
    return min(1.0, area / page_area)


def analyze_page_for_ocr(page, options=None, native_span_count=0):
    """Decide whether OCR should run for a page.

    The scanner stays native-first. OCR is routed in only for forced mode or
    when page-level diagnostics indicate raster/mixed content with weak text.
    """
    options = options or OcrOptions()
    mode = (options.mode or "auto").lower()
    if mode not in {"auto", "always", "off"}:
        raise ValueError("ocr mode must be one of: auto, always, off")

    try:
        words = page.get_text("words")
    except Exception:
        words = []
    try:
        text = page.get_text("text").strip()
    except Exception:
        text = ""
    try:
        drawings = page.get_drawings()
    except Exception:
        drawings = []
    try:
        images = page.get_images(full=True)
    except Exception:
        images = []
    coverage = _image_coverage(page) if images else 0.0

    word_count = len(words)
    char_count = len(text)
    drawing_count = len(drawings)
    image_count = len(images)
    weak_text = word_count < 10 or char_count < 40 or native_span_count < 5
    image_heavy = coverage >= 0.30 or (image_count > 0 and weak_text)
    mixed_sparse = image_count > 0 and native_span_count < 40 and drawing_count > 15
    vector_suspicion = drawing_count > 600 and word_count < 35

    if mode == "off":
        return PageOcrDecision(
            requested_mode=mode,
            needs_ocr=False,
            should_ocr=False,
            backend="",
            reason="disabled by --ocr off",
            native_word_count=word_count,
            native_char_count=char_count,
            native_span_count=native_span_count,
            drawing_count=drawing_count,
            image_count=image_count,
            image_coverage=coverage,
            vector_suspicion=vector_suspicion,
        )

    if mode == "always":
        reason = "forced by --ocr always"
        needs = True
    elif image_heavy:
        reason = "image-heavy or image-only page with weak native text"
        needs = True
    elif mixed_sparse:
        reason = "mixed raster/vector page with sparse native text"
        needs = True
    elif vector_suspicion:
        reason = "dense vector page with sparse native text"
        needs = True
    else:
        reason = "native text sufficient"
        needs = False

    return PageOcrDecision(
        requested_mode=mode,
        needs_ocr=needs,
        should_ocr=needs,
        backend="pymupdf" if needs else "",
        reason=reason,
        native_word_count=word_count,
        native_char_count=char_count,
        native_span_count=native_span_count,
        drawing_count=drawing_count,
        image_count=image_count,
        image_coverage=coverage,
        vector_suspicion=vector_suspicion,
    )


def get_ocr_textpage(page, decision, options=None):
    """Return a PyMuPDF OCR textpage if available; never raise for OCR failure."""
    options = options or OcrOptions()
    if not decision.should_ocr:
        return None
    try:
        textpage = page.get_textpage_ocr(full=True, dpi=int(options.dpi or 300))
    except Exception as exc:
        decision.used_ocr = False
        decision.warning = f"OCR unavailable: {exc}"
        return None
    decision.used_ocr = True
    decision.backend = "pymupdf"
    return textpage
