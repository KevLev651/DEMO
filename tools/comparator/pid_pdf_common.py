"""Universal P&ID tag extraction for born-digital and mixed-raster PDFs.

Design goals:
- Work across multiple P&ID styles (any rotation, any tag convention).
- Use spatial geometry (derotated to display frame) for parent/child pairing,
  not line-order within a text block.
- Split output into four classes: instrument, equipment, line, spec.
- Evidence-based confidence.

Dependencies: PyMuPDF (fitz), openpyxl (only in scanner/comparator).
"""

import os
import re
from dataclasses import dataclass, field
from statistics import mean

import fitz

from .pid_ocr import OcrOptions, analyze_page_for_ocr, get_ocr_textpage
from .pid_profiles import DEFAULT_PROFILE
from .pid_text_graph import build_text_graph


# Runtime policy is profile-backed. Customer/job language belongs in
# pid_profiles.py, while this module keeps parser mechanics and geometry.
ACTIVE_PROFILE = DEFAULT_PROFILE
ISA_INSTRUMENT_CODES = ACTIVE_PROFILE.isa_instrument_codes
EQUIPMENT_PREFIXES = ACTIVE_PROFILE.equipment_prefixes
DENY_PREFIXES = ACTIVE_PROFILE.deny_prefixes
LEGEND_MARKERS = ACTIVE_PROFILE.legend_markers
NOISE_CONTEXT = ACTIVE_PROFILE.noise_context
LINE_NOISE_WORDS = ACTIVE_PROFILE.line_noise_words
LINE_WORD_COMPONENTS = ACTIVE_PROFILE.line_word_components
BARE_INFER_CODES = ACTIVE_PROFILE.bare_infer_codes
CUSTOM_REVIEW_INSTRUMENT_CODES = ACTIVE_PROFILE.custom_review_instrument_codes


# ---------------------------------------------------------------------------
# Regexes
# ---------------------------------------------------------------------------

# Line tag: SIZE"-SERVICE-NUMBER[-SPEC] where SIZE is fraction/mixed like 3/4, 1.5, 1 3/8
# Example matches: 3/4"-CA-12A, 1.5"-CWS-5H-EL-1.5", 1 3/8"-FRE-18A-2"
LINE_TAG_RE = re.compile(ACTIVE_PROFILE.line_tag_pattern)

# Asset tag: alpha prefix + digits + optional suffix. Permissive; validated by prefix list.
ASSET_TAG_RE = re.compile(ACTIVE_PROFILE.asset_tag_pattern)

# Hyphenated root (like RL7-TNK-102, RL7-5000, RL7-GRR-103)
HYPHEN_ROOT_RE = re.compile(ACTIVE_PROFILE.hyphen_root_pattern)
COMPACT_HYPHEN_ROOT_RE = re.compile(ACTIVE_PROFILE.compact_hyphen_root_pattern)

# ISA instrument child: {code}[-]?{number}[A-Z]?
ISA_CHILD_RE = re.compile(ACTIVE_PROFILE.isa_child_pattern)

# Equipment child (component of a skid, named like CHR001, BLW001)
EQUIP_CHILD_RE = re.compile(ACTIVE_PROFILE.equip_child_pattern)

# Full hyphenated tag (catches pre-joined tags in one span like "RL7-CKR-101-TCV-01")
FULL_HYPHEN_TAG_RE = re.compile(ACTIVE_PROFILE.full_hyphen_tag_pattern)

# Drawing filename reference in title block
DWG_FILE_RE = re.compile(ACTIVE_PROFILE.dwg_file_pattern, re.IGNORECASE)

# Sheet ID in title-block, profile-defined grammar
SHEET_ID_RE = re.compile(ACTIVE_PROFILE.sheet_id_pattern)

PIPE_SIZE_CLEAN_RE = re.compile(
    r'^\s*(?:\d+\s+\d+/\d+|\d+/\d+|\d+(?:\.\d+)?)\s*"?\s*-\s*'
)


def normalize_token(value):
    """Uppercase, strip unicode dashes/whitespace/punctuation, no pipe-size prefix."""
    s = str(value or "").strip().upper()
    for dash in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014"):
        s = s.replace(dash, "-")
    s = s.replace("\u00a0", " ").replace("\t", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = s.strip('.,;:()[]{}<>"\'')
    return s


def strip_pipe_size(value):
    """Remove leading pipe size from a line tag: '3/4\"-CA-12A' -> 'CA-12A'."""
    s = str(value or "").strip()
    s = PIPE_SIZE_CLEAN_RE.sub("", s).strip()
    return s.strip('"').strip()


def normalize_pipe_size(value):
    """Normalize a pipe size token and keep mixed fractions readable."""
    s = str(value or "").strip().strip('"')
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s*/\s*", "/", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def canonical_line_tag(size, body):
    """Return the canonical report identity for a line tag.

    Keep pipe size plus service-number, but drop trailing spec fragments such
    as -EL-1. This keeps line lists useful without making spec text the key.
    """
    parts = normalize_token(body).split("-")
    if len(parts) < 2:
        return "", "", ""
    pipe_size = normalize_pipe_size(size)
    root = parts[0]
    component = parts[1]
    full = f'{pipe_size}"-{root}-{component}' if pipe_size else f"{root}-{component}"
    return full, root, component


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Span:
    """A single text span in DISPLAY coordinates (post-rotation)."""
    id: int
    text: str          # original text
    norm: str          # normalized uppercase
    x0: float
    y0: float
    x1: float
    y1: float
    size: float        # font size
    source: str = "native"
    node_id: str = ""
    ocr_confidence: float | None = None

    @property
    def width(self):
        return self.x1 - self.x0

    @property
    def height(self):
        return self.y1 - self.y0

    @property
    def cx(self):
        return (self.x0 + self.x1) / 2

    @property
    def cy(self):
        return (self.y0 + self.y1) / 2


@dataclass
class Tag:
    full_tag: str
    root: str
    component: str
    tag_class: str       # instrument | equipment | line | spec
    method: str          # how detected
    source_spans: tuple  # span ids contributing
    x0: float
    y0: float
    x1: float
    y1: float
    source_text: str
    confidence: float = 0.0
    rating: str = ""
    zone: str = ""
    evidence: str = ""
    sheet_id: str = ""
    page_num: int = 0
    text_source: str = "native"
    source_agreement: str = "native-only"
    ocr_confidence: float | None = None
    page_ocr_mode: str = "native"
    ocr_backend: str = ""
    ocr_reason: str = ""
    source_node_ids: str = ""
    source_node_count: int = 0
    source_rects: tuple = ()


@dataclass(frozen=True)
class PageProfile:
    page_num: int
    sheet_id: str
    page_role: str
    rotation: int
    native_size: tuple   # (width, height)
    display_size: tuple
    span_count: int
    word_count: int
    drawing_count: int
    image_count: int
    needs_ocr: bool
    used_ocr: bool
    ocr_warning: str
    native_span_count: int = 0
    ocr_span_count: int = 0
    native_word_count: int = 0
    ocr_word_count: int = 0
    image_coverage: float = 0.0
    vector_suspicion: bool = False
    ocr_mode: str = "native"
    ocr_backend: str = ""
    ocr_reason: str = ""
    ocr_dpi: int = 0
    graph_node_count: int = 0
    graph_group_count: int = 0
    rejected_text_count: int = 0
    text_tokens: tuple = field(default_factory=tuple)
    text_groups: tuple = field(default_factory=tuple)
    rejected_text: tuple = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Coordinate normalization — always work in DISPLAY frame after this step.
# ---------------------------------------------------------------------------

def extract_display_spans(page, textpage=None, source="native", id_prefix=None):
    """Extract all text spans from a page, with bboxes mapped to the DISPLAY
    (post-rotation) coordinate frame.

    PyMuPDF's get_text('dict') returns bboxes in the page's native coord system.
    For a rotated page (rotation != 0) we must apply page.rotation_matrix to
    get the coords a human viewer sees.
    """
    d = page.get_text("dict", textpage=textpage) if textpage else page.get_text("dict")
    rotation_matrix = page.rotation_matrix  # native -> display
    spans = []
    sid = 0
    for block in d.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                text = span.get("text", "").strip()
                if not text:
                    continue
                native = fitz.Rect(span["bbox"])
                display = native * rotation_matrix  # transform to viewer frame
                display.normalize()
                spans.append(Span(
                    id=sid,
                    text=text,
                    norm=normalize_token(text),
                    x0=display.x0,
                    y0=display.y0,
                    x1=display.x1,
                    y1=display.y1,
                    size=float(span.get("size", 0.0)),
                    source=source,
                    node_id=f"{id_prefix or source}:{sid}",
                ))
                sid += 1
    return spans


# ---------------------------------------------------------------------------
# Span utilities
# ---------------------------------------------------------------------------

def x_overlap(a, b):
    """Horizontal overlap fraction relative to the smaller span."""
    o = max(0.0, min(a.x1, b.x1) - max(a.x0, b.x0))
    denom = min(a.width, b.width)
    return o / denom if denom > 0 else 0.0


def y_overlap(a, b):
    o = max(0.0, min(a.y1, b.y1) - max(a.y0, b.y0))
    denom = min(a.height, b.height)
    return o / denom if denom > 0 else 0.0


def y_gap(above, below):
    """Positive if `above` is above `below`, measured as (below.y0 - above.y1)."""
    return below.y0 - above.y1


# ---------------------------------------------------------------------------
# Tag shape classification helpers
# ---------------------------------------------------------------------------

def alpha_prefix(value):
    m = re.match(r"^([A-Z]+)", value)
    return m.group(1) if m else ""


def looks_asset_tag(norm):
    """E.g. VBU20000, TNK41101, CHR20001, BIN20001."""
    if not ASSET_TAG_RE.match(norm):
        return False
    pref = alpha_prefix(norm)
    if pref in DENY_PREFIXES:
        return False
    if pref in ISA_INSTRUMENT_CODES and pref not in EQUIPMENT_PREFIXES:
        return False
    # At least one letter followed by at least 4 digits — excludes things like BA2
    m = re.match(r"^([A-Z]+)(\d+)", norm)
    if not m:
        return False
    if len(m.group(2)) < 4:
        return False
    return True


def looks_hyphen_root(norm):
    """E.g. RL7-TNK-102, RL7-5000, RL7-GRR-103."""
    return bool(HYPHEN_ROOT_RE.match(norm) or COMPACT_HYPHEN_ROOT_RE.match(norm))


def canonical_root(norm):
    """Canonicalize customer root variants like RL7-PMP103 -> RL7-PMP-103."""
    s = normalize_token(norm).replace(" ", "-")
    m = COMPACT_HYPHEN_ROOT_RE.match(s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return s


def looks_root(norm):
    return looks_asset_tag(norm) or looks_hyphen_root(norm)


def looks_isa_child(norm):
    """E.g. HV-01, LIT-001, TCV-01, XV001."""
    m = ISA_CHILD_RE.match(norm)
    if not m:
        return False
    code = m.group("code")
    return code in ISA_INSTRUMENT_CODES


def looks_bare_isa_code(norm):
    """Bare function code without a number, e.g. HV or PI."""
    return norm in ISA_INSTRUMENT_CODES


def looks_equip_child(norm):
    """E.g. BLW001, CHR001, CYC001. (Used when paired with a parent skid tag.)"""
    m = EQUIP_CHILD_RE.match(norm)
    if not m:
        return False
    code = m.group("code")
    return code in EQUIPMENT_PREFIXES


def looks_child(norm):
    return looks_isa_child(norm) or looks_equip_child(norm)


def normalize_isa_child(norm):
    """Canonicalize ISA child to compact form: 'LIT-01' / 'LIT 01' / 'LIT01' -> 'LIT01'.

    We keep the child compact (no internal hyphen) because that's the standard
    Excel tag convention (e.g. 'VBU20000-HV001'). Hyphens are reserved for
    separating root from child.
    """
    m = ISA_CHILD_RE.match(norm)
    if not m:
        return norm
    return f"{m.group('code')}{m.group('num')}{m.group('suf')}"


def normalize_generic_child(norm):
    m = ISA_CHILD_RE.match(norm)
    if not m:
        return norm
    return f"{m.group('code')}{m.group('num')}{m.group('suf')}"


def instrument_code(component):
    m = ISA_CHILD_RE.match(component)
    return m.group("code") if m else alpha_prefix(component)


def normalize_bare_isa_child(norm, seq):
    """Canonicalize a bare ISA label using a local sequence number."""
    return f"{norm}{seq:03d}"


# ---------------------------------------------------------------------------
# Spatial pairing: find stacked parent-over-child tags
# ---------------------------------------------------------------------------

def span_orientation(s):
    """'H' for horizontal reading (wide+short), 'V' for vertical reading (tall+skinny)."""
    return "H" if s.width >= s.height else "V"


def span_subrect(s, start, end):
    """Approximate the bbox for a substring inside one PDF text span."""
    text = s.text or ""
    n = max(len(text), 1)
    start = max(0, min(start, n))
    end = max(start + 1, min(end, n))
    if span_orientation(s) == "V":
        y0 = s.y0 + (s.height * start / n)
        y1 = s.y0 + (s.height * end / n)
        return (s.x0, y0, s.x1, y1)
    x0 = s.x0 + (s.width * start / n)
    x1 = s.x0 + (s.width * end / n)
    return (x0, s.y0, x1, s.y1)


def find_stacked_pairs(spans, overlap_thresh=None, max_gap_ratio=None):
    """For each root span, find the nearest child span 'stacked' with it.

    Handles BOTH orientations in the display frame:
      - H-oriented spans (wide+short): stacked vertically, parent ABOVE child.
      - V-oriented spans (tall+skinny, typically on sideways-rendered text):
        stacked horizontally, parent to the LEFT of child.

    Returns list of (parent_span, child_span) tuples.
    """
    if overlap_thresh is None:
        overlap_thresh = ACTIVE_PROFILE.stacked_overlap_threshold
    if max_gap_ratio is None:
        max_gap_ratio = ACTIVE_PROFILE.stacked_max_gap_ratio
    roots = [s for s in spans if looks_root(s.norm)]
    children = [s for s in spans if looks_child(s.norm)]

    pairs = []
    used_children = set()

    for parent in sorted(roots, key=lambda s: (s.y0, s.x0)):
        orient = span_orientation(parent)
        # Line height: for H, use bbox height; for V, use bbox width
        line_h = parent.height if orient == "H" else parent.width
        max_gap = max_gap_ratio * line_h
        best = None
        best_score = None
        for child in children:
            if child.id in used_children or child.id == parent.id:
                continue
            # Only pair within same orientation to avoid spurious matches
            if span_orientation(child) != orient:
                continue
            if orient == "H":
                # Parent above child: y-axis stacking
                gap = child.y0 - parent.y1
                if gap < -0.3 * line_h or gap > max_gap:
                    continue
                overlap = x_overlap(parent, child)
            else:
                # Parent left of child: x-axis stacking
                gap = child.x0 - parent.x1
                if gap < -0.3 * line_h or gap > max_gap:
                    continue
                overlap = y_overlap(parent, child)
            if overlap < overlap_thresh:
                continue
            size_penalty = abs(parent.size - child.size) / max(parent.size, 1.0)
            score = gap * (1.0 - 0.5 * overlap) + 5.0 * size_penalty
            if best is None or score < best_score:
                best = child
                best_score = score
        if best is not None:
            pairs.append((parent, best))
            used_children.add(best.id)

    return pairs


# ---------------------------------------------------------------------------
# Zone classification (in display frame)
# ---------------------------------------------------------------------------

def classify_zone(page, x0, y0, x1, y1):
    """Classify by display-frame bbox position within the display rect."""
    w = page.rect.width   # display width
    h = page.rect.height  # display height
    cx = (x0 + x1) / 2
    cy = (y0 + y1) / 2
    # Title block is usually a strip on the right side OR bottom strip
    if cx > w * ACTIVE_PROFILE.zone_right_title_min:
        return "title-block/right"
    if cy > h * ACTIVE_PROFILE.zone_bottom_title_min:
        return "title-block/bottom"
    if (
        cy < h * ACTIVE_PROFILE.zone_top_edge_max
        or cx < w * ACTIVE_PROFILE.zone_left_edge_max
        or cx > w * ACTIVE_PROFILE.zone_right_edge_min
    ):
        return "edge"
    return "drawing"


def classify_page_role(spans):
    """Legend/reference vs process/drawing, based on span content."""
    text = " ".join(s.text.lower() for s in spans)
    score = sum(1 for m in LEGEND_MARKERS if m in text)
    return "legend/reference" if score >= 2 else "process/drawing"


# ---------------------------------------------------------------------------
# Candidate extraction — split by class
# ---------------------------------------------------------------------------

def extract_instruments_and_equipment(page, spans):
    """Extract instrument and equipment tags from stacked pairs + standalone assets.

    Instruments: parent-root + ISA child (HV, CV, XV, TCV, FIT, etc.)
    Equipment (stacked): parent-root + equipment child (BLW, CHR, CYC, etc.)
    Equipment (standalone): root appearing alone (asset tag or hyphen-root
      that has no paired child — will be included only if it looks equipment-like).
    """
    pairs = find_stacked_pairs(spans)
    tags = []
    used_spans = set()
    used_child_ids = set()

    for parent, child in pairs:
        used_spans.add(parent.id)
        used_spans.add(child.id)
        used_child_ids.add(child.id)
        root = canonical_root(parent.norm)
        # Canonical child form (normalize ISA hyphen)
        if looks_isa_child(child.norm):
            comp = normalize_isa_child(child.norm)
            tag_class = "instrument"
            method = "stacked:isa"
        else:
            comp = child.norm
            tag_class = "equipment"
            method = "stacked:equip"
        full = f"{root}-{comp}"
        x0 = min(parent.x0, child.x0)
        y0 = min(parent.y0, child.y0)
        x1 = max(parent.x1, child.x1)
        y1 = max(parent.y1, child.y1)
        tags.append(Tag(
            full_tag=full,
            root=root,
            component=comp,
            tag_class=tag_class,
            method=method,
            source_spans=(parent.id, child.id),
            x0=x0, y0=y0, x1=x1, y1=y1,
            source_text=f"{parent.text} / {child.text}",
        ))

    # Some customer drawings show function letters at symbols ("HV", "PI")
    # while the tag list expects sequenced tags ("HV001", "PI001"). Add a
    # low-confidence inferred candidate by associating each bare function label
    # to the nearest nearby root in the drawing area.
    inferred_by_root_code = {}
    roots = [s for s in spans if looks_root(s.norm)]
    bare_children = [s for s in spans if s.norm in BARE_INFER_CODES]
    for child in sorted(bare_children, key=lambda s: (s.y0, s.x0)):
        best = None
        best_dist = None
        child_diag = max(child.width, child.height, 1.0)
        for parent in roots:
            if parent.id == child.id:
                continue
            # Accept any root shape (hyphenated OR compact asset). The old
            # hyphen-only restriction blocked inference on asset-style sites
            # like STR63104/ACC61101 entirely. Distance filter does the work.
            dx = child.cx - parent.cx
            dy = child.cy - parent.cy
            adx = abs(dx)
            ady = abs(dy)
            # Keep this deliberately local; otherwise title-block and note text
            # starts creating plausible but wrong inferred instruments.
            if adx > ACTIVE_PROFILE.bare_infer_max_dx or ady > ACTIVE_PROFILE.bare_infer_max_dy:
                continue
            # Prefer same-row/same-column relationships.
            row_col_bonus = 0
            if ady < max(parent.height, child.height) * 4:
                row_col_bonus -= 60
            if adx < max(parent.width, child.width) * 2:
                row_col_bonus -= 40
            dist = (adx * adx + ady * ady) ** 0.5
            dist += row_col_bonus
            # Penalize pairing a tiny bare label to a large note-like root span.
            if parent.width > child_diag * 15:
                dist += 150
            if best is None or dist < best_dist:
                best = parent
                best_dist = dist
        if best is None:
            continue
        root = canonical_root(best.norm)
        key = (root, child.norm)
        inferred_by_root_code[key] = inferred_by_root_code.get(key, 0) + 1
        comp = normalize_bare_isa_child(child.norm, inferred_by_root_code[key])
        full = f"{root}-{comp}"
        x0 = min(best.x0, child.x0)
        y0 = min(best.y0, child.y0)
        x1 = max(best.x1, child.x1)
        y1 = max(best.y1, child.y1)
        tags.append(Tag(
            full_tag=full,
            root=root,
            component=comp,
            tag_class="instrument",
            method="inferred:bare-isa",
            source_spans=(best.id, child.id),
            x0=x0, y0=y0, x1=x1, y1=y1,
            source_text=f"{best.text} / {child.text}",
        ))

    # Every full ISA-child span (LT96, LCV633, HV-01) that the strict
    # stacked-pair step did not bind is emitted as standalone:isa with
    # root="" and full_tag=component. The comparator's root+component
    # same-page fallback resolves these against truth tags; guessing a
    # root geometrically here just adds noise. This closes the dominant
    # "root exists, comp missing" miss pattern without inventing wrong
    # pairings on dense drawings.
    isa_children = [s for s in spans if looks_isa_child(s.norm) and s.id not in used_child_ids]
    emitted_standalone = set()
    for child in isa_children:
        comp = normalize_isa_child(child.norm)
        key = (comp, round(child.x0), round(child.y0))
        if key in emitted_standalone:
            continue
        emitted_standalone.add(key)
        tags.append(Tag(
            full_tag=comp,
            root="",
            component=comp,
            tag_class="instrument",
            method="standalone:isa",
            source_spans=(child.id,),
            x0=child.x0, y0=child.y0, x1=child.x1, y1=child.y1,
            source_text=child.text,
        ))

    # Standalone equipment roots are first-class tags even when the same root
    # also has child instruments. Engineers usually need both the equipment
    # asset and its component tags in the report.
    emitted_roots = set()
    for s in spans:
        if not looks_root(s.norm):
            continue
        # Standalone equipment roots are first-class tags. Do not restrict this
        # to a tiny prefix allow-list; customer equipment families vary widely.
        if looks_asset_tag(s.norm) or looks_hyphen_root(s.norm):
            root = canonical_root(s.norm)
            if root in emitted_roots:
                continue
            emitted_roots.add(root)
            tags.append(Tag(
                full_tag=root,
                root=root,
                component="",
                tag_class="equipment",
                method="standalone:asset",
                source_spans=(s.id,),
                x0=s.x0, y0=s.y0, x1=s.x1, y1=s.y1,
                source_text=s.text,
            ))

    return tags


def extract_line_tags(spans):
    """Line tags: SIZE\"-SERVICE-NUMBER[-SPEC] like 3/4\"-CA-12A.

    Also handles full hyphen tags that include a size prefix concatenated,
    and multi-span line tags joined by adjacency.
    """
    tags = []
    for s in spans:
        m = LINE_TAG_RE.search(s.text.upper())
        if not m:
            continue
        body = m.group("body")
        parts = body.split("-")
        if len(parts) < 2:
            continue
        if parts[0] in DENY_PREFIXES:
            continue
        if any(w in s.text.upper() for w in LINE_NOISE_WORDS):
            continue
        if not any(ch.isdigit() for ch in parts[0] + parts[1]) and parts[1] not in LINE_WORD_COMPONENTS:
            continue
        # Canonical line identity is size-service-number. Keep the full source
        # text for audit, but strip trailing spec fragments like -EL-1 or -2.
        full, root, component = canonical_line_tag(m.group("size"), body)
        if not full:
            continue
        tags.append(Tag(
            full_tag=full,
            root=root,
            component=component,
            tag_class="line",
            method="line:size-prefix",
            source_spans=(s.id,),
            x0=s.x0, y0=s.y0, x1=s.x1, y1=s.y1,
            source_text=s.text,
            source_rects=(span_subrect(s, m.start(), m.end()),),
        ))
    return tags


def extract_full_hyphen_tags(spans, existing):
    """Pre-joined full tags in a single span like 'RL7-CKR-101-TCV-01'.

    These occur when the PDF author wrote the whole tag as one text run.
    Classify by the tail component.
    """
    used_rects = {(round(t.x0), round(t.y0), t.full_tag) for t in existing}
    tags = []
    for s in spans:
        up = s.norm
        for m in FULL_HYPHEN_TAG_RE.finditer(up):
            tok = m.group(0)
            parts = tok.split("-")
            if len(parts) < 3:
                continue
            if alpha_prefix(parts[0]) in DENY_PREFIXES:
                continue
            # Classify by tail: if last 1-2 parts form an ISA child -> instrument
            tail1 = parts[-1]
            tail2 = "-".join(parts[-2:])
            tag_class = None
            method = None
            if looks_isa_child(tail2):
                comp = normalize_isa_child(tail2)
                root = canonical_root("-".join(parts[:-2]))
                if not looks_root(root):
                    continue
                tag_class = "instrument"
                method = "fulltag:isa"
            elif looks_isa_child(tail1):
                comp = normalize_isa_child(tail1)
                root = canonical_root("-".join(parts[:-1]))
                if not looks_root(root):
                    continue
                tag_class = "instrument"
                method = "fulltag:isa"
            elif looks_equip_child(tail1):
                comp = tail1
                root = canonical_root("-".join(parts[:-1]))
                tag_class = "equipment"
                method = "fulltag:equip"
            else:
                # Likely a line tag with no size prefix in this span
                continue
            full = f"{root}-{comp}"
            key = (round(s.x0), round(s.y0), full)
            if key in used_rects:
                continue
            tags.append(Tag(
                full_tag=full,
                root=root,
                component=comp,
                tag_class=tag_class,
                method=method,
                source_spans=(s.id,),
                x0=s.x0, y0=s.y0, x1=s.x1, y1=s.y1,
                source_text=s.text,
            ))
    return tags


# ---------------------------------------------------------------------------
# Confidence scoring (evidence-based)
# ---------------------------------------------------------------------------

def confidence_rating(confidence):
    if confidence >= ACTIVE_PROFILE.rating_high_threshold:
        return "High"
    if confidence >= ACTIVE_PROFILE.rating_medium_threshold:
        return "Medium"
    return "Low"


def score_tag(tag, page, sheet_id, page_role, source_text, repeated=1, sheet_verified=False):
    """Assign a confidence based on evidence."""
    weights = ACTIVE_PROFILE.confidence_weights
    base = weights.get(tag.method, weights.get("unknown", 0.60))

    conf = base
    evidence = []

    # Zone
    zone = classify_zone(page, tag.x0, tag.y0, tag.x1, tag.y1)
    if zone == "drawing":
        conf += weights.get("drawing_zone_bonus", 0.06)
        evidence.append("drawing zone")
    elif zone.startswith("title-block"):
        conf += weights.get("title_block_penalty", -0.22)
        evidence.append("title-block penalty")
    elif zone == "edge":
        conf += weights.get("edge_zone_penalty", -0.10)
        evidence.append("edge zone")

    # Page role
    if page_role == "legend/reference":
        conf += weights.get("legend_page_penalty", -0.25)
        evidence.append("legend page penalty")
    else:
        conf += weights.get("process_page_bonus", DEFAULT_PROFILE.confidence_weights["process_page_bonus"])
        evidence.append("process page")

    # Sheet mapping
    if sheet_id.startswith("PAGE_"):
        conf += weights.get("unmapped_sheet_penalty", -0.06)
        evidence.append("unmapped sheet")
    else:
        conf += weights.get("mapped_sheet_bonus", 0.03)
        evidence.append("mapped sheet")

    # Noisy context
    st = source_text.lower() if source_text else ""
    if any(m in st for m in NOISE_CONTEXT):
        conf += weights.get("noisy_context_penalty", -0.22)
        evidence.append("noisy context")

    # Valve-group internals are often useful, but they are also a common source
    # of dense schedule/manifold candidates that should be reviewed before
    # being treated as clean instrument-list output.
    if tag.tag_class == "instrument" and alpha_prefix(tag.root) == "VG":
        conf += weights.get("valve_group_penalty", -0.18)
        evidence.append("valve group review")

    if tag.tag_class == "instrument" and instrument_code(tag.component) in CUSTOM_REVIEW_INSTRUMENT_CODES:
        conf += weights.get("custom_code_penalty", -0.14)
        evidence.append("custom code review")

    if tag.source_agreement == "matched":
        conf += weights.get("matched_source_bonus", 0.06)
        evidence.append("native/ocr agreement")
    elif tag.source_agreement == "ocr-only" or tag.text_source == "ocr":
        conf += weights.get("ocr_only_penalty", -0.08)
        evidence.append("ocr only")
    elif tag.source_agreement == "conflict":
        conf += weights.get("source_conflict_penalty", -0.15)
        evidence.append("source conflict")

    # Repetition bonus
    if repeated > 1:
        bump = min(ACTIVE_PROFILE.repetition_bonus_max, ACTIVE_PROFILE.repetition_bonus_each * repeated)
        conf += bump
        evidence.append(f"repeated x{repeated}")

    if sheet_verified:
        conf += 0.07
        evidence.append("excel sheet match")

    conf = max(0.05, min(0.99, conf))
    tag.confidence = conf
    tag.rating = confidence_rating(conf)
    tag.zone = zone
    tag.evidence = "; ".join(evidence)
    return tag


# ---------------------------------------------------------------------------
# Sheet mapping (page -> drawing number)
# ---------------------------------------------------------------------------

def build_sheet_map(doc):
    """Map drawing sheet IDs to PDF page numbers.

    Returns:
      sheet_to_page: {sheet_id: [page_num, ...]}
      page_to_sheet: {page_num: sheet_id}

    A sheet ID is not always unique, so callers that filter by sheet must allow
    multiple pages.
    """
    sheet_to_page = {}
    page_to_sheet = {}
    for page_num, page in enumerate(doc, start=1):
        # Look for .dwg filename in text
        text = page.get_text("text")
        candidates = []
        for m in DWG_FILE_RE.finditer(text):
            candidates.append((m.group(1).upper(), 100))
        # Look for sheet-id label pattern
        m = re.search(
            r"(?:drawing|dwg|sheet)\s*(?:no\.?|number|#)?\s*[:#]?\s*([A-Z]{1,6}\d{3,7}[A-Z]?)",
            text, re.IGNORECASE,
        )
        if m:
            candidates.append((m.group(1).upper(), 60))
        # Title-block standalone sheet id token. Use display-frame spans here;
        # page.get_text("words") returns native coordinates on rotated pages.
        sheet_tokens = []
        drawing_tokens = []
        for s in extract_display_spans(page):
            token = normalize_token(s.text)
            if not SHEET_ID_RE.match(token):
                continue
            sheet_tokens.append(token)
            if ACTIVE_PROFILE.sheet_preferred_prefixes and any(
                token.startswith(prefix) for prefix in ACTIVE_PROFILE.sheet_preferred_prefixes
            ):
                drawing_tokens.append(token)
            if (
                s.x0 > page.rect.width * ACTIVE_PROFILE.sheet_title_right_min
                or s.y0 > page.rect.height * ACTIVE_PROFILE.sheet_title_bottom_min
            ):
                # Multiple drawing-like tokens can appear in a title block
                # (current drawing plus references). If filename text is not
                # available, prefer the token most strongly positioned in the
                # right/bottom title-block area instead of the first text span.
                pos_score = (s.cx / max(page.rect.width, 1.0)) + (s.cy / max(page.rect.height, 1.0))
                candidates.append((token, 50 + pos_score))
        if not candidates and len(set(drawing_tokens)) == 1:
            candidates.append((drawing_tokens[0], 30))
        elif not candidates and len(set(sheet_tokens)) == 1:
            candidates.append((sheet_tokens[0], 25))
        if candidates:
            best = sorted(candidates, key=lambda c: -c[1])[0][0]
            sheet_to_page.setdefault(best, [])
            if page_num not in sheet_to_page[best]:
                sheet_to_page[best].append(page_num)
            page_to_sheet[page_num] = best
    return sheet_to_page, page_to_sheet


def normalize_sheet_id(value):
    v = normalize_token(value)
    m = DWG_FILE_RE.search(v)
    if m:
        return m.group(1).upper()
    m = re.search(r"\b([A-Z]{1,6}\d{3,7}[A-Z]?)\b", v)
    return m.group(1).upper() if m else v


# ---------------------------------------------------------------------------
# OCR gating
# ---------------------------------------------------------------------------

def page_needs_ocr(page, ocr_mode="auto", native_span_count=0):
    decision = analyze_page_for_ocr(
        page,
        OcrOptions(mode=ocr_mode, dpi=300),
        native_span_count=native_span_count,
    )
    return decision.needs_ocr


def textpage_for_page(page, ocr_mode="auto", ocr_dpi=300, native_span_count=0):
    options = OcrOptions(mode=ocr_mode, dpi=ocr_dpi)
    decision = analyze_page_for_ocr(page, options, native_span_count=native_span_count)
    textpage = get_ocr_textpage(page, decision, options)
    return textpage, decision.page_ocr_mode, decision.needs_ocr, decision.used_ocr, decision.warning


def _node_ids_for_tag(tag, span_map):
    ids = []
    for span_id in tag.source_spans:
        span = span_map.get(span_id)
        if not span:
            continue
        grouped = getattr(span, "grouped_from", None)
        if grouped:
            ids.extend(str(g) for g in grouped)
        else:
            ids.append(str(getattr(span, "node_id", "") or span_id))
    return tuple(dict.fromkeys(ids))


def _extract_tags_from_spans(page, spans, sheet_id, page_num, page_role, source_name, decision):
    all_tags = []
    pair_tags = extract_instruments_and_equipment(page, spans)
    all_tags.extend(pair_tags)
    hyphen_tags = extract_full_hyphen_tags(spans, pair_tags)
    all_tags.extend(hyphen_tags)
    all_tags.extend(extract_line_tags(spans))

    span_map = {s.id: s for s in spans}
    for t in all_tags:
        rects = []
        for span_id in t.source_spans:
            span = span_map.get(span_id)
            if span:
                rects.append((span.x0, span.y0, span.x1, span.y1))
        if not t.source_rects:
            t.source_rects = tuple(rects)
        t.sheet_id = sheet_id
        t.page_num = page_num
        t.text_source = source_name
        t.source_agreement = "ocr-only" if source_name == "ocr" else "native-only"
        t.page_ocr_mode = decision.page_ocr_mode
        t.ocr_backend = decision.backend if decision.needs_ocr else ""
        t.ocr_reason = decision.reason
        node_ids = _node_ids_for_tag(t, span_map)
        t.source_node_ids = ", ".join(node_ids)
        t.source_node_count = len(node_ids)
        score_tag(t, page, sheet_id, page_role, t.source_text)
    return all_tags


def _fuse_source_tags(native_tags, ocr_tags):
    if not ocr_tags:
        return native_tags
    fused = list(native_tags)
    native_by_key = {(t.page_num, t.sheet_id, t.tag_class, t.full_tag): t for t in fused}
    for ocr_tag in ocr_tags:
        key = (ocr_tag.page_num, ocr_tag.sheet_id, ocr_tag.tag_class, ocr_tag.full_tag)
        native = native_by_key.get(key)
        if native:
            native.text_source = "native+ocr"
            native.source_agreement = "matched"
            if ocr_tag.source_text and ocr_tag.source_text not in native.source_text:
                native.source_text = f"{native.source_text} | OCR: {ocr_tag.source_text}"
            merged_nodes = list(filter(None, [native.source_node_ids, ocr_tag.source_node_ids]))
            native.source_node_ids = ", ".join(merged_nodes)
            native.source_node_count = len([v for v in native.source_node_ids.split(", ") if v])
            native.ocr_backend = ocr_tag.ocr_backend
            native.ocr_reason = ocr_tag.ocr_reason
            native.confidence = min(0.99, native.confidence + ACTIVE_PROFILE.confidence_weights.get("matched_source_bonus", 0.06))
            native.rating = confidence_rating(native.confidence)
            if "native/ocr agreement" not in native.evidence:
                native.evidence = f"{native.evidence}; native/ocr agreement" if native.evidence else "native/ocr agreement"
        else:
            fused.append(ocr_tag)
    return fused


# ---------------------------------------------------------------------------
# Main extraction pipeline
# ---------------------------------------------------------------------------

def extract_page(page, page_num, sheet_id, mode="instrument", ocr_mode="auto", ocr_dpi=300, debug_text=False):
    """Extract tags from a single page.

    mode: 'instrument' | 'equipment' | 'line' | 'all'
    Returns (tags, profile)
    """
    native_spans = extract_display_spans(page, source="native", id_prefix=f"p{page_num}:native")
    native_graph = build_text_graph(native_spans, page.rect.width, page.rect.height, source="native")
    native_scan_spans = native_spans + native_graph.synthetic_spans
    page_role = classify_page_role(native_spans)

    options = OcrOptions(mode=ocr_mode, dpi=ocr_dpi)
    decision = analyze_page_for_ocr(page, options, native_span_count=len(native_spans))

    native_tags = _extract_tags_from_spans(
        page, native_scan_spans, sheet_id, page_num, page_role, "native", decision
    )

    ocr_spans = []
    ocr_graph = build_text_graph([], page.rect.width, page.rect.height, source="ocr")
    ocr_word_count = 0
    ocr_textpage = get_ocr_textpage(page, decision, options)
    if ocr_textpage is not None:
        ocr_spans = extract_display_spans(
            page,
            textpage=ocr_textpage,
            source="ocr",
            id_prefix=f"p{page_num}:ocr",
        )
        ocr_graph = build_text_graph(ocr_spans, page.rect.width, page.rect.height, source="ocr")
        try:
            ocr_word_count = len(page.get_text("words", textpage=ocr_textpage))
        except Exception:
            ocr_word_count = len(ocr_spans)

    ocr_tags = []
    if ocr_spans:
        ocr_tags = _extract_tags_from_spans(
            page,
            ocr_spans + ocr_graph.synthetic_spans,
            sheet_id,
            page_num,
            page_role,
            "ocr",
            decision,
        )

    all_tags = _fuse_source_tags(native_tags, ocr_tags)

    # Filter by mode
    if mode != "all":
        wanted = {mode}
        if mode == "instrument":
            pass  # strictly instrument only
        all_tags = [t for t in all_tags if t.tag_class in wanted]

    debug_tokens = ()
    debug_groups = ()
    debug_rejected = ()
    if debug_text:
        debug_tokens = tuple({
            "sheet_id": sheet_id,
            "page_num": page_num,
            "source": getattr(s, "source", "native"),
            "span_id": s.id,
            "node_id": getattr(s, "node_id", ""),
            "x0": s.x0,
            "y0": s.y0,
            "x1": s.x1,
            "y1": s.y1,
            "size": getattr(s, "size", 0.0),
            "text": s.text,
            "norm": s.norm,
        } for s in native_spans + ocr_spans)
        debug_groups = tuple({
            "sheet_id": sheet_id,
            "page_num": page_num,
            "source": g.source,
            "group_id": g.group_id,
            "node_ids": ", ".join(g.node_ids),
            "x0": g.x0,
            "y0": g.y0,
            "x1": g.x1,
            "y1": g.y1,
            "text": g.text,
            "norm": g.norm,
        } for g in native_graph.groups + ocr_graph.groups)
        debug_rejected = tuple({
            "sheet_id": sheet_id,
            "page_num": page_num,
            "source": "graph",
            "text": text,
        } for text in native_graph.rejected_text + ocr_graph.rejected_text)

    profile = PageProfile(
        page_num=page_num,
        sheet_id=sheet_id,
        page_role=page_role,
        rotation=page.rotation,
        native_size=(page.mediabox.width, page.mediabox.height),
        display_size=(page.rect.width, page.rect.height),
        span_count=len(native_spans) + len(ocr_spans),
        word_count=decision.native_word_count,
        drawing_count=decision.drawing_count,
        image_count=decision.image_count,
        needs_ocr=decision.needs_ocr,
        used_ocr=decision.used_ocr,
        ocr_warning=decision.warning,
        native_span_count=len(native_spans),
        ocr_span_count=len(ocr_spans),
        native_word_count=decision.native_word_count,
        ocr_word_count=ocr_word_count,
        image_coverage=decision.image_coverage,
        vector_suspicion=decision.vector_suspicion,
        ocr_mode=decision.page_ocr_mode,
        ocr_backend=decision.backend if decision.needs_ocr else "",
        ocr_reason=decision.reason,
        ocr_dpi=ocr_dpi if decision.needs_ocr else 0,
        graph_node_count=len(native_graph.nodes) + len(ocr_graph.nodes),
        graph_group_count=len(native_graph.groups) + len(ocr_graph.groups),
        rejected_text_count=len(native_graph.rejected_text) + len(ocr_graph.rejected_text),
        text_tokens=debug_tokens,
        text_groups=debug_groups,
        rejected_text=debug_rejected,
    )
    return all_tags, profile


def _apply_repetition_bump(tags):
    """Apply per-sheet repetition confidence after all source fusion is done."""
    counts = {}
    for t in tags:
        counts[(t.sheet_id, t.full_tag)] = counts.get((t.sheet_id, t.full_tag), 0) + 1
    for t in tags:
        n = counts[(t.sheet_id, t.full_tag)]
        if n > 1:
            t.confidence = min(
                0.99,
                t.confidence + min(ACTIVE_PROFILE.repetition_bonus_max, ACTIVE_PROFILE.repetition_bonus_each * n),
            )
            t.rating = confidence_rating(t.confidence)
            t.evidence = t.evidence + f"; repeated x{n}"
    return tags


def scan_open_document(doc, page_to_sheet=None, mode="instrument", ocr_mode="auto", ocr_dpi=300, debug_text=False):
    """Scan an already-open PyMuPDF document."""
    tags = []
    profiles = []
    if page_to_sheet is None:
        _, page_to_sheet = build_sheet_map(doc)
    for page_num, page in enumerate(doc, start=1):
        sheet_id = page_to_sheet.get(page_num, f"PAGE_{page_num}")
        page_tags, profile = extract_page(
            page,
            page_num,
            sheet_id,
            mode=mode,
            ocr_mode=ocr_mode,
            ocr_dpi=ocr_dpi,
            debug_text=debug_text,
        )
        tags.extend(page_tags)
        profiles.append(profile)
    return _apply_repetition_bump(tags), profiles


def scan_document(pdf_path, mode="instrument", ocr_mode="auto", ocr_dpi=300, debug_text=False):
    """Scan a full PDF, return (tags, profiles). Mode filters the output class."""
    with fitz.open(pdf_path) as doc:
        return scan_open_document(
            doc,
            mode=mode,
            ocr_mode=ocr_mode,
            ocr_dpi=ocr_dpi,
            debug_text=debug_text,
        )


# ---------------------------------------------------------------------------
# Dedupe / ranking
# ---------------------------------------------------------------------------

def tag_identity_key(t):
    return (t.sheet_id, t.page_num, t.full_tag)


def tag_location_key(t):
    return (t.sheet_id, t.page_num, t.full_tag, round(t.x0), round(t.y0))


def tag_sort_key(t):
    method_rank = {
        "stacked:isa": 0, "fulltag:isa": 1, "line:size-prefix": 2,
        "stacked:equip": 3, "fulltag:equip": 4, "standalone:asset": 5,
    }.get(t.method, 9)
    return (-t.confidence, method_rank, t.sheet_id, t.page_num, t.full_tag)


def dedupe(tags, by_location=False):
    best = {}
    for t in tags:
        key = tag_location_key(t) if by_location else tag_identity_key(t)
        if key not in best or tag_sort_key(t) < tag_sort_key(best[key]):
            best[key] = t
    return sorted(best.values(), key=tag_sort_key)


def confidence_counts(tags):
    out = {"High": 0, "Medium": 0, "Low": 0}
    for t in tags:
        out[t.rating] += 1
    return out


def summarize_profiles(profiles):
    return {
        "pages": len(profiles),
        "ocr_needed": len([p for p in profiles if p.needs_ocr]),
        "ocr_used": len([p for p in profiles if p.used_ocr]),
        "warnings": len([p for p in profiles if p.ocr_warning]),
        "avg_words": round(mean([p.word_count for p in profiles]), 1) if profiles else 0,
        "native_spans": sum(getattr(p, "native_span_count", 0) for p in profiles),
        "ocr_spans": sum(getattr(p, "ocr_span_count", 0) for p in profiles),
        "graph_groups": sum(getattr(p, "graph_group_count", 0) for p in profiles),
    }


# ---------------------------------------------------------------------------
# Public helpers used by scanner/comparator (validation, legacy shims)
# ---------------------------------------------------------------------------

def validate_input_file(path, allowed_ext):
    if not path:
        raise ValueError("A file path is required.")
    full = os.path.abspath(path)
    if not os.path.isfile(full):
        raise FileNotFoundError(f"File not found: {full}")
    if allowed_ext and os.path.splitext(full)[1].lower() not in allowed_ext:
        raise ValueError(f"Unsupported file type: {full}")
    return full


def validate_pdf_content(pdf_path):
    try:
        with fitz.open(pdf_path) as doc:
            if len(doc) == 0:
                raise ValueError("The PDF is empty.")
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("The PDF could not be opened.") from exc


def parse_excel_tag(value):
    """Parse a tag from an Excel cell. Accepts: 'VBU20000-XV001', 'VBU20000 XV001',
    'RL7-TNK-102-LIT-01', '3/4"-CA-12A'."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    # Try line tag with size prefix first
    m = LINE_TAG_RE.search(raw.upper())
    if m:
        full, root, component = canonical_line_tag(m.group("size"), m.group("body"))
        if not full:
            return None
        return {
            "full_tag": full,
            "root": root,
            "component": component,
            "tag_class": "line",
        }
    # Strip pipe size if present
    stripped = strip_pipe_size(raw)
    norm = normalize_token(stripped).replace(" ", "-")
    # Collapse multiple hyphens
    norm = re.sub(r"-+", "-", norm).strip("-")
    if not norm:
        return None
    parts = norm.split("-")
    if alpha_prefix(parts[0]) in DENY_PREFIXES:
        return None
    if looks_hyphen_root(norm):
        root = canonical_root(norm)
        return {"full_tag": root, "root": root, "component": "", "tag_class": "equipment"}
    if len(parts) >= 2:
        tail1 = parts[-1]
        tail2 = "-".join(parts[-2:]) if len(parts) >= 2 else tail1
        if looks_isa_child(tail2):
            comp = normalize_isa_child(tail2)
            root = canonical_root("-".join(parts[:-2]))
            return {"full_tag": f"{root}-{comp}", "root": root, "component": comp, "tag_class": "instrument"}
        if looks_isa_child(tail1):
            comp = normalize_isa_child(tail1)
            root = canonical_root("-".join(parts[:-1]))
            return {"full_tag": f"{root}-{comp}", "root": root, "component": comp, "tag_class": "instrument"}
        if looks_equip_child(tail1):
            root = canonical_root("-".join(parts[:-1]))
            return {"full_tag": f"{root}-{tail1}", "root": root, "component": tail1, "tag_class": "equipment"}
        if ISA_CHILD_RE.match(tail2):
            comp = normalize_generic_child(tail2)
            root = canonical_root("-".join(parts[:-2]))
            return {"full_tag": f"{root}-{comp}", "root": root, "component": comp, "tag_class": "instrument"}
        if ISA_CHILD_RE.match(tail1):
            comp = normalize_generic_child(tail1)
            root = canonical_root("-".join(parts[:-1]))
            return {"full_tag": f"{root}-{comp}", "root": root, "component": comp, "tag_class": "instrument"}
        if len(parts) == 2 and parts[0].isalpha() and any(ch.isdigit() for ch in parts[1]):
            return {
                "full_tag": norm,
                "root": parts[0],
                "component": parts[1],
                "tag_class": "line",
            }
    if looks_asset_tag(norm) or looks_hyphen_root(norm):
        root = canonical_root(norm)
        return {"full_tag": root, "root": root, "component": "", "tag_class": "equipment"}
    return None
