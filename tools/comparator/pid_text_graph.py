"""Page-local text graph helpers for P&ID extraction.

The current extractor still uses established tag parsers for precision, but
they now receive both raw spans and conservative grouped spans from this graph.
That gives split-text tags a path into the same candidate flow without making
regex patches the center of the design.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


def _normalize(value):
    s = str(value or "").strip().upper()
    for dash in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014"):
        s = s.replace(dash, "-")
    s = s.replace("\u00a0", " ").replace("\t", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.strip('.,;:()[]{}<>"\'')


@dataclass(frozen=True)
class TextNode:
    node_id: str
    span_id: object
    text: str
    norm: str
    x0: float
    y0: float
    x1: float
    y1: float
    source: str


@dataclass(frozen=True)
class TextEdge:
    source_id: str
    target_id: str
    relation: str
    weight: float = 1.0


@dataclass(frozen=True)
class SyntheticSpan:
    id: int
    text: str
    norm: str
    x0: float
    y0: float
    x1: float
    y1: float
    size: float
    source: str = "graph"
    node_id: str = ""
    ocr_confidence: float | None = None
    grouped_from: tuple = ()

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


@dataclass(frozen=True)
class TextGroup:
    group_id: str
    node_ids: tuple
    text: str
    norm: str
    x0: float
    y0: float
    x1: float
    y1: float
    source: str
    relation: str = "same-line"


@dataclass
class TextGraph:
    nodes: list[TextNode] = field(default_factory=list)
    edges: list[TextEdge] = field(default_factory=list)
    groups: list[TextGroup] = field(default_factory=list)
    synthetic_spans: list[SyntheticSpan] = field(default_factory=list)
    rejected_text: list[str] = field(default_factory=list)


TAGISH_RE = re.compile(
    r"("
    r"\d+(?:\s+\d+/\d+|/\d+)?\s*\"?\s*-\s*[A-Z0-9]"
    r"|[A-Z0-9]{2,}(?:\s*-\s*[A-Z0-9]{1,}){1,}"
    r"|[A-Z]{1,5}\s*-?\s*\d{1,5}"
    r")"
)


def _is_tagish(text):
    norm = _normalize(text)
    if len(norm) < 4:
        return False
    if not any(ch.isdigit() for ch in norm):
        return False
    return bool(TAGISH_RE.search(norm))


def _line_orientation(span):
    return "H" if (span.x1 - span.x0) >= (span.y1 - span.y0) else "V"


def _same_line(a, b):
    if _line_orientation(a) != _line_orientation(b):
        return False
    if _line_orientation(a) == "H":
        overlap = max(0.0, min(a.y1, b.y1) - max(a.y0, b.y0))
        denom = max(1.0, min(a.y1 - a.y0, b.y1 - b.y0))
        return overlap / denom >= 0.45 or abs(((a.y0 + a.y1) / 2) - ((b.y0 + b.y1) / 2)) <= max(a.y1 - a.y0, b.y1 - b.y0)
    overlap = max(0.0, min(a.x1, b.x1) - max(a.x0, b.x0))
    denom = max(1.0, min(a.x1 - a.x0, b.x1 - b.x0))
    return overlap / denom >= 0.45 or abs(((a.x0 + a.x1) / 2) - ((b.x0 + b.x1) / 2)) <= max(a.x1 - a.x0, b.x1 - b.x0)


def _gap(a, b):
    if _line_orientation(a) == "H":
        return b.x0 - a.x1
    return b.y0 - a.y1


def _ordered_key(span):
    if _line_orientation(span) == "H":
        return (span.y0, span.x0)
    return (span.x0, span.y0)


def build_text_graph(spans, page_width=0, page_height=0, source="native", start_id=None):
    """Build a conservative same-line text graph.

    Returns graph debug objects plus synthetic grouped spans that can be passed
    to existing candidate extraction. Group generation is intentionally narrow:
    only adjacent text that already looks tag-like is emitted.
    """
    graph = TextGraph()
    next_id = (max([int(s.id) for s in spans if isinstance(s.id, int)] or [0]) + 1) if start_id is None else start_id
    for s in spans:
        node_id = getattr(s, "node_id", "") or f"{source}:{s.id}"
        graph.nodes.append(TextNode(
            node_id=node_id,
            span_id=s.id,
            text=s.text,
            norm=s.norm,
            x0=s.x0,
            y0=s.y0,
            x1=s.x1,
            y1=s.y1,
            source=getattr(s, "source", source),
        ))

    line_groups = []
    for s in sorted(spans, key=_ordered_key):
        placed = False
        for group in line_groups:
            if _same_line(group[-1], s):
                group.append(s)
                placed = True
                break
        if not placed:
            line_groups.append([s])

    group_idx = 0
    for line in line_groups:
        if len(line) < 2:
            continue
        line = sorted(line, key=lambda s: (s.x0, s.y0) if _line_orientation(s) == "H" else (s.y0, s.x0))
        for a, b in zip(line, line[1:]):
            gap = _gap(a, b)
            line_h = max(a.y1 - a.y0, b.y1 - b.y0, a.x1 - a.x0 if _line_orientation(a) == "V" else 0, 1.0)
            if -0.5 * line_h <= gap <= max(18.0, 2.5 * line_h):
                graph.edges.append(TextEdge(
                    getattr(a, "node_id", "") or f"{source}:{a.id}",
                    getattr(b, "node_id", "") or f"{source}:{b.id}",
                    "same-line-adjacent",
                    1.0,
                ))

        max_window = min(6, len(line))
        for start in range(len(line)):
            for end in range(start + 2, min(len(line), start + max_window) + 1):
                window = line[start:end]
                gaps_ok = True
                for a, b in zip(window, window[1:]):
                    gap = _gap(a, b)
                    line_h = max(a.y1 - a.y0, b.y1 - b.y0, 1.0)
                    if gap < -0.5 * line_h or gap > max(22.0, 3.0 * line_h):
                        gaps_ok = False
                        break
                if not gaps_ok:
                    continue
                spaced = " ".join(s.text for s in window)
                compact = re.sub(r"\s*-\s*", "-", spaced)
                compact = re.sub(r"\s*\"\s*", '"', compact)
                compact = re.sub(r"\s+", " ", compact).strip()
                if not _is_tagish(compact):
                    if len(compact) <= 40:
                        graph.rejected_text.append(compact)
                    continue
                x0 = min(s.x0 for s in window)
                y0 = min(s.y0 for s in window)
                x1 = max(s.x1 for s in window)
                y1 = max(s.y1 for s in window)
                node_ids = tuple((getattr(s, "node_id", "") or f"{source}:{s.id}") for s in window)
                group_idx += 1
                gid = f"{source}:g{group_idx}"
                group = TextGroup(
                    group_id=gid,
                    node_ids=node_ids,
                    text=compact,
                    norm=_normalize(compact),
                    x0=x0,
                    y0=y0,
                    x1=x1,
                    y1=y1,
                    source=source,
                )
                graph.groups.append(group)
                graph.synthetic_spans.append(SyntheticSpan(
                    id=next_id,
                    text=compact,
                    norm=group.norm,
                    x0=x0,
                    y0=y0,
                    x1=x1,
                    y1=y1,
                    size=max(float(getattr(s, "size", 0.0) or 0.0) for s in window),
                    source=f"{source}+graph",
                    node_id=gid,
                    grouped_from=node_ids,
                ))
                next_id += 1
    return graph
