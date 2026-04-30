"""Dependency-free XLSX writer for small reporting workloads.

This module intentionally stays tiny and stdlib-only. It supports:

- multiple worksheets
- rows provided as dictionaries or sequences
- strings, numbers, booleans, and blanks
- a simple header style and auto-sized column widths

The resulting workbook is a valid .xlsx file built directly from OpenXML
parts packaged into a ZIP archive.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping, Sequence
from xml.sax.saxutils import escape
import zipfile

__all__ = [
    "SheetSpec",
    "Workbook",
    "sanitize_sheet_name",
    "write_xlsx",
]


EXCEL_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"
DOC_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
CP_NS = "http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
DC_NS = "http://purl.org/dc/elements/1.1/"
DCTERMS_NS = "http://purl.org/dc/terms/"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"

INVALID_SHEET_CHARS = set("[]:*?/\\")


@dataclass
class SheetSpec:
    """Description of a worksheet to be written."""

    name: str
    rows: Sequence[Any] = field(default_factory=list)
    columns: Sequence[str] | None = None
    freeze_header: bool = True
    autofilter: bool = True
    header_style: bool = True


def sanitize_sheet_name(name: str, existing: set[str] | None = None) -> str:
    """Return a valid Excel worksheet name.

    Excel worksheet names are limited to 31 characters and cannot contain
    certain punctuation characters. If ``existing`` is supplied the returned
    name will also be made unique within that set.
    """

    raw = (name or "Sheet").strip()
    cleaned = "".join("_" if ch in INVALID_SHEET_CHARS else ch for ch in raw)
    cleaned = cleaned or "Sheet"
    cleaned = cleaned[:31]

    if existing is None:
        return cleaned

    candidate = cleaned
    suffix = 2
    while candidate in existing:
        base = cleaned[: max(0, 31 - len(f" ({suffix})"))].rstrip()
        candidate = f"{base} ({suffix})"[:31] or f"Sheet ({suffix})"
        suffix += 1
    existing.add(candidate)
    return candidate


def _column_name(index: int) -> str:
    if index < 1:
        raise ValueError("Excel columns are 1-based")
    result = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _sheet_dimension(row_count: int, column_count: int) -> str:
    if row_count <= 0 or column_count <= 0:
        return "A1"
    return f"A1:{_column_name(column_count)}{row_count}"


def _text_width(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 4 if value else 5
    if isinstance(value, (int, float)):
        text = f"{value}"
    else:
        text = str(value)
    if "\n" in text:
        text = max(text.splitlines(), key=len, default=text)
    return len(text)


def _normalize_scalar(value: Any) -> Any:
    """Reduce common Python objects to workbook-friendly scalars."""

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, Mapping):
        return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)
    if isinstance(value, (list, tuple, set)):
        return json.dumps(_jsonable(list(value)), ensure_ascii=False)
    return str(value)


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return str(value)


def _coerce_rows(
    rows: Sequence[Any],
    columns: Sequence[str] | None = None,
) -> tuple[list[str], list[list[Any]]]:
    """Convert dict/list rows into a rectangular matrix.

    If rows contain dictionaries and columns are not provided, headers are
    inferred from the first-seen key order across the row sequence.
    If rows contain sequences, columns should be provided for meaningful
    headers; otherwise generic columns are inferred from the widest row.
    """

    row_list = list(rows or [])
    if not row_list:
        headers = list(columns or [])
        return headers, []

    first_non_empty = next((row for row in row_list if row is not None), None)
    if first_non_empty is None:
        headers = list(columns or [])
        return headers, [[None for _ in headers] for _ in row_list]

    if isinstance(first_non_empty, Mapping):
        ordered_headers: list[str] = list(columns or [])
        seen = set(ordered_headers)
        if not ordered_headers:
            for row in row_list:
                if not isinstance(row, Mapping):
                    raise TypeError("Mixed row types require explicit columns")
                for key in row.keys():
                    key_str = str(key)
                    if key_str not in seen:
                        ordered_headers.append(key_str)
                        seen.add(key_str)
        matrix: list[list[Any]] = []
        for row in row_list:
            if not isinstance(row, Mapping):
                raise TypeError("Mixed row types require explicit columns")
            normalized_row = {str(key): value for key, value in row.items()}
            matrix.append([_normalize_scalar(normalized_row.get(header)) for header in ordered_headers])
        return ordered_headers, matrix

    # Sequence rows.
    sequence_rows: list[Sequence[Any]] = []
    widest = 0
    for row in row_list:
        if isinstance(row, (str, bytes)) or not isinstance(row, Sequence):
            raise TypeError("Sequence sheets require list/tuple rows")
        sequence_rows.append(row)
        widest = max(widest, len(row))

    if columns is None:
        headers = [f"Column {idx}" for idx in range(1, widest + 1)]
    else:
        headers = [str(item) for item in columns]
        widest = max(widest, len(headers))
        if len(headers) < widest:
            headers = headers + [f"Column {idx}" for idx in range(len(headers) + 1, widest + 1)]

    matrix = []
    for row in sequence_rows:
        values = [_normalize_scalar(row[idx]) if idx < len(row) else None for idx in range(len(headers))]
        matrix.append(values)
    return headers, matrix


def _cell_xml(ref: str, value: Any, style_id: int | None = None) -> str:
    style_attr = f' s="{style_id}"' if style_id is not None else ""
    if value is None:
        return ""
    if isinstance(value, bool):
        return f'<c r="{ref}" t="b"{style_attr}><v>{1 if value else 0}</v></c>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float):
            text = repr(value)
        else:
            text = str(value)
        return f'<c r="{ref}"{style_attr}><v>{text}</v></c>'

    text = escape(str(value))
    return (
        f'<c r="{ref}" t="inlineStr"{style_attr}>'
        f'<is><t xml:space="preserve">{text}</t></is>'
        f"</c>"
    )


def _sheet_xml(
    headers: list[str],
    matrix: list[list[Any]],
    *,
    freeze_header: bool,
    autofilter: bool,
    header_style: bool,
) -> tuple[str, list[int]]:
    row_count = len(matrix) + (1 if headers else 0)
    column_count = len(headers)

    widths = [max(_text_width(header), 8) for header in headers]
    for row in matrix:
        for idx, value in enumerate(row):
            if idx < len(widths):
                widths[idx] = min(max(widths[idx], _text_width(value) + 2), 60)

    parts: list[str] = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        f'<worksheet xmlns="{EXCEL_NS}" xmlns:r="{REL_NS}">',
    ]

    if freeze_header or autofilter:
        parts.append("<sheetViews><sheetView workbookViewId=\"0\">")
        if freeze_header:
            parts.append(
                "<pane ySplit=\"1\" topLeftCell=\"A2\" activePane=\"bottomLeft\" state=\"frozen\"/>"
            )
            parts.append(
                "<selection pane=\"bottomLeft\" activeCell=\"A2\" sqref=\"A2\"/>"
            )
        parts.append("</sheetView></sheetViews>")

    parts.append(f'<dimension ref="{_sheet_dimension(row_count, column_count)}"/>')
    parts.append("<sheetFormatPr defaultRowHeight=\"15\"/>")

    if widths:
        parts.append("<cols>")
        for idx, width in enumerate(widths, start=1):
            parts.append(
                f'<col min="{idx}" max="{idx}" width="{max(width, 8):.2f}" customWidth="1"/>'
            )
        parts.append("</cols>")

    parts.append("<sheetData>")

    current_row = 1
    if headers:
        parts.append(f'<row r="{current_row}">')
        for idx, header in enumerate(headers, start=1):
            ref = f"{_column_name(idx)}{current_row}"
            parts.append(_cell_xml(ref, header, 1 if header_style else None))
        parts.append("</row>")
        current_row += 1

    for row in matrix:
        parts.append(f'<row r="{current_row}">')
        for idx, value in enumerate(row, start=1):
            ref = f"{_column_name(idx)}{current_row}"
            cell_xml = _cell_xml(ref, value, None)
            if cell_xml:
                parts.append(cell_xml)
        parts.append("</row>")
        current_row += 1

    parts.append("</sheetData>")

    if autofilter and headers:
        parts.append(f'<autoFilter ref="{_sheet_dimension(row_count, column_count)}"/>')

    parts.append("</worksheet>")
    return "".join(parts), widths


def _workbook_xml(sheet_names: Sequence[str]) -> str:
    sheet_entries = []
    for index, name in enumerate(sheet_names, start=1):
        sheet_entries.append(
            f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<workbook xmlns="{EXCEL_NS}" xmlns:r="{REL_NS}">'
        "<fileVersion appName=\"xl\"/>"
        "<workbookPr date1904=\"0\"/>"
        "<bookViews><workbookView xWindow=\"0\" yWindow=\"0\" windowWidth=\"28800\" windowHeight=\"16620\"/></bookViews>"
        f"<sheets>{''.join(sheet_entries)}</sheets>"
        "<calcPr calcId=\"124519\"/>"
        "</workbook>"
    )


def _workbook_rels_xml(sheet_count: int) -> str:
    rels = []
    for index in range(1, sheet_count + 1):
        rels.append(
            f'<Relationship Id="rId{index}" Type="{DOC_REL_NS}/worksheet" Target="worksheets/sheet{index}.xml"/>'
        )
    rels.append(
        f'<Relationship Id="rId{sheet_count + 1}" Type="{DOC_REL_NS}/styles" Target="styles.xml"/>'
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<Relationships xmlns="{PKG_REL_NS}">'
        f"{''.join(rels)}"
        "</Relationships>"
    )


def _root_rels_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<Relationships xmlns="{PKG_REL_NS}">'
        f'<Relationship Id="rId1" Type="{DOC_REL_NS}/officeDocument" Target="xl/workbook.xml"/>'
        f'<Relationship Id="rId2" Type="{DOC_REL_NS}/extended-properties" Target="docProps/app.xml"/>'
        f'<Relationship Id="rId3" Type="{DOC_REL_NS}/core-properties" Target="docProps/core.xml"/>'
        "</Relationships>"
    )


def _content_types_xml(sheet_count: int) -> str:
    overrides = []
    for index in range(1, sheet_count + 1):
        overrides.append(
            f'<Override PartName="/xl/worksheets/sheet{index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<Types xmlns="{CT_NS}">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
        '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        f"{''.join(overrides)}"
        "</Types>"
    )


def _styles_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<styleSheet xmlns="{EXCEL_NS}">'
        "<fonts count=\"2\">"
        "<font><sz val=\"11\"/><color theme=\"1\"/><name val=\"Calibri\"/><family val=\"2\"/><scheme val=\"minor\"/></font>"
        "<font><b/><sz val=\"11\"/><color rgb=\"FFFFFFFF\"/><name val=\"Calibri\"/><family val=\"2\"/></font>"
        "</fonts>"
        "<fills count=\"3\">"
        "<fill><patternFill patternType=\"none\"/></fill>"
        "<fill><patternFill patternType=\"gray125\"/></fill>"
        "<fill><patternFill patternType=\"solid\"><fgColor rgb=\"FF4F81BD\"/><bgColor indexed=\"64\"/></patternFill></fill>"
        "</fills>"
        "<borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders>"
        "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>"
        "<cellXfs count=\"2\">"
        "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/>"
        "<xf numFmtId=\"0\" fontId=\"1\" fillId=\"2\" borderId=\"0\" xfId=\"0\" applyFont=\"1\" applyFill=\"1\" applyAlignment=\"1\">"
        "<alignment horizontal=\"center\" vertical=\"center\" wrapText=\"1\"/>"
        "</xf>"
        "</cellXfs>"
        "<cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles>"
        "<dxfs count=\"0\"/>"
        "<tableStyles count=\"0\" defaultTableStyle=\"TableStyleMedium2\" defaultPivotStyle=\"PivotStyleLight16\"/>"
        "</styleSheet>"
    )


def _app_xml(sheet_names: Sequence[str]) -> str:
    titles = "".join(f"<vt:lpstr>{escape(name)}</vt:lpstr>" for name in sheet_names)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
        'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
        "<Application>PDFCompare</Application>"
        "<DocSecurity>0</DocSecurity>"
        "<ScaleCrop>false</ScaleCrop>"
        f"<HeadingPairs><vt:vector size=\"2\" baseType=\"variant\"><vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant><vt:variant><vt:i4>{len(sheet_names)}</vt:i4></vt:variant></vt:vector></HeadingPairs>"
        f"<TitlesOfParts><vt:vector size=\"{len(sheet_names)}\" baseType=\"lpstr\">{titles}</vt:vector></TitlesOfParts>"
        "<Company></Company>"
        "<LinksUpToDate>false</LinksUpToDate>"
        "<SharedDoc>false</SharedDoc>"
        "<HyperlinksChanged>false</HyperlinksChanged>"
        "<AppVersion>16.0300</AppVersion>"
        "</Properties>"
    )


def _core_xml(title: str, creator: str, created: datetime) -> str:
    created_iso = created.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<cp:coreProperties xmlns:cp="{CP_NS}" xmlns:dc="{DC_NS}" xmlns:dcterms="{DCTERMS_NS}" '
        f'xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="{XSI_NS}">'
        f"<dc:title>{escape(title)}</dc:title>"
        f"<dc:creator>{escape(creator)}</dc:creator>"
        "<cp:lastModifiedBy>PDFCompare</cp:lastModifiedBy>"
        f"<dcterms:created xsi:type=\"dcterms:W3CDTF\">{created_iso}</dcterms:created>"
        f"<dcterms:modified xsi:type=\"dcterms:W3CDTF\">{created_iso}</dcterms:modified>"
        "</cp:coreProperties>"
    )


@dataclass
class Workbook:
    """A tiny workbook abstraction that writes .xlsx files without dependencies."""

    title: str = "Workbook"
    creator: str = "PDFCompare"
    sheets: list[SheetSpec] = field(default_factory=list)

    def add_sheet(
        self,
        name: str,
        rows: Sequence[Any] | None = None,
        columns: Sequence[str] | None = None,
        *,
        freeze_header: bool = True,
        autofilter: bool = True,
        header_style: bool = True,
    ) -> SheetSpec:
        spec = SheetSpec(
            name=name,
            rows=list(rows or []),
            columns=list(columns) if columns is not None else None,
            freeze_header=freeze_header,
            autofilter=autofilter,
            header_style=header_style,
        )
        self.sheets.append(spec)
        return spec

    def save(self, path: str | Path) -> Path:
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        names: set[str] = set()
        sheet_specs: list[tuple[str, str]] = []
        effective_sheets = self.sheets or [SheetSpec(name="Sheet1", rows=[])]
        for spec in effective_sheets:
            sheet_name = sanitize_sheet_name(spec.name, names)
            headers, matrix = _coerce_rows(spec.rows, spec.columns)
            sheet_xml, widths = _sheet_xml(
                headers,
                matrix,
                freeze_header=spec.freeze_header,
                autofilter=spec.autofilter,
                header_style=spec.header_style,
            )
            sheet_specs.append((sheet_name, sheet_xml))

        created = datetime.now(timezone.utc)
        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", _content_types_xml(len(sheet_specs)))
            zf.writestr("_rels/.rels", _root_rels_xml())
            zf.writestr("docProps/app.xml", _app_xml([name for name, _ in sheet_specs]))
            zf.writestr("docProps/core.xml", _core_xml(self.title, self.creator, created))
            zf.writestr("xl/workbook.xml", _workbook_xml([name for name, _ in sheet_specs]))
            zf.writestr("xl/_rels/workbook.xml.rels", _workbook_rels_xml(len(sheet_specs)))
            zf.writestr("xl/styles.xml", _styles_xml())

            for index, (_, sheet_xml) in enumerate(sheet_specs, start=1):
                zf.writestr(f"xl/worksheets/sheet{index}.xml", sheet_xml)

        return out_path


def write_xlsx(path: str | Path, sheets: Sequence[SheetSpec], *, title: str = "Workbook", creator: str = "PDFCompare") -> Path:
    """Write a workbook containing the provided worksheets."""

    workbook = Workbook(title=title, creator=creator)
    workbook.sheets.extend(sheets)
    return workbook.save(path)
