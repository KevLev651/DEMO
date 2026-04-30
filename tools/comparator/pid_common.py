import os
import re

import fitz


ROOT_RE = re.compile(r"^(R[LM]\d*-[A-Z]+-\d+[A-Z]?|R[LM]\d*-LINE)$")
INSTR_RE = re.compile(r"^[A-Z]{1,5}-\d+[A-Z0-9]*$")
SHEET_FILENAME_RE = re.compile(r"\\(DG\d{4,})\.dwg", re.IGNORECASE)
SHEET_ID_RE = re.compile(r"^DG\d{4,}$")


def validate_input_file(path, allowed_ext):
    if not path:
        raise ValueError("A file path is required.")

    full_path = os.path.abspath(path)
    if not os.path.isfile(full_path):
        raise FileNotFoundError(f"File not found: {full_path}")

    if allowed_ext and os.path.splitext(full_path)[1].lower() not in allowed_ext:
        raise ValueError(f"Unsupported file type: {full_path}")

    return full_path


def validate_pdf_content(pdf_path):
    try:
        with fitz.open(pdf_path) as doc:
            if len(doc) == 0:
                raise ValueError("The uploaded PDF is empty.")
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("The uploaded PDF could not be opened or read.") from exc


def known_sheet_fallbacks():
    raw_value = os.environ.get("PIDTOOL_SHEET_FALLBACKS", "DG7002:6")
    fallbacks = {}
    for item in raw_value.split(","):
        if ":" not in item:
            continue
        sheet_id, page_num = item.split(":", 1)
        sheet_id = sheet_id.strip().upper()
        try:
            page_num = int(page_num.strip())
        except ValueError:
            continue
        if SHEET_ID_RE.match(sheet_id) and page_num > 0:
            fallbacks[sheet_id] = page_num
    return fallbacks


def extract_sheet_id_from_page(page):
    blocks = page.get_text("blocks")
    words = page.get_text("words")
    page_height = page.rect.height

    filename_id = None
    for block in blocks:
        match = SHEET_FILENAME_RE.search(block[4])
        if match:
            filename_id = match.group(1)
            break

    bottom_matches = [
        (word[4], word[1])
        for word in words
        if SHEET_ID_RE.match(word[4]) and word[1] > page_height * 0.80
    ]
    bottom_id = max(bottom_matches, key=lambda value: value[1])[0] if bottom_matches else None
    return filename_id or bottom_id


def build_pdf_sheet_maps(doc):
    sheet_to_page = {}
    page_to_sheet = {}

    for page_num, page in enumerate(doc, start=1):
        sheet_id = extract_sheet_id_from_page(page)
        if sheet_id:
            sheet_to_page[sheet_id] = page_num
            page_to_sheet[page_num] = sheet_id

    for sheet_id, page_num in known_sheet_fallbacks().items():
        if sheet_id not in sheet_to_page:
            sheet_to_page[sheet_id] = page_num
            page_to_sheet[page_num] = sheet_id

    return sheet_to_page, page_to_sheet


def build_pdf_sheet_map_from_path(pdf_path):
    with fitz.open(pdf_path) as doc:
        sheet_to_page, _page_to_sheet = build_pdf_sheet_maps(doc)
    return sheet_to_page
