"""Profile data for P&ID tag extraction.

The scanner engine should stay generic. Customer/project-specific language,
tag families, deny terms, and confidence weights live in profiles so later
jobs can swap or extend them without patching extraction code.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import FrozenSet, Mapping, Tuple


@dataclass(frozen=True)
class PidProfile:
    isa_instrument_codes: FrozenSet[str] = field(default_factory=frozenset)
    equipment_prefixes: FrozenSet[str] = field(default_factory=frozenset)
    deny_prefixes: FrozenSet[str] = field(default_factory=frozenset)
    legend_markers: Tuple[str, ...] = ()
    noise_context: Tuple[str, ...] = ()
    line_noise_words: Tuple[str, ...] = ()
    line_word_components: FrozenSet[str] = field(default_factory=frozenset)
    bare_infer_codes: FrozenSet[str] = field(default_factory=frozenset)
    custom_review_instrument_codes: FrozenSet[str] = field(default_factory=frozenset)
    confidence_weights: Mapping[str, float] = field(default_factory=dict)
    rating_high_threshold: float = 0.90
    rating_medium_threshold: float = 0.70
    zone_right_title_min: float = 0.84
    zone_bottom_title_min: float = 0.90
    zone_top_edge_max: float = 0.04
    zone_left_edge_max: float = 0.02
    zone_right_edge_min: float = 0.98
    stacked_overlap_threshold: float = 0.50
    stacked_max_gap_ratio: float = 2.0
    bare_infer_max_dx: float = 420.0
    bare_infer_max_dy: float = 420.0
    sheet_title_right_min: float = 0.65
    sheet_title_bottom_min: float = 0.55
    sheet_preferred_prefixes: Tuple[str, ...] = ()
    repetition_bonus_max: float = 0.10
    repetition_bonus_each: float = 0.025

    # Grammar strings are intentionally profile-owned. The default profile is
    # broad, but a job profile can override these without changing code.
    line_tag_pattern: str = (
        r'''(?<![A-Z0-9/\-])(?P<size>\d+\s+\d+/\d+|\d+/\d+|\d+(?:\.\d+)?)\s*"?\s*-\s*'''
        r'''(?P<body>[A-Z][A-Z0-9]{0,9}(?:-[A-Z0-9]{1,10}){1,5})'''
    )
    asset_tag_pattern: str = r"^[A-Z]{2,8}\d{3,7}[A-Z]?$"
    hyphen_root_pattern: str = r"^R[LM]\d*(?:-[A-Z]{2,6})?-\d{3,5}[A-Z]?$"
    compact_hyphen_root_pattern: str = r"^(R[LM]\d*)-([A-Z]{2,6})(\d{3,5}[A-Z]?)$"
    isa_child_pattern: str = r"^(?P<code>[A-Z]{1,4})-?(?P<num>\d{1,4})(?P<suf>[A-Z]?)$"
    equip_child_pattern: str = r"^(?P<code>[A-Z]{2,5})(?P<num>\d{2,5})(?P<suf>[A-Z]?)$"
    full_hyphen_tag_pattern: str = r"\b[A-Z]{1,10}\d{0,5}(?:-[A-Z0-9]{1,14}){2,8}\b"
    dwg_file_pattern: str = (
        r"(?:^|[\\/])(?:[A-Z0-9_ -]*-)?([A-Z]{1,6}\d{3,7}[A-Z]?)\s*(?:GMP)?\.dwg\b"
    )
    sheet_id_pattern: str = r"^[A-Z]{1,6}\d{3,7}[A-Z]?$"

    def with_updates(self, **updates):
        """Return a copy with profile fields overridden."""
        return replace(self, **updates)


DEFAULT_CONFIDENCE_WEIGHTS = {
    "stacked:isa": 0.88,
    "stacked:equip": 0.50,
    "fulltag:isa": 0.86,
    "fulltag:equip": 0.80,
    "line:size-prefix": 0.66,
    "standalone:asset": 0.66,
    "inferred:bare-isa": 0.46,
    "samepage:isa": 0.68,
    "standalone:isa": 0.40,
    "unknown": 0.60,
    "drawing_zone_bonus": 0.06,
    "title_block_penalty": -0.22,
    "edge_zone_penalty": -0.10,
    "legend_page_penalty": -0.25,
    "process_page_bonus": 0.02,
    "unmapped_sheet_penalty": -0.06,
    "mapped_sheet_bonus": 0.03,
    "noisy_context_penalty": -0.22,
    "valve_group_penalty": -0.18,
    "custom_code_penalty": -0.14,
    "matched_source_bonus": 0.06,
    "ocr_only_penalty": -0.08,
    "source_conflict_penalty": -0.15,
}


DEFAULT_PROFILE = PidProfile(
    isa_instrument_codes=frozenset({
        "HV", "CV", "XV", "SV", "RV", "MV", "AV", "ZV", "BV", "PV", "FV", "LV", "TV",
        "FCV", "TCV", "PCV", "LCV", "ACV", "HCV",
        "PSV", "TSV", "PRV", "FRV", "LRV", "TRV",
        "CKV", "CHV", "FLR", "TVI", "PVI",
        "PI", "TI", "LI", "FI", "AI", "SI", "VI", "WI", "JI", "II", "ZI",
        "PT", "TT", "LT", "FT", "AT", "ST", "VT", "WT", "ZT",
        "PIT", "TIT", "LIT", "FIT", "AIT", "SIT", "WIT",
        "PDT", "PDIT", "FDT", "TDT", "LDT", "ADT",
        "PE", "TE", "LE", "FE", "AE", "SE", "VE", "ZE",
        "FIC", "PIC", "LIC", "TIC", "AIC", "HIC", "SIC",
        "FC", "PC", "LC", "TC", "AC",
        "PS", "TS", "LS", "FS", "AS", "HS", "ZS", "YS",
        "PSL", "PSH", "TSL", "TSH", "LSL", "LSH", "FSL", "FSH",
        "SOV", "SOL",
        "PAL", "PAH", "TAL", "TAH", "LAL", "LAH", "FAL", "FAH",
        "PG", "TG", "LG", "FG",
        "FR", "PR", "TR", "LR",
        "FQ", "FQI", "FQIT",
        "FVI", "TP", "WV",
        "FO", "PO", "TO",
        "AL", "AH", "AE", "AR",
        "AG", "BFP", "CS", "DM", "FDR", "FLT", "FTR", "INJ", "LFT",
        "PB", "PBL", "PL", "PMP", "SC", "STR", "TMV", "UV", "VLV", "VR",
        "WE", "WIT", "WQ", "XC", "XY", "XYS", "YI", "ZEC", "ZSC", "ZSO",
        "ZSS",
    }),
    equipment_prefixes=frozenset({
        "VBU", "CHR", "CON", "TNK", "BIN", "CER", "BLW", "VG", "FAN", "CYC", "DCB",
        "VPM", "BFP", "SKID", "HTX", "HX", "HE", "MTR", "PMP", "PUMP", "TANK", "VES",
        "VSL", "CMP", "COMP", "COL", "RCV", "DRM", "DRUM", "STG", "SEP", "CKR",
        "AGC", "GRR", "MXR", "MIXER", "FDR", "FEED", "HPR", "HOPPER", "SLO", "SILO",
        "EVA", "EVAP", "REC", "COOL", "HTR", "HEAT", "BLR", "BOIL", "PVT", "FLT",
        "STR", "FIL", "FLTR", "AGT", "AGITATOR",
        "ACC", "BLD", "CDR", "CHR", "CIP", "CLN", "COM", "COP", "CPK", "CPN",
        "CSR", "DEH", "DES", "DPL", "DVR", "DRY", "FLR", "HEX", "HOD", "IJM",
        "IMJ", "ISP", "KIT", "LBR", "LDR", "LHT", "LNR", "LUB", "MPC", "OZG",
        "PAL", "PAW", "PNL", "PRS", "REJ", "RIN", "RLC", "ROB", "RVL", "SGR",
        "SKD", "SLV", "SSU", "SWR", "TLT", "TPK", "UNL", "VCR", "WSR", "WTS",
    }),
    deny_prefixes=frozenset({
        "ANSI", "BA", "DG", "DWG", "ISO", "ISA", "PDF", "REV", "F", "NO",
        "NTS", "TYP", "SIM", "REF", "SCH", "CLS", "DIN", "STD",
        "NON", "XX",
    }),
    legend_markers=(
        "symbol description", "equipment identifiers", "identification letters",
        "table is per ansi/isa", "major equipment key", "minor equipment key",
        "sanitary valves", "isa-5.1", "legend sheet",
    ),
    noise_context=(
        "filename:", "last saved", "plotted:", "registered engineering firm",
        "drawing no", "sheet title", "designed by", "reviewed by", "approved by",
        "project number", "date issued",
    ),
    line_noise_words=(
        "CONDUIT", "NON-WASHDOWN", "PIPE SPEC", "SANITARY", "WASHDOWN",
    ),
    line_word_components=frozenset({"FLEX"}),
    bare_infer_codes=frozenset({"BV", "CV", "FLT", "HV", "PI", "PR", "RV", "TR", "XV"}),
    custom_review_instrument_codes=frozenset({
        "AG", "BFP", "CS", "DM", "FDR", "FTR", "INJ", "LFT", "PMP", "STR", "TP", "VR", "WV",
    }),
    confidence_weights=DEFAULT_CONFIDENCE_WEIGHTS,
    sheet_preferred_prefixes=("DG",),
)
