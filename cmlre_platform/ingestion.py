"""
CMLRE — Modular Ingestion Pipelines  (robust version)
Improvements over v1:
  - Encoding auto-detection (UTF-8, ISO-8859-1, Windows-1252, etc.)
  - Fuzzy column name matching for real government CSVs
  - Multi-header / metadata row skipping
  - Date normalisation to ISO 8601
  - Mixed-type / dirty value cleaning
  - Domain confidence score returned so caller can warn on low confidence
"""

import io
import re
import math
from typing import Optional
from difflib import SequenceMatcher

import pandas as pd
import numpy as np

# ─── Encoding detection ───────────────────────────────────────────────────────

def decode_bytes(raw: bytes) -> tuple[str, str]:
    """
    Try common encodings in order. Returns (text, encoding_used).
    Falls back to latin-1 which never fails (every byte is valid).
    """
    for enc in ("utf-8-sig", "utf-8", "cp1252", "iso-8859-1", "latin-1"):
        try:
            return raw.decode(enc), enc
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("latin-1", errors="replace"), "latin-1"


# ─── Multi-header / metadata row detection ───────────────────────────────────

def find_header_row(lines: list[str], max_scan: int = 20) -> int:
    """
    Scan the first max_scan lines to find the actual header row.
    Heuristic: the header row has the most comma-separated tokens that look
    like column names (non-numeric, reasonable length).
    """
    best_row, best_score = 0, 0
    for i, line in enumerate(lines[:max_scan]):
        parts = [p.strip().strip('"') for p in line.split(",")]
        score = sum(
            1 for p in parts
            if p and not re.match(r"^-?\d+(\.\d+)?$", p) and len(p) < 60
        )
        if score > best_score:
            best_score, best_row = score, i
    return best_row


def load_csv_robust(raw: bytes) -> tuple[pd.DataFrame, dict]:
    """
    Load a CSV from raw bytes with encoding detection and header sniffing.
    Returns (dataframe, meta) where meta has encoding, header_row, warnings.
    """
    text, encoding = decode_bytes(raw)
    warnings = []

    lines = text.splitlines()
    header_row = find_header_row(lines)
    if header_row > 0:
        warnings.append(f"Skipped {header_row} metadata row(s) before header.")

    try:
        df = pd.read_csv(
            io.StringIO(text),
            skiprows=header_row,
            on_bad_lines="skip",
            low_memory=False,
        )
    except Exception as e:
        # Last resort — try with python engine and skip bad lines
        df = pd.read_csv(
            io.StringIO(text),
            skiprows=header_row,
            engine="python",
            on_bad_lines="skip",
        )
        warnings.append(f"Used fallback CSV parser: {e}")

    # Drop completely empty rows and columns
    df.dropna(how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)

    return df, {"encoding": encoding, "header_row": header_row, "warnings": warnings}


# ─── Darwin Core canonical aliases ───────────────────────────────────────────
# NOTE: "class" and "order" intentionally excluded — taxonomy parser handles
# them via .iloc to avoid pandas Series ambiguity.

DARWIN_CORE_MAP = {
    "lat": "decimal_latitude", "latitude": "decimal_latitude",
    "lon": "decimal_longitude", "longitude": "decimal_longitude",
    "long": "decimal_longitude",
    "depth": "depth_m", "depth_m": "depth_m",
    "common_name": "vernacular_name",
    "date": "event_date", "event_date": "event_date",
    "datetime": "event_date", "collection_date": "event_date",
    "sample_date": "event_date",
}

# ─── Fuzzy column matching ────────────────────────────────────────────────────

# Extended synonym map — covers common government dataset column names
COLUMN_SYNONYMS: dict[str, list[str]] = {
    # Oceanography
    "temperature_c":    ["temp", "temperature", "sst", "sea_surface_temp",
                         "water_temp", "temp_c", "temp(c)", "temperature(c)",
                         "in_situ_temp", "t_deg_c"],
    "salinity_psu":     ["salinity", "sal", "psal", "practical_salinity",
                         "salinity(psu)", "sal_psu"],
    "dissolved_oxygen": ["do", "oxygen", "doxy", "dissolved_o2", "o2",
                         "do_mg_l", "oxygen_ml_l", "dissolved_oxygen_mg_l"],
    "ph":               ["ph_value", "seawater_ph", "ph_nbs", "ph_total"],
    "chlorophyll_a":    ["chla", "chl_a", "chlorophyll", "chl",
                         "chlorophyll_a_ug_l", "fluorescence"],
    "turbidity":        ["turb", "ntu", "turbidity_ntu", "secchi"],
    "current_speed_ms": ["current_speed", "speed", "vel", "velocity",
                         "current_velocity"],
    "station_id":       ["station", "stn", "site", "site_id", "cruise_station"],
    "depth_m":          ["depth", "dep", "depth_m", "depth(m)", "water_depth",
                         "bottom_depth", "z"],
    "decimal_latitude": ["lat", "latitude", "latitude_n", "lat_dd", "y"],
    "decimal_longitude":["lon", "longitude", "long", "longitude_e",
                         "lon_dd", "x"],
    "event_date":       ["date", "datetime", "collection_date", "sample_date",
                         "observation_date", "survey_date", "cast_date"],
    # Fisheries
    "scientific_name":  ["species", "scientific_name", "taxon", "taxa",
                         "species_name", "binomial", "latin_name",
                         "scientific name"],
    "vernacular_name":  ["common_name", "english_name", "local_name",
                         "fish_name", "vernacular", "common name"],
    "catch_weight_kg":  ["catch_weight", "weight_kg", "catch_kg",
                         "landings_kg", "catch_weight_kg", "weight",
                         "catch(kg)", "total_catch", "landings"],
    "catch_count":      ["count", "number", "catch_count", "catch_number",
                         "abundance", "n_individuals", "no_of_fish"],
    "gear_type":        ["gear", "fishing_gear", "gear_type", "method",
                         "fishing_method", "gear_code"],
    "vessel_id":        ["vessel", "boat_id", "vessel_id", "boat",
                         "vessel_name", "boat_name"],
    "fishing_area":     ["area", "zone", "fishing_zone", "region",
                         "fishing_ground", "iccat_area"],
    "length_cm":        ["length", "fish_length", "total_length",
                         "standard_length", "length_cm", "tl_cm", "sl_cm"],
    "weight_g":         ["weight", "fish_weight", "body_weight",
                         "weight_g", "wt_g"],
    "maturity_stage":   ["maturity", "maturity_stage", "gonads",
                         "reproductive_stage"],
    # Taxonomy
    "kingdom":          ["kingdom", "regnum"],
    "phylum":           ["phylum", "division"],
    "class_name":       ["class", "classis"],
    "order_name":       ["order", "ordo"],
    "family":           ["family", "familia"],
    "genus":            ["genus"],
    "species":          ["species", "specific_epithet"],
    "iucn_status":      ["iucn", "iucn_status", "red_list", "conservation_status",
                         "threat_status"],
    "taxon_id":         ["taxon_id", "aphia_id", "worms_id", "itis_id"],
    "gbif_id":          ["gbif_id", "gbif_key", "gbif"],
    "habitat":          ["habitat", "environment", "biome"],
    # Morphology
    "length_mm":        ["length_mm", "otolith_length", "oto_length",
                         "sagittal_length", "l_mm"],
    "width_mm":         ["width_mm", "otolith_width", "oto_width", "w_mm"],
    "area_mm2":         ["area", "area_mm2", "otolith_area", "oto_area"],
    "perimeter_mm":     ["perimeter", "perimeter_mm", "circumference"],
    "circularity":      ["circularity", "circ", "shape_circularity"],
    "aspect_ratio":     ["aspect_ratio", "ar", "shape_ar"],
    "form_factor":      ["form_factor", "ff", "shape_ff"],
    "roundness":        ["roundness", "round", "shape_roundness"],
    "sample_id":        ["sample_id", "sample", "specimen_id", "otolith_id",
                         "individual_id"],
    "fish_length_cm":   ["fish_length", "total_length_cm", "body_length"],
    "fish_weight_g":    ["fish_weight", "body_weight_g", "somatic_weight"],
    "estimated_age_yr": ["age", "estimated_age", "age_years",
                         "age_yr", "otolith_age"],
    # eDNA
    "read_count":       ["reads", "read_count", "n_reads", "sequence_count",
                         "count", "total_reads"],
    "relative_abundance":["rel_abundance", "relative_abundance", "abundance",
                          "proportion", "rel_abund"],
    "target_gene":      ["gene", "target_gene", "marker", "locus",
                         "genetic_marker"],
    "sequencing_platform":["platform", "sequencer", "instrument",
                            "sequencing_platform"],
    "primer_pair":      ["primers", "primer_pair", "primer_set",
                         "amplicon_primer"],
    "sequence_quality": ["quality", "seq_quality", "phred_score", "qscore"],
    "detection_status": ["detected", "detection", "presence",
                         "detection_status", "presence_absence"],
}


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def fuzzy_match_columns(df_cols: list[str]) -> dict[str, str]:
    """
    For each DataFrame column, find the best canonical target name.
    Returns {original_col: canonical_col} for matches above threshold.
    Only maps each target once (first best match wins).
    """
    normalised = {c: re.sub(r"[\s\-/\\()]+", "_", c.lower().strip()) for c in df_cols}
    mapping: dict[str, str] = {}
    used_targets: set[str] = set()

    for orig, norm in normalised.items():
        best_target, best_score = None, 0.0
        for canonical, synonyms in COLUMN_SYNONYMS.items():
            if canonical in used_targets:
                continue
            for syn in synonyms:
                syn_norm = re.sub(r"[\s\-/\\()]+", "_", syn.lower())
                # Exact match
                if norm == syn_norm:
                    score = 1.0
                else:
                    score = _similarity(norm, syn_norm)
                if score > best_score:
                    best_score, best_target = score, canonical
        if best_target and best_score >= 0.82 and best_target not in used_targets:
            mapping[orig] = best_target
            used_targets.add(best_target)

    return mapping


# ─── Domain detection ─────────────────────────────────────────────────────────

DOMAIN_SIGNATURES = {
    "oceanography": {
        "temperature_c", "salinity_psu", "dissolved_oxygen", "ph",
        "chlorophyll_a", "turbidity", "current_speed_ms", "station_id",
        # raw synonyms still scored before remapping
        "temperature", "temp", "salinity", "chlorophyll",
    },
    "fisheries": {
        "catch_weight_kg", "catch_count", "gear_type", "vessel_id",
        "fishing_area", "maturity_stage", "length_cm", "weight_g",
        "catch_weight", "gear", "vessel",
    },
    "taxonomy": {
        "kingdom", "phylum", "class", "order", "family", "genus",
        "iucn_status", "taxon_id", "gbif_id", "authority", "habitat",
    },
    "morphology": {
        "length_mm", "width_mm", "area_mm2", "perimeter_mm",
        "circularity", "aspect_ratio", "form_factor", "sample_id",
        "fish_length_cm", "estimated_age_yr",
    },
    "edna": {
        "read_count", "relative_abundance", "target_gene",
        "sequencing_platform", "primer_pair", "sequence_quality",
        "detection_status", "reads", "amplicon",
    },
}


def detect_domain(df: pd.DataFrame) -> tuple[str, float]:
    """
    Returns (domain, confidence_0_to_1).
    Confidence = matched_signatures / total_signatures_for_domain.
    """
    cols = {c.lower().strip() for c in df.columns}
    # Also include normalised versions
    cols |= {re.sub(r"[\s\-/\\()]+", "_", c) for c in cols}

    scores = {}
    for domain, sigs in DOMAIN_SIGNATURES.items():
        matched = len(cols & sigs)
        scores[domain] = matched / len(sigs) if sigs else 0

    best = max(scores, key=scores.get)
    confidence = scores[best]
    return (best if confidence > 0 else "oceanography"), confidence


# ─── Date normalisation ───────────────────────────────────────────────────────

DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
    "%d.%m.%Y", "%Y/%m/%d", "%d %b %Y", "%d-%b-%Y",
    "%Y%m%d", "%b %d %Y", "%B %d %Y",
]


def normalise_date(val) -> Optional[str]:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "na", "n/a", "--"):
        return None
    # Already ISO
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    for fmt in DATE_FORMATS:
        try:
            from datetime import datetime
            return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s  # return as-is if unparseable


# ─── Value cleaning ───────────────────────────────────────────────────────────

NULL_STRINGS = {"na", "n/a", "nan", "none", "null", "--", "-", "nd",
                "not detected", "missing", "", ".", "unknown"}


def clean_value(val):
    """Turn dirty values into proper Python scalars."""
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if isinstance(val, str):
        stripped = val.strip()
        if stripped.lower() in NULL_STRINGS:
            return None
        # Remove thousands separators and stray units  e.g. "1,234.5 kg"
        cleaned = re.sub(r"[,\s]", "", stripped.split()[0]) if stripped else stripped
        try:
            return int(cleaned)
        except ValueError:
            pass
        try:
            return float(cleaned)
        except ValueError:
            pass
        return stripped
    return val


# ─── Column normalisation ─────────────────────────────────────────────────────

def normalise_columns(df: pd.DataFrame, apply_fuzzy: bool = True) -> pd.DataFrame:
    df = df.copy()
    # Step 1: lowercase + underscore
    df.columns = [re.sub(r"[\s\-/\\()]+", "_", c.lower().strip()) for c in df.columns]
    # Step 2: apply Darwin Core exact aliases
    df = df.rename(columns={k: v for k, v in DARWIN_CORE_MAP.items() if k in df.columns})
    # Step 3: fuzzy match remaining columns
    if apply_fuzzy:
        fuzz = fuzzy_match_columns(list(df.columns))
        df = df.rename(columns=fuzz)
    # Step 4: deduplicate columns (keep first)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


# ─── Scalar cell extractor ────────────────────────────────────────────────────

def _get(df: pd.DataFrame, col: str, i: int):
    raw = df[col].iloc[i]
    if isinstance(raw, pd.Series):
        raw = raw.iloc[0] if len(raw) > 0 else None
    return clean_value(raw)


# ─── Domain parsers ───────────────────────────────────────────────────────────

def parse_oceanography(df: pd.DataFrame) -> tuple[list[dict], dict]:
    df = normalise_columns(df)
    targets = [
        "decimal_latitude", "decimal_longitude", "depth_m", "event_date",
        "temperature_c", "salinity_psu", "dissolved_oxygen", "ph",
        "chlorophyll_a", "turbidity", "current_speed_ms", "station_id",
    ]
    records = []
    for i in range(len(df)):
        rec = {}
        for t in targets:
            if t in df.columns:
                val = _get(df, t, i)
                rec[t] = normalise_date(val) if t == "event_date" else val
        records.append(rec)

    stats: dict = {}
    for col in ["temperature_c", "salinity_psu", "dissolved_oxygen", "ph", "chlorophyll_a"]:
        vals = [r[col] for r in records if r.get(col) is not None]
        if vals:
            try:
                fvals = [float(v) for v in vals]
                stats[col] = {
                    "mean": round(float(np.mean(fvals)), 3),
                    "min":  round(float(np.min(fvals)), 3),
                    "max":  round(float(np.max(fvals)), 3),
                }
            except Exception:
                pass

    avg_temp = stats.get("temperature_c", {}).get("mean")
    return records, {"stats": stats, "avg_temp": avg_temp, "species_count": 0}


def parse_fisheries(df: pd.DataFrame) -> tuple[list[dict], dict]:
    df = normalise_columns(df)
    targets = [
        "scientific_name", "vernacular_name", "taxon_rank",
        "catch_weight_kg", "catch_count", "gear_type", "vessel_id",
        "fishing_area", "decimal_latitude", "decimal_longitude",
        "event_date", "maturity_stage", "length_cm", "weight_g",
    ]
    records = []
    for i in range(len(df)):
        rec = {}
        for t in targets:
            if t in df.columns:
                val = _get(df, t, i)
                rec[t] = normalise_date(val) if t == "event_date" else val
        records.append(rec)

    species = {r["scientific_name"] for r in records if r.get("scientific_name")}
    total_catch = 0.0
    for r in records:
        w = r.get("catch_weight_kg")
        try:
            total_catch += float(w) if w is not None else 0
        except (TypeError, ValueError):
            pass

    return records, {
        "avg_temp": None,
        "species_count": len(species),
        "total_catch_kg": round(total_catch, 2),
    }


def parse_taxonomy(df: pd.DataFrame) -> tuple[list[dict], dict]:
    # Raw normalise only — no fuzzy remap of class/order to avoid Series issues
    df = df.copy()
    df.columns = [re.sub(r"[\s\-/\\()]+", "_", c.lower().strip()) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    col_targets = {
        "kingdom": "kingdom", "phylum": "phylum",
        "class": "class_name", "order": "order_name",
        "family": "family", "genus": "genus", "species": "species",
        "scientific_name": "scientific_name",
        "vernacular_name": "vernacular_name",
        "taxon_id": "taxon_id", "gbif_id": "gbif_id",
        "iucn_status": "iucn_status", "habitat": "habitat",
        "distribution": "distribution", "authority": "authority",
    }
    present = {src: tgt for src, tgt in col_targets.items() if src in df.columns}

    records = []
    for i in range(len(df)):
        rec = {}
        for src, tgt in present.items():
            rec[tgt] = clean_value(df[src].iloc[i])
        if not rec.get("scientific_name") and rec.get("genus") and rec.get("species"):
            rec["scientific_name"] = f"{rec['genus']} {rec['species']}"
        records.append(rec)

    species_count = len({r.get("scientific_name") for r in records if r.get("scientific_name")})
    iucn_counts: dict[str, int] = {}
    for r in records:
        s = r.get("iucn_status")
        if s:
            iucn_counts[str(s)] = iucn_counts.get(str(s), 0) + 1

    return records, {"avg_temp": None, "species_count": species_count, "iucn_summary": iucn_counts}


def parse_morphology(df: pd.DataFrame) -> tuple[list[dict], dict]:
    df = normalise_columns(df)
    float_targets = [
        "length_mm", "width_mm", "area_mm2", "perimeter_mm",
        "aspect_ratio", "circularity", "form_factor", "rectangularity",
        "roundness", "fish_length_cm", "fish_weight_g", "estimated_age_yr",
        "decimal_latitude", "decimal_longitude",
    ]
    str_targets = ["scientific_name", "sample_id", "image_filename",
                   "image_path", "event_date", "notes"]

    records = []
    for i in range(len(df)):
        rec = {}
        for f in float_targets:
            if f in df.columns:
                val = _get(df, f, i)
                try:
                    rec[f] = None if val is None else float(val)
                except (TypeError, ValueError):
                    rec[f] = None
        for f in str_targets:
            if f in df.columns:
                val = _get(df, f, i)
                if f == "event_date":
                    rec[f] = normalise_date(val)
                else:
                    rec[f] = None if val is None else str(val)
        if rec.get("length_mm") and rec.get("width_mm") and not rec.get("aspect_ratio"):
            try:
                rec["aspect_ratio"] = round(rec["length_mm"] / rec["width_mm"], 4)
            except Exception:
                pass
        records.append(rec)

    stats: dict = {}
    for f in ["length_mm", "width_mm", "area_mm2", "circularity"]:
        vals = [r[f] for r in records if r.get(f) is not None]
        if vals:
            stats[f] = {
                "mean": round(float(np.mean(vals)), 3),
                "min":  round(float(np.min(vals)), 3),
                "max":  round(float(np.max(vals)), 3),
            }

    species = {r.get("scientific_name") for r in records if r.get("scientific_name")}
    return records, {"avg_temp": None, "species_count": len(species), "shape_stats": stats}


def parse_edna(df: pd.DataFrame) -> tuple[list[dict], dict]:
    df = normalise_columns(df)
    targets = [
        "sample_id", "decimal_latitude", "decimal_longitude", "depth_m",
        "event_date", "target_gene", "sequencing_platform", "primer_pair",
        "scientific_name", "taxon_rank", "read_count", "relative_abundance",
        "sequence_quality", "detection_status", "notes",
    ]
    records = []
    for i in range(len(df)):
        rec = {}
        for t in targets:
            if t in df.columns:
                val = _get(df, t, i)
                if t == "event_date":
                    rec[t] = normalise_date(val)
                elif t == "detection_status":
                    if val is None:
                        rec[t] = None
                    elif isinstance(val, bool):
                        rec[t] = val
                    elif isinstance(val, (int, float)):
                        rec[t] = bool(int(val))
                    else:
                        rec[t] = str(val).strip().lower() in ("true", "1", "yes", "detected", "present")
                else:
                    rec[t] = val
        records.append(rec)

    species = {r.get("scientific_name") for r in records if r.get("scientific_name")}
    total_reads = 0
    for r in records:
        cnt = r.get("read_count")
        try:
            total_reads += int(cnt) if cnt is not None else 0
        except (TypeError, ValueError):
            pass
    detected = sum(1 for r in records if r.get("detection_status"))

    return records, {
        "avg_temp": None,
        "species_count": len(species),
        "total_reads": total_reads,
        "detection_rate": round(detected / len(records), 3) if records else 0,
    }


# ─── Dispatcher ───────────────────────────────────────────────────────────────

PARSERS = {
    "oceanography": parse_oceanography,
    "fisheries":    parse_fisheries,
    "taxonomy":     parse_taxonomy,
    "morphology":   parse_morphology,
    "edna":         parse_edna,
}


def ingest(
    df: pd.DataFrame,
    domain: Optional[str] = None,
) -> tuple[str, list[dict], dict]:
    """
    Returns (domain, records, stats).
    stats includes 'confidence' and 'warnings' keys.
    """
    detected, confidence = detect_domain(df)
    if domain is None or domain == "auto":
        domain = detected

    parser = PARSERS.get(domain, parse_oceanography)
    records, stats = parser(df)
    stats["confidence"] = round(confidence, 3)
    stats["domain_used"] = domain
    return domain, records, stats


def ingest_raw(
    raw: bytes,
    domain: Optional[str] = None,
) -> tuple[str, list[dict], dict, dict]:
    """
    Full pipeline from raw bytes.
    Returns (domain, records, stats, load_meta).
    load_meta has encoding, header_row, warnings.
    """
    df, load_meta = load_csv_robust(raw)
    domain_out, records, stats = ingest(df, domain)
    stats["load_warnings"] = load_meta.get("warnings", [])
    stats["encoding_detected"] = load_meta.get("encoding", "utf-8")
    return domain_out, records, stats, load_meta