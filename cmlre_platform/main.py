"""
CMLRE Unified Marine Data Platform — FastAPI Backend  (robust version)
"""

from fastapi import FastAPI, UploadFile, File, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import pandas as pd
import numpy as np
import io
import math
import requests as http_req

from database import (
    SessionLocal, MarineRecord,
    OceanographyRecord, FisheriesRecord,
    TaxonomyRecord, MorphologyRecord, EDNARecord
)
from ingestion import ingest_raw, detect_domain, load_csv_robust

app = FastAPI(
    title="CMLRE Marine Data Platform API",
    description="Unified API for oceanographic, fisheries, taxonomy, morphology, and eDNA data",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─── Universal serialisation ──────────────────────────────────────────────────

def _clean(val):
    """Convert any DB/pandas value to a JSON-safe Python scalar."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        try:
            return bool(int.from_bytes(val, "little"))
        except Exception:
            try:
                return val.decode("utf-8")
            except Exception:
                return None
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(val, np.bool_):
        return bool(val)
    if isinstance(val, float):
        return None if (math.isnan(val) or math.isinf(val)) else val
    return val


def sanitize(d: dict) -> dict:
    return {k: _clean(v) for k, v in d.items()}


def clean_record_for_db(rec: dict) -> dict:
    """Sanitize a record dict before writing to DB."""
    out = {}
    for k, v in rec.items():
        if isinstance(v, (bytes, bytearray)):
            try:
                out[k] = bool(int.from_bytes(v, "little"))
            except Exception:
                out[k] = None
        elif isinstance(v, np.bool_):
            out[k] = bool(v)
        elif isinstance(v, np.integer):
            out[k] = int(v)
        elif isinstance(v, np.floating):
            f = float(v)
            out[k] = None if (math.isnan(f) or math.isinf(f)) else f
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            out[k] = None
        else:
            out[k] = v
    return out


# ─── Domain table registry ────────────────────────────────────────────────────

DOMAIN_MODEL = {
    "oceanography": OceanographyRecord,
    "fisheries":    FisheriesRecord,
    "taxonomy":     TaxonomyRecord,
    "morphology":   MorphologyRecord,
    "edna":         EDNARecord,
}

DOMAIN_RELATIONSHIP = {
    "oceanography": "oceanography_records",
    "fisheries":    "fisheries_records",
    "taxonomy":     "taxonomy_records",
    "morphology":   "morphology_records",
    "edna":         "edna_records",
}


# ─── Preview endpoint (non-destructive) ──────────────────────────────────────

@app.post("/ingest/preview")
async def preview_ingest(
    file: UploadFile = File(...),
    domain: str = Query(default="auto"),
):
    """
    Dry-run: detect domain, show column mapping, first 5 parsed rows.
    Does NOT write to the database.
    """
    raw = await file.read()
    try:
        df, load_meta = load_csv_robust(raw)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")

    detected, confidence = detect_domain(df)
    forced = domain if domain != "auto" else detected

    # Show which columns were detected and what they map to
    from ingestion import normalise_columns, fuzzy_match_columns
    import re
    raw_cols = list(df.columns)
    norm_cols_map = {
        c: re.sub(r"[\s\-/\\()]+", "_", c.lower().strip())
        for c in raw_cols
    }
    fuzz_map = fuzzy_match_columns(list(norm_cols_map.values()))
    col_report = []
    for orig, norm in norm_cols_map.items():
        mapped = fuzz_map.get(norm, norm)
        col_report.append({
            "original": orig,
            "normalised": norm,
            "mapped_to": mapped,
            "changed": mapped != norm,
        })

    # Parse first 5 rows only
    try:
        _, records, stats, _ = ingest_raw(raw, forced)
        preview_rows = records[:5]
    except Exception as e:
        preview_rows = []
        stats = {"error": str(e)}

    return {
        "filename": file.filename,
        "encoding_detected": load_meta.get("encoding"),
        "header_row_found": load_meta.get("header_row"),
        "load_warnings": load_meta.get("warnings", []),
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "domain_detected": detected,
        "domain_confidence": round(confidence, 3),
        "domain_to_use": forced,
        "column_mapping": col_report,
        "parsed_preview": preview_rows,
        "stats_preview": stats,
    }


# ─── Ingest ───────────────────────────────────────────────────────────────────

@app.post("/ingest")
async def ingest_marine_data(
    file: UploadFile = File(...),
    domain: str = Query(default="auto"),
    uploader: str = Query(default="anonymous"),
    notes: str = Query(default=""),
    db: Session = Depends(get_db),
):
    raw = await file.read()

    try:
        detected_domain, records, stats, load_meta = ingest_raw(
            raw, domain if domain != "auto" else None
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ingestion failed: {e}")

    # Warn if confidence is low
    confidence = stats.get("confidence", 0)
    warnings = load_meta.get("warnings", [])
    if confidence < 0.2:
        warnings.append(
            f"Low domain confidence ({confidence:.0%}). "
            f"Consider manually selecting the domain."
        )

    master = MarineRecord(
        filename=file.filename,
        domain=detected_domain,
        species_count=stats.get("species_count", 0),
        avg_temp=stats.get("avg_temp"),
        record_count=len(records),
        uploader=uploader,
        notes=notes,
    )
    db.add(master)
    db.flush()

    Model = DOMAIN_MODEL[detected_domain]
    inserted = 0
    skipped = 0
    for rec in records:
        try:
            clean = clean_record_for_db(rec)
            db.add(Model(source_file_id=master.id, **clean))
            inserted += 1
        except Exception:
            skipped += 1
            continue

    db.commit()
    db.refresh(master)

    return {
        "status": "success",
        "record_id": master.id,
        "domain_detected": detected_domain,
        "domain_confidence": confidence,
        "encoding_detected": load_meta.get("encoding", "utf-8"),
        "rows_ingested": inserted,
        "rows_skipped": skipped,
        "warnings": warnings,
        "stats": stats,
    }


# ─── Master records ───────────────────────────────────────────────────────────

@app.get("/records")
def get_all_records(db: Session = Depends(get_db)):
    rows = db.query(MarineRecord).order_by(MarineRecord.upload_date.desc()).all()
    return [
        sanitize({
            "id": r.id, "filename": r.filename, "domain": r.domain,
            "upload_date": r.upload_date.isoformat() if r.upload_date else None,
            "species_count": r.species_count, "avg_temp": r.avg_temp,
            "record_count": r.record_count, "uploader": r.uploader,
        })
        for r in rows
    ]


@app.get("/records/{record_id}")
def get_record_detail(record_id: int, db: Session = Depends(get_db)):
    r = db.query(MarineRecord).filter(MarineRecord.id == record_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="Record not found")
    return sanitize({
        "id": r.id, "filename": r.filename, "domain": r.domain,
        "upload_date": r.upload_date.isoformat() if r.upload_date else None,
        "record_count": r.record_count, "notes": r.notes,
    })


# ─── Oceanography ─────────────────────────────────────────────────────────────

@app.get("/oceanography")
def get_oceanography(
    station_id: str = None,
    min_depth: float = None,
    max_depth: float = None,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    q = db.query(OceanographyRecord)
    if station_id:
        q = q.filter(OceanographyRecord.station_id == station_id)
    if min_depth is not None:
        q = q.filter(OceanographyRecord.depth_m >= min_depth)
    if max_depth is not None:
        q = q.filter(OceanographyRecord.depth_m <= max_depth)
    return [
        sanitize({
            "id": r.id, "station_id": r.station_id,
            "lat": r.decimal_latitude, "lon": r.decimal_longitude,
            "depth_m": r.depth_m, "event_date": r.event_date,
            "temperature_c": r.temperature_c, "salinity_psu": r.salinity_psu,
            "dissolved_oxygen": r.dissolved_oxygen, "ph": r.ph,
            "chlorophyll_a": r.chlorophyll_a, "turbidity": r.turbidity,
            "current_speed_ms": r.current_speed_ms,
        })
        for r in q.limit(limit).all()
    ]


# ─── Fisheries ────────────────────────────────────────────────────────────────

@app.get("/fisheries")
def get_fisheries(
    species: str = None,
    gear_type: str = None,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    q = db.query(FisheriesRecord)
    if species:
        q = q.filter(FisheriesRecord.scientific_name.ilike(f"%{species}%"))
    if gear_type:
        q = q.filter(FisheriesRecord.gear_type.ilike(f"%{gear_type}%"))
    return [
        sanitize({
            "id": r.id, "scientific_name": r.scientific_name,
            "vernacular_name": r.vernacular_name,
            "catch_weight_kg": r.catch_weight_kg,
            "catch_count": r.catch_count,
            "gear_type": r.gear_type, "vessel_id": r.vessel_id,
            "fishing_area": r.fishing_area, "event_date": r.event_date,
            "length_cm": r.length_cm, "weight_g": r.weight_g,
            "lat": r.decimal_latitude, "lon": r.decimal_longitude,
        })
        for r in q.limit(limit).all()
    ]


# ─── Taxonomy ─────────────────────────────────────────────────────────────────

@app.get("/taxonomy")
def get_taxonomy(
    scientific_name: str = None,
    family: str = None,
    iucn_status: str = None,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    q = db.query(TaxonomyRecord)
    if scientific_name:
        q = q.filter(TaxonomyRecord.scientific_name.ilike(f"%{scientific_name}%"))
    if family:
        q = q.filter(TaxonomyRecord.family.ilike(f"%{family}%"))
    if iucn_status:
        q = q.filter(TaxonomyRecord.iucn_status == iucn_status)
    return [
        sanitize({
            "id": r.id, "kingdom": r.kingdom, "phylum": r.phylum,
            "class_name": getattr(r, "class_name", None),
            "order_name": getattr(r, "order_name", None),
            "family": r.family, "genus": r.genus, "species": r.species,
            "scientific_name": r.scientific_name,
            "vernacular_name": r.vernacular_name,
            "iucn_status": r.iucn_status, "habitat": r.habitat,
            "gbif_id": r.gbif_id, "taxon_id": r.taxon_id,
        })
        for r in q.limit(limit).all()
    ]


# ─── Morphology ───────────────────────────────────────────────────────────────

@app.get("/morphology")
def get_morphology(
    species: str = None,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    q = db.query(MorphologyRecord)
    if species:
        q = q.filter(MorphologyRecord.scientific_name.ilike(f"%{species}%"))
    return [
        sanitize({
            "id": r.id, "sample_id": r.sample_id,
            "scientific_name": r.scientific_name,
            "length_mm": r.length_mm, "width_mm": r.width_mm,
            "area_mm2": r.area_mm2, "perimeter_mm": r.perimeter_mm,
            "aspect_ratio": r.aspect_ratio, "circularity": r.circularity,
            "form_factor": r.form_factor, "roundness": r.roundness,
            "fish_length_cm": r.fish_length_cm,
            "estimated_age_yr": r.estimated_age_yr,
            "event_date": r.event_date,
        })
        for r in q.limit(limit).all()
    ]


# ─── eDNA ─────────────────────────────────────────────────────────────────────

@app.get("/edna")
def get_edna(
    species: str = None,
    target_gene: str = None,
    min_reads: int = None,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    q = db.query(EDNARecord)
    if species:
        q = q.filter(EDNARecord.scientific_name.ilike(f"%{species}%"))
    if target_gene:
        q = q.filter(EDNARecord.target_gene.ilike(f"%{target_gene}%"))
    if min_reads:
        q = q.filter(EDNARecord.read_count >= min_reads)
    return [
        sanitize({
            "id": r.id, "sample_id": r.sample_id,
            "scientific_name": r.scientific_name,
            "target_gene": r.target_gene,
            "sequencing_platform": r.sequencing_platform,
            "read_count": r.read_count,
            "relative_abundance": r.relative_abundance,
            "detection_status": r.detection_status,
            "lat": r.decimal_latitude, "lon": r.decimal_longitude,
            "depth_m": r.depth_m, "event_date": r.event_date,
        })
        for r in q.limit(limit).all()
    ]


# ─── Analytics ────────────────────────────────────────────────────────────────

def shannon_diversity(counts: list) -> float:
    total = sum(counts)
    if total == 0:
        return 0.0
    props = [c / total for c in counts if c > 0]
    return round(-sum(p * math.log(p) for p in props), 4)


def simpson_diversity(counts: list) -> float:
    total = sum(counts)
    if total <= 1:
        return 0.0
    return round(1 - sum(c * (c - 1) for c in counts) / (total * (total - 1)), 4)


@app.get("/analytics/biodiversity")
def biodiversity_indices(db: Session = Depends(get_db)):
    fish_rows = db.query(FisheriesRecord).all()
    fish_counts: dict = {}
    for r in fish_rows:
        name = r.scientific_name or "unknown"
        cnt = _clean(r.catch_count)
        fish_counts[name] = fish_counts.get(name, 0) + (int(cnt) if cnt else 1)

    edna_rows = db.query(EDNARecord).all()
    edna_counts: dict = {}
    for r in edna_rows:
        name = r.scientific_name or "unknown"
        cnt = _clean(r.read_count)
        edna_counts[name] = edna_counts.get(name, 0) + (int(cnt) if cnt else 1)

    fish_vals = list(fish_counts.values())
    edna_vals  = list(edna_counts.values())

    return {
        "fisheries": {
            "species_richness": len(fish_counts),
            "shannon_index": shannon_diversity(fish_vals),
            "simpson_index": simpson_diversity(fish_vals),
            "top_species": sorted(fish_counts.items(), key=lambda x: -x[1])[:10],
        },
        "edna": {
            "species_richness": len(edna_counts),
            "shannon_index": shannon_diversity(edna_vals),
            "simpson_index": simpson_diversity(edna_vals),
            "top_species": sorted(edna_counts.items(), key=lambda x: -x[1])[:10],
        },
    }


@app.get("/analytics/trends")
def temporal_trends(db: Session = Depends(get_db)):
    ocean_rows = db.query(OceanographyRecord).filter(
        OceanographyRecord.event_date.isnot(None),
        OceanographyRecord.temperature_c.isnot(None),
    ).all()
    temp_by_date: dict = {}
    for r in ocean_rows:
        d = r.event_date[:10] if r.event_date else None
        t = _clean(r.temperature_c)
        if d and t is not None:
            temp_by_date.setdefault(d, []).append(float(t))

    temp_trend = [
        {"date": d, "avg_temp": round(float(np.mean(v)), 3)}
        for d, v in sorted(temp_by_date.items())
    ]

    fish_rows = db.query(FisheriesRecord).filter(
        FisheriesRecord.event_date.isnot(None),
        FisheriesRecord.catch_weight_kg.isnot(None),
    ).all()
    catch_by_date: dict = {}
    for r in fish_rows:
        d = r.event_date[:10] if r.event_date else None
        w = _clean(r.catch_weight_kg)
        if d and w is not None:
            catch_by_date[d] = catch_by_date.get(d, 0) + float(w)

    catch_trend = [
        {"date": d, "total_catch_kg": round(v, 2)}
        for d, v in sorted(catch_by_date.items())
    ]

    return {"temperature_trend": temp_trend, "catch_trend": catch_trend}


@app.get("/analytics/cross-domain")
def cross_domain_species(db: Session = Depends(get_db)):
    fish_names  = {r.scientific_name for r in db.query(FisheriesRecord).all()  if r.scientific_name}
    tax_names   = {r.scientific_name for r in db.query(TaxonomyRecord).all()   if r.scientific_name}
    edna_names  = {r.scientific_name for r in db.query(EDNARecord).all()        if r.scientific_name}
    morph_names = {r.scientific_name for r in db.query(MorphologyRecord).all() if r.scientific_name}

    all_names = fish_names | tax_names | edna_names | morph_names
    result = []
    for name in sorted(all_names):
        domains = (
            (["fisheries"] if name in fish_names else []) +
            (["taxonomy"]  if name in tax_names  else []) +
            (["edna"]      if name in edna_names else []) +
            (["morphology"]if name in morph_names else [])
        )
        result.append({"scientific_name": name, "domains": domains, "domain_count": len(domains)})

    result.sort(key=lambda x: -x["domain_count"])
    return result[:50]


@app.get("/analytics/hotspots")
def geographic_hotspots(db: Session = Depends(get_db)):
    fish_rows = db.query(FisheriesRecord).filter(
        FisheriesRecord.decimal_latitude.isnot(None),
        FisheriesRecord.decimal_longitude.isnot(None),
    ).all()
    grid: dict = {}
    for r in fish_rows:
        lat = _clean(r.decimal_latitude)
        lon = _clean(r.decimal_longitude)
        w   = _clean(r.catch_weight_kg) or 1
        if lat is not None and lon is not None:
            cell = (round(float(lat)), round(float(lon)))
            grid[cell] = grid.get(cell, 0) + float(w)

    return [
        {"lat": lat, "lon": lon, "total_catch_kg": round(val, 2)}
        for (lat, lon), val in sorted(grid.items(), key=lambda x: -x[1])[:20]
    ]


# ─── Species search ───────────────────────────────────────────────────────────

@app.get("/species/search")
def species_search(
    q: str = Query(...),
    use_gbif: bool = Query(default=True),
    db: Session = Depends(get_db),
):
    local = db.query(TaxonomyRecord).filter(
        TaxonomyRecord.scientific_name.ilike(f"%{q}%")
    ).limit(20).all()

    local_results = [
        sanitize({
            "source": "local",
            "scientific_name": r.scientific_name,
            "vernacular_name": r.vernacular_name,
            "family": r.family, "kingdom": r.kingdom,
            "iucn_status": r.iucn_status,
        })
        for r in local
    ]

    gbif_results = []
    if use_gbif:
        try:
            resp = http_req.get(
                "https://api.gbif.org/v1/species/suggest",
                params={"q": q, "limit": 10},
                timeout=5,
            )
            if resp.status_code == 200:
                for item in resp.json():
                    gbif_results.append({
                        "source": "gbif",
                        "scientific_name": item.get("scientificName"),
                        "vernacular_name": item.get("vernacularName"),
                        "family": item.get("family"),
                        "kingdom": item.get("kingdom"),
                        "gbif_key": item.get("key"),
                    })
        except Exception:
            pass

    return {"local": local_results, "gbif": gbif_results}


# ─── Export ───────────────────────────────────────────────────────────────────

@app.get("/export/csv")
def export_csv(
    domain: str = Query(...),
    db: Session = Depends(get_db),
):
    Model = DOMAIN_MODEL.get(domain)
    if not Model:
        raise HTTPException(status_code=400, detail=f"Unknown domain: {domain}")
    rows = db.query(Model).all()
    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    data = [
        {c.name: _clean(getattr(r, c.name)) for c in Model.__table__.columns}
        for r in rows
    ]
    df = pd.DataFrame(data)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=cmlre_{domain}_export.csv"},
    )