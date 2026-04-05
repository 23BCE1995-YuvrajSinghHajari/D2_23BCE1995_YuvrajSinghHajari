# CMLRE Unified Marine Data Platform

## Overview
A scalable, intelligent platform integrating oceanography, fisheries, taxonomy, otolith morphology, and eDNA datasets into a single interoperable system.

## Project structure
```
cmlre_platform/
├── database.py          ← SQLAlchemy models (all 5 domain tables + master log)
├── ingestion.py         ← Modular ingestion pipelines + Darwin Core standardisation
├── main.py              ← FastAPI backend (all endpoints + analytics)
├── app.py               ← Streamlit dashboard (all 8 pages + visualisations)
├── requirements.txt
└── sample_data/
    ├── oceanography_sample.csv
    ├── fisheries_sample.csv
    ├── taxonomy_sample.csv
    ├── morphology_sample.csv
    └── edna_sample.csv
```

## Setup

```bash
pip install -r requirements.txt
```

## Running

**Terminal 1 — FastAPI backend:**
```bash
uvicorn main:app --reload --port 8000
```

**Terminal 2 — Streamlit frontend:**
```bash
streamlit run app.py
```

Then open http://localhost:8501

## API docs
FastAPI auto-generates interactive docs at: http://localhost:8000/docs

## Ingestion flow
1. Upload any CSV via the Streamlit UI or POST to `/ingest`
2. Domain is **auto-detected** from column names (or you can force it)
3. Columns are mapped to Darwin Core standard names automatically
4. Data is written to domain-specific tables linked to the master log
5. Summary stats are returned immediately

## Analytics endpoints
| Endpoint | Description |
|---|---|
| `GET /analytics/biodiversity` | Shannon + Simpson indices from fisheries & eDNA |
| `GET /analytics/trends` | Temporal temperature + catch trends |
| `GET /analytics/cross-domain` | Species present across multiple domains |
| `GET /analytics/hotspots` | Geographic catch hotspots (1° grid) |
| `GET /species/search?q=...` | Fuzzy search + GBIF API lookup |
| `GET /export/csv?domain=...` | Download any domain as CSV |

## Extending
- **Add a new domain:** Add a model in `database.py`, a parser in `ingestion.py`, and endpoint in `main.py`
- **Switch to PostgreSQL:** Change `SQLALCHEMY_DATABASE_URL` in `database.py`
- **Add image analysis:** The morphology module is ready to accept `image_path` for future OpenCV integration