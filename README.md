# CMLRE Unified Marine Data Platform

This started as a college project and kind of grew into something bigger than I expected. The basic problem I was trying to solve: CMLRE (Centre for Marine Living Resources and Ecology) handles data from five completely different scientific domains — oceanography, fisheries, taxonomy, otolith morphology, and eDNA — and none of it talks to each other. If you want to know whether a species showing up in your eDNA samples from the Arabian Sea also appears in fisheries catch records for the same region, you're manually digging through at least three databases and hoping the species names are spelled the same way. That's the problem this platform fixes.

---

## What it actually does

- Upload any CSV from any of the five domains and the system figures out what it is automatically — no manual tagging, no data cleaning
- Maps messy government column names to a clean standard schema (Darwin Core) using fuzzy matching against 200+ synonyms. So `TEMP`, `SST`, `T_DEG_C` and `temperature` all get treated as the same thing
- Handles whatever encoding your file comes in — UTF-8, Windows-1252, ISO-8859-1 — without breaking
- Detects and skips metadata rows that government CSVs often have before the actual header row
- Computes Shannon-Wiener (H') and Simpson's (1-D) biodiversity indices from both fisheries catch data and eDNA read counts
- Cross-domain species query — find species that appear in fisheries records, taxonomy, eDNA, and morphology simultaneously
- Geographic hotspot mapping on a one-degree grid
- Interactive dashboards for every domain: temperature/salinity depth profiles, catch breakdown by species and gear, age-length curves, eDNA read count distributions, IUCN status breakdown, and more
- Export any domain's data as a clean CSV

---

## Project structure

```
cmlre_platform/
├── app.py              — Streamlit frontend, all 8 pages
├── main.py             — FastAPI backend, 14 endpoints
├── database.py         — SQLAlchemy models for all 6 tables
├── ingestion.py        — Ingestion pipeline (encoding, header detection, fuzzy matching, parsers)
├── requirements.txt
├── marine_data.db      — SQLite database (created on first run)
└── sample_data/
    ├── oceanography_sample.csv
    ├── fisheries_sample.csv
    ├── taxonomy_sample.csv
    ├── morphology_sample.csv
    └── edna_sample.csv
```

---

## Setup

**Requirements:** Python 3.10 or above (tested on 3.13.4)

```bash
# Clone the repo
git clone <your-repo-url>
cd cmlre_platform

# Create a virtual environment
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## Running the platform

You need two terminals running at the same time.

**Terminal 1 — FastAPI backend:**
```bash
uvicorn main:app --reload --port 8000
```

Wait until you see `Application startup complete` before opening the frontend.

**Terminal 2 — Streamlit frontend:**
```bash
streamlit run app.py
```

Then open **http://localhost:8501** in your browser.

The API docs are auto-generated at **http://localhost:8000/docs** — useful if you want to test individual endpoints.

---

## First time? Upload the sample data

Go to the **Upload & Ingest** page and upload the five sample CSVs from `sample_data/` one by one. Leave the domain dropdown on **auto** — it'll detect each file correctly. Once all five are in, every tab in the dashboard will have data to show.

Upload order doesn't matter, but for cross-domain analytics to show anything interesting you need at least the fisheries and eDNA files in.

---

## Using real data

The platform was specifically built to handle the mess that real government datasets come in. These are the sources I tested with:

| Domain | Source | URL |
|--------|--------|-----|
| Oceanography | INCOIS Argo data via data.gov.in | https://www.data.gov.in/catalog/indian-ocean-argo-data |
| Fisheries | CMFRI / data.gov.in catch statistics | https://www.data.gov.in/catalog/fish-catch-and-landings-group-species |
| Taxonomy | IndOBIS / CMLRE OBIS dataset | https://obis.org/dataset/31d93350-097e-456c-8e12-8af658c1107b |
| eDNA | OBIS eDNA node | https://obis.org/data/access/ |
| Morphology | Field data / CMFRI publications | — |

When uploading real files, use the **Preview** button first (calls `/ingest/preview` under the hood) — it shows you what domain was detected, what columns got remapped, and the first five parsed rows, all without writing anything to the database. Good way to catch issues before committing.

If the auto-detection picks the wrong domain (it'll show you a confidence score — anything below 20% means it's not sure), just select the correct domain from the dropdown manually.

---

## The five data tabs

**🌊 Oceanography**
Temperature, salinity, dissolved oxygen, pH, chlorophyll-a, turbidity. Depth profile scatter plots, time-series for each parameter, and a station map if your data has lat/lon.

**🐟 Fisheries**
Catch weight by species, gear type breakdown, size distributions, fishing area map. Works best with record-level data that has coordinates — the aggregated annual stats from data.gov.in will populate the species charts but not the map.

**🔬 Species ID & Taxonomy**
Full Darwin Core classification (kingdom → species), IUCN status breakdown, top families. The search box queries both your local database and the GBIF API simultaneously. Toggle the GBIF lookup off if you're working offline.

**🦷 Otolith Morphology**
Shape metrics (circularity, aspect ratio, form factor, roundness), age-length curves with OLS trendline, inter-species morphometric comparison. This tab is most useful when you have samples from multiple species — the box plots show you how cleanly the shape metrics separate them.

**🧬 eDNA & Molecular**
Read counts per species, relative abundance pie chart, detection location map. The detection rate KPI at the top tells you what fraction of records came back positive, which is a quick proxy for how species-rich the area is.

---

## API endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/ingest` | Upload and ingest a CSV |
| POST | `/ingest/preview` | Dry run — shows detection result without writing to DB |
| GET | `/records` | All ingestion log entries |
| GET | `/records/{id}` | Single file detail |
| GET | `/oceanography` | Filtered oceanography records |
| GET | `/fisheries` | Filtered fisheries records |
| GET | `/taxonomy` | Filtered taxonomy records |
| GET | `/morphology` | Filtered morphology records |
| GET | `/edna` | Filtered eDNA records |
| GET | `/analytics/biodiversity` | Shannon and Simpson indices |
| GET | `/analytics/trends` | Temperature and catch over time |
| GET | `/analytics/cross-domain` | Species present in multiple domains |
| GET | `/analytics/hotspots` | Geographic catch hotspots |
| GET | `/species/search` | Local + GBIF species search |
| GET | `/export/csv` | Download domain data as CSV |

---

## Biodiversity indices

The platform computes two standard indices:

**Shannon-Wiener (H')** — measures species diversity accounting for both richness and evenness. Higher values mean more diverse communities. Computed separately from fisheries catch counts and eDNA read counts.

**Simpson's (1-D)** — probability that two randomly picked individuals belong to different species. Ranges from 0 to 1, higher is more diverse.

The interesting thing that came out of testing: fisheries data from a region typically shows 8-12 species while eDNA from the same area detects 20-25. That gap isn't noise — it's the difference between what gets caught and what's actually present. Rare species, juveniles, and species that avoid certain gear types all show up in eDNA but not in catch records. Having both numbers in the same system is the whole point.

---

## Database schema

Six tables total. One master log (`marine_records`) that tracks every uploaded file, and one domain table per data type. All domain tables have `source_file_id` linking back to the master log so you can always trace a record to its original file.

Darwin Core fields (`decimal_latitude`, `decimal_longitude`, `event_date`, `scientific_name`) appear in all five domain tables with the same names — this is what makes cross-domain joins actually work without complicated transformations.

The database is SQLite by default (single file, zero setup). To switch to PostgreSQL, change the connection string in `database.py`:

```python
# SQLite (default)
SQLALCHEMY_DATABASE_URL = "sqlite:///./marine_data.db"

# PostgreSQL
SQLALCHEMY_DATABASE_URL = "postgresql://user:password@localhost/cmlre_db"
```

Everything else stays the same — SQLAlchemy handles the difference.

---

## Common issues

**"No module named plotly" or similar on startup**
Your venv lost its packages. Run:
```bash
pip install plotly streamlit fastapi uvicorn sqlalchemy pandas numpy python-multipart requests statsmodels
```

**"no such column: marine_records.domain"**
You have an old database from before the schema was updated. Delete it and let it recreate:
```bash
del marine_data.db   # Windows
rm marine_data.db    # Mac/Linux
```
Then restart uvicorn and re-upload your data.

**Taxonomy CSV fails to ingest with ValueError**
Known issue with pandas 3.x and the `class` column name (which conflicts with Python's reserved word). The `ingestion.py` in this repo already has the fix — make sure you're using the latest version of the file.

**Low domain confidence warning on upload**
The auto-detector isn't confident about which domain the file belongs to. Either select the domain manually from the dropdown, or check if the column names are too generic. The preview endpoint will show you exactly what columns were detected.

**API error messages in the dashboard tabs**
Make sure the FastAPI server is actually running (Terminal 1). If it crashed for some reason, the Streamlit frontend just shows the API error rather than crashing itself.

---

## Tech stack

- **FastAPI** — backend API, async, auto-generates docs at /docs
- **SQLAlchemy** — ORM, database-agnostic so SQLite in dev and PostgreSQL in production
- **SQLite** — default database, single file
- **Streamlit** — interactive dashboard, Python-native
- **Plotly** — charts and maps including Mapbox scatter maps
- **Pandas 3.x** — data processing in the ingestion pipeline
- **Python difflib** — fuzzy column name matching (no extra dependencies needed)
- **GBIF API** — external taxonomy lookup at api.gbif.org

---

## Extending the platform

**Adding a new data domain:**
1. Add a SQLAlchemy model in `database.py`
2. Add a parser function in `ingestion.py` and update `PARSERS` dict and `DOMAIN_SIGNATURES`
3. Add an endpoint in `main.py`
4. Add a page in `app.py`

The rest of the pipeline (encoding detection, header finding, fuzzy matching, sanitisation) picks it up automatically.

**Switching to PostgreSQL for production:**
Change the connection string in `database.py` and run `pip install psycopg2-binary`. No other changes needed.

---

## Data standards

The platform follows [Darwin Core](https://dwc.tdwg.org/) vocabulary for all fields related to taxonomy, location, and time. Taxonomy is cross-referenced against [WoRMS](https://www.marinespecies.org/) AphiaIDs where available. External species lookup uses the [GBIF API](https://www.gbif.org/developer/species).

---

Built for CMLRE — Centre for Marine Living Resources and Ecology, Kochi, under the Ministry of Earth Sciences, Government of India.