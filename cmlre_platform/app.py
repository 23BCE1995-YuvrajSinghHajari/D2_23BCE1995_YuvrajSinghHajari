"""
CMLRE Unified Marine Data Platform — Streamlit UI
Pages:
  🏠 Dashboard           — platform overview, recent ingestions, KPIs
  📤 Upload & Ingest     — multi-domain CSV upload with domain detection
  🌊 Oceanography        — temperature / salinity / DO trends
  🐟 Fisheries           — catch analysis, gear types, species breakdown
  🔬 Species ID          — taxonomy search, GBIF lookup, classification tree
  🦷 Otolith Morphology  — shape metrics, scatter plots, age-length curves
  🧬 eDNA & Molecular    — read counts, diversity, detection map
  📊 Biodiversity        — Shannon/Simpson indices, cross-domain species
  📥 Export              — download data as CSV
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json

API = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="CMLRE Marine Data Platform",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.image(
    "https://upload.wikimedia.org/wikipedia/commons/thumb/b/b1/Ocean_surface.jpg/320px-Ocean_surface.jpg",
    width=300,
)
st.sidebar.title("🌊 CMLRE Platform")
st.sidebar.caption("Unified Marine Data Intelligence")

page = st.sidebar.radio(
    "Navigate",
    [
        "🏠 Dashboard",
        "📤 Upload & Ingest",
        "🌊 Oceanography",
        "🐟 Fisheries",
        "🔬 Species ID & Taxonomy",
        "🦷 Otolith Morphology",
        "🧬 eDNA & Molecular",
        "📊 Biodiversity Analytics",
        "📥 Export Data",
    ],
)

st.sidebar.divider()
st.sidebar.caption("Backend: FastAPI · DB: SQLite · Standards: Darwin Core")


def api_get(endpoint, params=None):
    try:
        r = requests.get(f"{API}{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error at {endpoint}: {e}")
        return None


def domain_color(domain):
    return {
        "oceanography": "#1E88E5",
        "fisheries":    "#43A047",
        "taxonomy":     "#8E24AA",
        "morphology":   "#FB8C00",
        "edna":         "#00ACC1",
    }.get(domain, "#757575")


# ─── 🏠 Dashboard ─────────────────────────────────────────────────────────────

if page == "🏠 Dashboard":
    st.title("🌊 CMLRE Unified Marine Data Platform")
    st.markdown("**AI-driven insights for oceanographic, fisheries, taxonomy, morphology, and eDNA data**")
    st.divider()

    records = api_get("/records")
    if records:
        df_rec = pd.DataFrame(records)

        # KPI row
        col1, col2, col3, col4, col5 = st.columns(5)
        total_files    = len(df_rec)
        total_rows     = df_rec["record_count"].sum() if "record_count" in df_rec else 0
        total_species  = df_rec["species_count"].sum() if "species_count" in df_rec else 0
        domains_active = df_rec["domain"].nunique() if "domain" in df_rec else 0
        avg_temp       = df_rec["avg_temp"].dropna().mean() if "avg_temp" in df_rec else None

        col1.metric("Files ingested", total_files)
        col2.metric("Total records", f"{int(total_rows):,}")
        col3.metric("Species tracked", f"{int(total_species):,}")
        col4.metric("Domains active", domains_active)
        col5.metric("Avg temperature", f"{avg_temp:.1f} °C" if avg_temp else "—")

        st.divider()

        left, right = st.columns([3, 2])

        with left:
            st.subheader("Recent ingestions")
            display_cols = ["id", "filename", "domain", "record_count", "species_count", "upload_date"]
            show = [c for c in display_cols if c in df_rec.columns]
            st.dataframe(df_rec[show].head(20), use_container_width=True)

        with right:
            st.subheader("Records by domain")
            if "domain" in df_rec.columns:
                domain_counts = df_rec.groupby("domain")["record_count"].sum().reset_index()
                domain_counts.columns = ["Domain", "Records"]
                fig = px.pie(
                    domain_counts, names="Domain", values="Records",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                    hole=0.4,
                )
                fig.update_layout(margin=dict(t=10, b=10, l=0, r=0), height=280)
                st.plotly_chart(fig, use_container_width=True)

        # Temperature trend from oceanography
        st.subheader("Temperature trend (all stations)")
        trends = api_get("/analytics/trends")
        if trends and trends.get("temperature_trend"):
            df_t = pd.DataFrame(trends["temperature_trend"])
            if not df_t.empty:
                fig2 = px.line(df_t, x="date", y="avg_temp",
                               labels={"avg_temp": "Avg temperature (°C)", "date": "Date"},
                               color_discrete_sequence=["#1E88E5"])
                fig2.update_layout(height=260, margin=dict(t=10, b=10))
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("No temperature trend data yet. Upload oceanography CSVs.")
        else:
            st.info("Upload oceanography data to see trends.")
    else:
        st.warning("Could not reach the FastAPI backend. Start it with: `uvicorn main:app --reload`")
        st.code("cd cmlre_platform && uvicorn main:app --reload", language="bash")


# ─── 📤 Upload & Ingest ───────────────────────────────────────────────────────

elif page == "📤 Upload & Ingest":
    st.title("📤 Upload & Ingest Data")
    st.markdown("Upload CSVs from any domain. The platform auto-detects the data type and standardises to Darwin Core.")

    col_form, col_help = st.columns([2, 1])

    with col_form:
        with st.form("upload_form"):
            uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
            domain_choice = st.selectbox(
                "Domain (auto = detect from columns)",
                ["auto", "oceanography", "fisheries", "taxonomy", "morphology", "edna"],
            )
            uploader_name = st.text_input("Your name / team", value="anonymous")
            notes_text    = st.text_area("Notes (optional)")
            submitted = st.form_submit_button("🚀 Ingest to Platform", type="primary")

        if submitted and uploaded_file is not None:
            df_preview = pd.read_csv(uploaded_file)
            uploaded_file.seek(0)

            with st.spinner("Ingesting and standardising data…"):
                response = requests.post(
                    f"{API}/ingest",
                    files={"file": (uploaded_file.name, uploaded_file.getvalue(), "text/csv")},
                    params={
                        "domain": domain_choice,
                        "uploader": uploader_name,
                        "notes": notes_text,
                    },
                )

            if response.status_code == 200:
                res = response.json()
                st.success(f"✅ Ingested {res['rows_ingested']:,} rows as **{res['domain_detected']}** (record ID: {res['record_id']})")

                stats = res.get("stats", {})
                if stats:
                    st.json(stats)

                # Auto-chart based on domain
                domain = res["domain_detected"]
                if domain == "oceanography" and "temperature_c" in df_preview.columns:
                    fig = px.line(df_preview, y="temperature_c", title="Temperature (uploaded data)")
                    st.plotly_chart(fig, use_container_width=True)
                elif domain == "fisheries" and "catch_weight_kg" in df_preview.columns:
                    fig = px.bar(df_preview, y="catch_weight_kg", title="Catch weight (uploaded data)")
                    st.plotly_chart(fig, use_container_width=True)
                elif domain == "edna" and "read_count" in df_preview.columns:
                    fig = px.bar(df_preview, x="scientific_name", y="read_count",
                                 title="Read counts by species")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.error(f"Ingestion failed: {response.text}")

        elif submitted:
            st.warning("Please select a file first.")

    with col_help:
        st.subheader("Expected columns by domain")
        with st.expander("🌊 Oceanography"):
            st.markdown("""
- `temperature` / `temperature_c`
- `salinity` / `salinity_psu`
- `dissolved_oxygen`
- `ph`, `chlorophyll_a`, `turbidity`
- `decimal_latitude`, `decimal_longitude`
- `depth_m`, `event_date`, `station_id`
            """)
        with st.expander("🐟 Fisheries"):
            st.markdown("""
- `scientific_name`, `vernacular_name`
- `catch_weight_kg`, `catch_count`
- `gear_type`, `vessel_id`, `fishing_area`
- `length_cm`, `weight_g`, `maturity_stage`
- `decimal_latitude`, `decimal_longitude`
- `event_date`
            """)
        with st.expander("🔬 Taxonomy"):
            st.markdown("""
- `kingdom`, `phylum`, `class`, `order`
- `family`, `genus`, `species`
- `scientific_name`, `vernacular_name`
- `iucn_status`, `habitat`, `distribution`
- `gbif_id`, `taxon_id`, `authority`
            """)
        with st.expander("🦷 Morphology"):
            st.markdown("""
- `sample_id`, `scientific_name`
- `length_mm`, `width_mm`
- `area_mm2`, `perimeter_mm`
- `aspect_ratio`, `circularity`
- `fish_length_cm`, `fish_weight_g`
- `estimated_age_yr`, `event_date`
            """)
        with st.expander("🧬 eDNA"):
            st.markdown("""
- `sample_id`, `scientific_name`
- `target_gene`, `sequencing_platform`
- `primer_pair`, `read_count`
- `relative_abundance`, `sequence_quality`
- `detection_status`
- `decimal_latitude`, `decimal_longitude`
- `depth_m`, `event_date`
            """)

    # Preview uploaded data
    if uploaded_file is not None and not submitted:
        df_prev = pd.read_csv(uploaded_file)
        st.subheader("Data preview")
        st.dataframe(df_prev.head(20), use_container_width=True)
        st.caption(f"{len(df_prev):,} rows · {len(df_prev.columns)} columns · Detected columns: {', '.join(df_prev.columns.tolist())}")


# ─── 🌊 Oceanography ──────────────────────────────────────────────────────────

elif page == "🌊 Oceanography":
    st.title("🌊 Oceanography")

    with st.expander("Filters", expanded=False):
        col1, col2, col3 = st.columns(3)
        station_filter = col1.text_input("Station ID (optional)")
        min_depth = col2.number_input("Min depth (m)", value=0.0)
        max_depth = col3.number_input("Max depth (m)", value=5000.0)

    data = api_get("/oceanography", params={
        "station_id": station_filter or None,
        "min_depth": min_depth,
        "max_depth": max_depth,
        "limit": 1000,
    })

    if data:
        df = pd.DataFrame(data)
        if df.empty:
            st.info("No oceanography data found. Upload a CSV first.")
        else:
            # KPIs
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Measurements", len(df))
            c2.metric("Avg temperature", f"{df['temperature_c'].dropna().mean():.2f} °C" if 'temperature_c' in df else "—")
            c3.metric("Avg salinity", f"{df['salinity_psu'].dropna().mean():.2f} PSU" if 'salinity_psu' in df else "—")
            c4.metric("Avg DO", f"{df['dissolved_oxygen'].dropna().mean():.2f}" if 'dissolved_oxygen' in df else "—")

            tab1, tab2, tab3, tab4 = st.tabs(["Temperature & salinity", "Profile by depth", "Map", "Raw data"])

            with tab1:
                numeric_cols = [c for c in ["temperature_c", "salinity_psu", "dissolved_oxygen", "ph", "chlorophyll_a", "turbidity"] if c in df.columns]
                if len(numeric_cols) >= 2:
                    fig = make_subplots(rows=len(numeric_cols), cols=1,
                                        subplot_titles=numeric_cols, shared_xaxes=True)
                    for i, col in enumerate(numeric_cols, 1):
                        fig.add_trace(go.Scatter(x=df.get("event_date"), y=df[col],
                                                  mode="lines+markers", name=col), row=i, col=1)
                    fig.update_layout(height=120 * len(numeric_cols), showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Upload data with temperature, salinity etc. columns.")

            with tab2:
                if "depth_m" in df.columns and "temperature_c" in df.columns:
                    fig = px.scatter(df, x="temperature_c", y="depth_m",
                                     color="salinity_psu" if "salinity_psu" in df.columns else None,
                                     title="Temperature profile by depth",
                                     labels={"temperature_c": "Temperature (°C)", "depth_m": "Depth (m)"},
                                     color_continuous_scale="RdBu_r")
                    fig.update_yaxes(autorange="reversed")
                    st.plotly_chart(fig, use_container_width=True)

            with tab3:
                if "lat" in df.columns and "lon" in df.columns:
                    map_df = df.dropna(subset=["lat", "lon"])
                    if not map_df.empty:
                        fig = px.scatter_mapbox(
                            map_df, lat="lat", lon="lon",
                            color="temperature_c" if "temperature_c" in map_df.columns else None,
                            size_max=12,
                            hover_data=["station_id", "depth_m", "temperature_c", "salinity_psu"],
                            color_continuous_scale="RdBu_r",
                            mapbox_style="open-street-map", zoom=3, height=500,
                            title="Station locations",
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No lat/lon data available for mapping.")

            with tab4:
                st.dataframe(df, use_container_width=True)
    else:
        st.info("No data or backend not running.")


# ─── 🐟 Fisheries ─────────────────────────────────────────────────────────────

elif page == "🐟 Fisheries":
    st.title("🐟 Fisheries Analysis")

    with st.expander("Filters", expanded=False):
        col1, col2 = st.columns(2)
        species_filter = col1.text_input("Species name (partial match)")
        gear_filter    = col2.text_input("Gear type (partial match)")

    data = api_get("/fisheries", params={
        "species": species_filter or None,
        "gear_type": gear_filter or None,
        "limit": 1000,
    })

    if data:
        df = pd.DataFrame(data)
        if df.empty:
            st.info("No fisheries data. Upload a fisheries CSV.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Records", len(df))
            c2.metric("Species", df["scientific_name"].nunique() if "scientific_name" in df.columns else 0)
            total_catch = df["catch_weight_kg"].sum() if "catch_weight_kg" in df.columns else 0
            c3.metric("Total catch (kg)", f"{total_catch:,.1f}")
            c4.metric("Gear types", df["gear_type"].nunique() if "gear_type" in df.columns else 0)

            tab1, tab2, tab3, tab4 = st.tabs(["Catch by species", "Gear analysis", "Size distribution", "Map"])

            with tab1:
                if "scientific_name" in df.columns and "catch_weight_kg" in df.columns:
                    top = (df.groupby("scientific_name")["catch_weight_kg"]
                             .sum().reset_index()
                             .sort_values("catch_weight_kg", ascending=False).head(15))
                    fig = px.bar(top, x="catch_weight_kg", y="scientific_name", orientation="h",
                                 color="catch_weight_kg", color_continuous_scale="Greens",
                                 labels={"catch_weight_kg": "Total catch (kg)", "scientific_name": "Species"},
                                 title="Top species by catch weight")
                    fig.update_layout(yaxis=dict(autorange="reversed"), height=420)
                    st.plotly_chart(fig, use_container_width=True)

            with tab2:
                if "gear_type" in df.columns:
                    gear_df = df.groupby("gear_type").agg(
                        catch_kg=("catch_weight_kg", "sum"),
                        records=("id", "count"),
                    ).reset_index()
                    fig = px.bar(gear_df, x="gear_type", y="catch_kg",
                                 color="records", color_continuous_scale="Viridis",
                                 title="Catch by gear type",
                                 labels={"catch_kg": "Total catch (kg)", "gear_type": "Gear type"})
                    st.plotly_chart(fig, use_container_width=True)

            with tab3:
                size_cols = [c for c in ["length_cm", "weight_g"] if c in df.columns]
                if size_cols:
                    cols = st.columns(len(size_cols))
                    for i, col in enumerate(size_cols):
                        fig = px.histogram(df, x=col, nbins=30, title=f"{col} distribution",
                                           color_discrete_sequence=["#43A047"])
                        cols[i].plotly_chart(fig, use_container_width=True)

            with tab4:
                if "lat" in df.columns and "lon" in df.columns:
                    map_df = df.dropna(subset=["lat", "lon"])
                    if not map_df.empty:
                        fig = px.scatter_mapbox(
                            map_df, lat="lat", lon="lon",
                            color="scientific_name",
                            size="catch_weight_kg" if "catch_weight_kg" in map_df.columns else None,
                            hover_data=["scientific_name", "gear_type", "catch_weight_kg"],
                            mapbox_style="open-street-map", zoom=3, height=500,
                        )
                        st.plotly_chart(fig, use_container_width=True)


# ─── 🔬 Species ID & Taxonomy ─────────────────────────────────────────────────

elif page == "🔬 Species ID & Taxonomy":
    st.title("🔬 Species ID & Taxonomy")
    st.markdown("Search the local taxonomy database and cross-reference with GBIF.")

    search_q = st.text_input("Search species (scientific or common name)", placeholder="e.g. Rastrelliger kanagurta")
    col_local, col_gbif = st.columns(2)
    use_gbif = col_gbif.toggle("Include GBIF lookup", value=True)

    if search_q:
        with st.spinner("Searching…"):
            results = api_get("/species/search", params={"q": search_q, "use_gbif": str(use_gbif).lower()})

        if results:
            local = results.get("local", [])
            gbif  = results.get("gbif", [])

            tab1, tab2 = st.tabs([f"Local results ({len(local)})", f"GBIF results ({len(gbif)})"])

            with tab1:
                if local:
                    df_local = pd.DataFrame(local)
                    st.dataframe(df_local, use_container_width=True)
                else:
                    st.info("No local taxonomy records found. Upload a taxonomy CSV.")

            with tab2:
                if gbif:
                    df_gbif = pd.DataFrame(gbif)
                    st.dataframe(df_gbif, use_container_width=True)
                    st.caption("Data from GBIF API (api.gbif.org)")
                else:
                    st.info("No GBIF results (may require internet access).")

    st.divider()
    st.subheader("Taxonomy database overview")

    tax_data = api_get("/taxonomy", params={"limit": 500})
    if tax_data:
        df_tax = pd.DataFrame(tax_data)
        if not df_tax.empty:
            col1, col2 = st.columns(2)

            with col1:
                if "family" in df_tax.columns:
                    fam_counts = df_tax["family"].value_counts().head(15).reset_index()
                    fam_counts.columns = ["Family", "Count"]
                    fig = px.bar(fam_counts, x="Count", y="Family", orientation="h",
                                 color="Count", color_continuous_scale="Purples",
                                 title="Top families")
                    fig.update_layout(yaxis=dict(autorange="reversed"), height=420)
                    st.plotly_chart(fig, use_container_width=True)

            with col2:
                if "iucn_status" in df_tax.columns:
                    iucn_counts = df_tax["iucn_status"].dropna().value_counts().reset_index()
                    iucn_counts.columns = ["IUCN Status", "Count"]
                    iucn_order = ["EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD", "NE"]
                    iucn_colors = {
                        "EX": "#000000", "EW": "#542344", "CR": "#D4070B",
                        "EN": "#FC7F3F", "VU": "#F9E814", "NT": "#CCE226",
                        "LC": "#60C659", "DD": "#D1D1C6", "NE": "#FFFFFF",
                    }
                    fig = px.pie(iucn_counts, names="IUCN Status", values="Count",
                                 title="IUCN conservation status",
                                 color="IUCN Status",
                                 color_discrete_map=iucn_colors)
                    st.plotly_chart(fig, use_container_width=True)

            st.dataframe(df_tax, use_container_width=True)
        else:
            st.info("No taxonomy data. Upload a taxonomy CSV.")


# ─── 🦷 Otolith Morphology ────────────────────────────────────────────────────

elif page == "🦷 Otolith Morphology":
    st.title("🦷 Otolith Morphology Analysis")
    st.markdown("Shape metrics, age-length relationships, and inter-species morphological comparison.")

    species_filter = st.text_input("Filter by species (optional)")
    data = api_get("/morphology", params={"species": species_filter or None, "limit": 1000})

    if data:
        df = pd.DataFrame(data)
        if df.empty:
            st.info("No morphology data. Upload a morphology CSV with columns like length_mm, width_mm, circularity, etc.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Samples", len(df))
            c2.metric("Species", df["scientific_name"].nunique() if "scientific_name" in df.columns else "—")
            c3.metric("Avg length (mm)", f"{df['length_mm'].dropna().mean():.2f}" if "length_mm" in df.columns else "—")
            c4.metric("Avg circularity", f"{df['circularity'].dropna().mean():.3f}" if "circularity" in df.columns else "—")

            tab1, tab2, tab3, tab4 = st.tabs(["Shape scatter", "Age-length curve", "Species comparison", "Raw data"])

            with tab1:
                x_axis = st.selectbox("X axis", [c for c in ["length_mm", "width_mm", "area_mm2", "perimeter_mm"] if c in df.columns], index=0)
                y_axis = st.selectbox("Y axis", [c for c in ["circularity", "aspect_ratio", "form_factor", "roundness"] if c in df.columns], index=0)
                if x_axis and y_axis:
                    fig = px.scatter(
                        df, x=x_axis, y=y_axis,
                        color="scientific_name" if "scientific_name" in df.columns else None,
                        hover_data=["sample_id", "estimated_age_yr"] if "sample_id" in df.columns else None,
                        title=f"{y_axis} vs {x_axis}",
                        opacity=0.75,
                    )
                    st.plotly_chart(fig, use_container_width=True)

            with tab2:
                if "fish_length_cm" in df.columns and "estimated_age_yr" in df.columns:
                    plot_df = df.dropna(subset=["fish_length_cm", "estimated_age_yr"])
                    if not plot_df.empty:
                        fig = px.scatter(
                            plot_df, x="estimated_age_yr", y="fish_length_cm",
                            color="scientific_name" if "scientific_name" in df.columns else None,
                            trendline="ols",
                            title="Age-length relationship",
                            labels={"estimated_age_yr": "Estimated age (years)", "fish_length_cm": "Fish length (cm)"},
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No age-length data available.")
                else:
                    st.info("Columns `fish_length_cm` and `estimated_age_yr` needed for age-length curves.")

            with tab3:
                if "scientific_name" in df.columns:
                    metric_col = st.selectbox(
                        "Metric to compare",
                        [c for c in ["length_mm", "circularity", "aspect_ratio", "area_mm2"] if c in df.columns],
                    )
                    if metric_col:
                        fig = px.box(
                            df, x="scientific_name", y=metric_col,
                            color="scientific_name",
                            title=f"{metric_col} by species",
                            labels={"scientific_name": "Species"},
                        )
                        fig.update_xaxes(tickangle=30)
                        st.plotly_chart(fig, use_container_width=True)

            with tab4:
                st.dataframe(df, use_container_width=True)
    else:
        st.info("No morphology data or backend not running.")


# ─── 🧬 eDNA & Molecular ──────────────────────────────────────────────────────

elif page == "🧬 eDNA & Molecular":
    st.title("🧬 eDNA & Molecular Biology")

    col1, col2, col3 = st.columns(3)
    species_filter  = col1.text_input("Species filter")
    gene_filter     = col2.text_input("Target gene (e.g. 12S, COI)")
    min_reads_input = col3.number_input("Min read count", value=0, step=10)

    data = api_get("/edna", params={
        "species": species_filter or None,
        "target_gene": gene_filter or None,
        "min_reads": min_reads_input if min_reads_input > 0 else None,
        "limit": 1000,
    })

    if data:
        df = pd.DataFrame(data)
        if df.empty:
            st.info("No eDNA data. Upload an eDNA CSV.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Detections", len(df))
            c2.metric("Species", df["scientific_name"].nunique() if "scientific_name" in df.columns else "—")
            total_reads = df["read_count"].sum() if "read_count" in df.columns else 0
            c3.metric("Total reads", f"{int(total_reads):,}")
            detection_rate = df["detection_status"].mean() if "detection_status" in df.columns else None
            c4.metric("Detection rate", f"{detection_rate*100:.1f}%" if detection_rate else "—")

            tab1, tab2, tab3, tab4 = st.tabs(["Read counts", "Relative abundance", "Detection map", "Raw data"])

            with tab1:
                if "scientific_name" in df.columns and "read_count" in df.columns:
                    top = (df.groupby("scientific_name")["read_count"]
                             .sum().reset_index()
                             .sort_values("read_count", ascending=False).head(20))
                    fig = px.bar(top, x="scientific_name", y="read_count",
                                 color="read_count", color_continuous_scale="Teal",
                                 title="Read counts by species")
                    fig.update_xaxes(tickangle=35)
                    st.plotly_chart(fig, use_container_width=True)

            with tab2:
                if "relative_abundance" in df.columns and "scientific_name" in df.columns:
                    abund = (df.groupby("scientific_name")["relative_abundance"]
                               .mean().reset_index()
                               .sort_values("relative_abundance", ascending=False).head(20))
                    fig = px.pie(abund, names="scientific_name", values="relative_abundance",
                                 title="Relative abundance by species",
                                 hole=0.35, color_discrete_sequence=px.colors.qualitative.Set3)
                    st.plotly_chart(fig, use_container_width=True)

            with tab3:
                if "lat" in df.columns and "lon" in df.columns:
                    map_df = df.dropna(subset=["lat", "lon"])
                    if not map_df.empty:
                        fig = px.scatter_mapbox(
                            map_df, lat="lat", lon="lon",
                            color="scientific_name",
                            size="read_count" if "read_count" in map_df.columns else None,
                            hover_data=["scientific_name", "target_gene", "read_count", "depth_m"],
                            mapbox_style="open-street-map", zoom=3, height=500,
                            title="eDNA detection locations",
                        )
                        st.plotly_chart(fig, use_container_width=True)

            with tab4:
                st.dataframe(df, use_container_width=True)


# ─── 📊 Biodiversity Analytics ────────────────────────────────────────────────

elif page == "📊 Biodiversity Analytics":
    st.title("📊 Biodiversity Analytics")
    st.markdown("Shannon & Simpson diversity indices, cross-domain species overlap, and hotspot analysis.")

    tab1, tab2, tab3 = st.tabs(["Diversity indices", "Cross-domain species", "Geographic hotspots"])

    with tab1:
        bio = api_get("/analytics/biodiversity")
        if bio:
            col1, col2 = st.columns(2)

            for domain_key, domain_label, col in [
                ("fisheries", "🐟 Fisheries", col1),
                ("edna", "🧬 eDNA", col2),
            ]:
                d = bio.get(domain_key, {})
                with col:
                    st.subheader(domain_label)
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Species richness", d.get("species_richness", 0))
                    m2.metric("Shannon index (H')", d.get("shannon_index", "—"))
                    m3.metric("Simpson index (1-D)", d.get("simpson_index", "—"))

                    top = d.get("top_species", [])
                    if top:
                        df_top = pd.DataFrame(top, columns=["Species", "Count"])
                        fig = px.bar(df_top, x="Count", y="Species", orientation="h",
                                     color="Count",
                                     color_continuous_scale="Greens" if domain_key == "fisheries" else "Teal",
                                     title=f"Top {len(df_top)} species")
                        fig.update_layout(yaxis=dict(autorange="reversed"), height=380)
                        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        cross = api_get("/analytics/cross-domain")
        if cross:
            df_cross = pd.DataFrame(cross)
            if not df_cross.empty:
                st.subheader(f"{len(df_cross)} species found across domains")
                # Heatmap of species × domains
                all_domains = ["fisheries", "taxonomy", "edna", "morphology"]
                for d in all_domains:
                    df_cross[d] = df_cross["domains"].apply(lambda x: 1 if d in x else 0)

                top50 = df_cross.head(50)
                heat_data = top50[all_domains].values
                fig = px.imshow(
                    heat_data.T,
                    x=top50["scientific_name"].tolist(),
                    y=all_domains,
                    color_continuous_scale="Blues",
                    title="Species presence across domains",
                    aspect="auto",
                )
                fig.update_xaxes(tickangle=40, tickfont=dict(size=9))
                st.plotly_chart(fig, use_container_width=True)

                st.dataframe(df_cross[["scientific_name", "domain_count", "domains"]], use_container_width=True)
            else:
                st.info("No cross-domain data yet.")

    with tab3:
        hotspots = api_get("/analytics/hotspots")
        if hotspots:
            df_hot = pd.DataFrame(hotspots)
            if not df_hot.empty:
                fig = px.scatter_mapbox(
                    df_hot, lat="lat", lon="lon",
                    size="total_catch_kg",
                    color="total_catch_kg",
                    color_continuous_scale="YlOrRd",
                    hover_data=["total_catch_kg"],
                    mapbox_style="open-street-map", zoom=2, height=500,
                    title="Fishing hotspots (1° grid cells)",
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_hot, use_container_width=True)
            else:
                st.info("No location data. Upload fisheries CSVs with lat/lon.")


# ─── 📥 Export ────────────────────────────────────────────────────────────────

elif page == "📥 Export Data":
    st.title("📥 Export Data")
    st.markdown("Download cleaned, standardised data as CSV for any domain.")

    domain_choice = st.selectbox(
        "Select domain to export",
        ["oceanography", "fisheries", "taxonomy", "morphology", "edna"],
    )

    if st.button("⬇️ Download CSV", type="primary"):
        url = f"{API}/export/csv?domain={domain_choice}"
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                st.download_button(
                    label=f"Save {domain_choice}_export.csv",
                    data=r.content,
                    file_name=f"cmlre_{domain_choice}_export.csv",
                    mime="text/csv",
                )
            else:
                st.error(f"Export failed: {r.text}")
        except Exception as e:
            st.error(f"Could not reach backend: {e}")

    st.divider()
    st.subheader("All ingestion records")
    records = api_get("/records")
    if records:
        df = pd.DataFrame(records)
        st.dataframe(df, use_container_width=True)