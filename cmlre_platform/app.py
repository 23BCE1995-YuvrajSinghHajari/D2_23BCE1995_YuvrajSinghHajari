import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="CMLRE Marine Data Platform", layout="wide")

st.title("🌊 CMLRE Unified Marine Data Platform")
st.markdown("### AI-Driven Insights for Oceanographic & Biodiversity Data")

# Sidebar for Navigation
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Upload Data", "View Database Records"])

if page == "Upload Data":
    st.subheader("Upload Oceanographic Dataset")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        # 1. Display Preview
        df = pd.read_csv(uploaded_file)
        st.write("#### Raw Data Preview", df.head())

        # 2. Upload to Backend API
        if st.button("Ingest to Central Database"):
            files = {"file": uploaded_file.getvalue()}
            response = requests.post("http://127.0.0.1:8000/ingest", files={"file": (uploaded_file.name, uploaded_file.getvalue())})
            
            if response.status_code == 200:
                st.success(f"Successfully ingested! Record ID: {response.json().get('record_id')}")
                # Simple Visualization
                if 'temperature' in df.columns:
                    st.line_chart(df['temperature'])
                    st.caption("Real-time Temperature Trend Analysis")
            else:
                st.error("Failed to connect to Backend API.")

elif page == "View Database Records":
    st.subheader("Historical Marine Records")
    # Fetch from Backend
    response = requests.get("http://127.0.0.1:8000/records")
    if response.status_code == 200:
        records = response.json()
        st.table(records)
    else:
        st.warning("Could not fetch records. Is the FastAPI server running?")