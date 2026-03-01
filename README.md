# 🌊 AI-Driven Unified Data Platform for CMLRE

### *Ministry of Earth Sciences (MoES) Project*
**Developed by:** Yuvraj Singh Hajari | **Reg No:** 23BCE1995
**Institution:** Vellore Institute of Technology (VIT)

---

## 📌 Project Overview
This platform is designed for the **Centre for Marine Living Resources and Ecology (CMLRE)** to unify siloed marine datasets into a single, AI-enabled ecosystem. It facilitates the ingestion, standardization, and analysis of heterogeneous data, including Oceanographic, Fisheries, and Molecular Biodiversity insights.

## 🚀 50% Implementation Progress (Week 5)
We have successfully moved from design to a functional MVP (Minimum Viable Product).

### ✅ Core Features Implemented:
* **Automated Data Ingestion:** FastAPI backend capable of handling multi-modal CSV datasets.
* **Persistent Data Backbone:** SQLite database integration with SQLAlchemy ORM for secure, long-term storage.
* **Scientist Dashboard:** Interactive Streamlit UI featuring real-time data previews and trend visualizations.
* **Architectural Foundation:** Full UML modeling (Use Case, Class, Activity, and Sequence Diagrams).

---

## 🏗️ System Architecture

### Use Case Diagram
Describes the interaction between the Marine Scientist and the Data Platform.

### Class Diagram
Defines the inheritance structure for `MarineDataset`, `MolecularData`, and `OtolithData`.

### Sequence Diagram
Visualizes the step-by-step logic of species matching using external APIs (e.g., BLAST/GenBank).

### Activity Diagram
Maps the system workflow from user authentication to AI-driven report generation.

---

## 🛠️ Tech Stack
* **Backend:** Python, FastAPI, Uvicorn
* **Frontend:** Streamlit
* **Database:** SQLite, SQLAlchemy
* **Data Analysis:** Pandas, NumPy
* **Design Tools:** StarUML

---
