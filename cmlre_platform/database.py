"""
CMLRE Unified Marine Data Platform — Database Models
Covers: oceanography, fisheries, taxonomy, morphology, eDNA
Metadata follows Darwin Core standard fields where applicable.
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime,
    Text, JSON, ForeignKey, Boolean
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import datetime

SQLALCHEMY_DATABASE_URL = "sqlite:///./marine_data.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ─── Darwin Core compliant base record ────────────────────────────────────────

class MarineRecord(Base):
    """Master ingestion log — one row per uploaded file."""
    __tablename__ = "marine_records"

    id                  = Column(Integer, primary_key=True, index=True)
    filename            = Column(String, nullable=False)
    domain              = Column(String, default="oceanography")   # oceanography | fisheries | taxonomy | morphology | edna
    upload_date         = Column(DateTime, default=datetime.datetime.utcnow)
    species_count       = Column(Integer, default=0)
    avg_temp            = Column(Float, nullable=True)
    standard_type       = Column(String, default="Darwin Core")
    record_count        = Column(Integer, default=0)
    uploader            = Column(String, default="anonymous")
    notes               = Column(Text, nullable=True)

    # Relationships
    oceanography_records = relationship("OceanographyRecord", back_populates="source_file", cascade="all, delete")
    fisheries_records    = relationship("FisheriesRecord",    back_populates="source_file", cascade="all, delete")
    taxonomy_records     = relationship("TaxonomyRecord",     back_populates="source_file", cascade="all, delete")
    morphology_records   = relationship("MorphologyRecord",   back_populates="source_file", cascade="all, delete")
    edna_records         = relationship("EDNARecord",         back_populates="source_file", cascade="all, delete")


# ─── Oceanography ─────────────────────────────────────────────────────────────

class OceanographyRecord(Base):
    __tablename__ = "oceanography_records"

    id              = Column(Integer, primary_key=True, index=True)
    source_file_id  = Column(Integer, ForeignKey("marine_records.id"), nullable=False)
    # Darwin Core location fields
    decimal_latitude    = Column(Float, nullable=True)
    decimal_longitude   = Column(Float, nullable=True)
    depth_m             = Column(Float, nullable=True)
    event_date          = Column(String, nullable=True)    # ISO 8601
    # Oceanographic measurements
    temperature_c       = Column(Float, nullable=True)
    salinity_psu        = Column(Float, nullable=True)
    dissolved_oxygen    = Column(Float, nullable=True)
    ph                  = Column(Float, nullable=True)
    chlorophyll_a       = Column(Float, nullable=True)
    turbidity           = Column(Float, nullable=True)
    current_speed_ms    = Column(Float, nullable=True)
    station_id          = Column(String, nullable=True)

    source_file = relationship("MarineRecord", back_populates="oceanography_records")


# ─── Fisheries ────────────────────────────────────────────────────────────────

class FisheriesRecord(Base):
    __tablename__ = "fisheries_records"

    id              = Column(Integer, primary_key=True, index=True)
    source_file_id  = Column(Integer, ForeignKey("marine_records.id"), nullable=False)
    # Darwin Core taxonomy
    scientific_name     = Column(String, nullable=True)
    vernacular_name     = Column(String, nullable=True)
    taxon_rank          = Column(String, nullable=True)
    # Fisheries-specific
    catch_weight_kg     = Column(Float, nullable=True)
    catch_count         = Column(Integer, nullable=True)
    gear_type           = Column(String, nullable=True)
    vessel_id           = Column(String, nullable=True)
    fishing_area        = Column(String, nullable=True)
    decimal_latitude    = Column(Float, nullable=True)
    decimal_longitude   = Column(Float, nullable=True)
    event_date          = Column(String, nullable=True)
    maturity_stage      = Column(String, nullable=True)
    length_cm           = Column(Float, nullable=True)
    weight_g            = Column(Float, nullable=True)

    source_file = relationship("MarineRecord", back_populates="fisheries_records")


# ─── Taxonomy ─────────────────────────────────────────────────────────────────

class TaxonomyRecord(Base):
    __tablename__ = "taxonomy_records"

    id              = Column(Integer, primary_key=True, index=True)
    source_file_id  = Column(Integer, ForeignKey("marine_records.id"), nullable=False)
    # Darwin Core classification
    kingdom         = Column(String, nullable=True)
    phylum          = Column(String, nullable=True)
    class_name      = Column(String, nullable=True)
    order_name      = Column(String, nullable=True)
    family          = Column(String, nullable=True)
    genus           = Column(String, nullable=True)
    species         = Column(String, nullable=True)
    scientific_name = Column(String, nullable=True, index=True)
    vernacular_name = Column(String, nullable=True)
    taxon_id        = Column(String, nullable=True)       # e.g. WoRMS AphiaID
    gbif_id         = Column(String, nullable=True)
    iucn_status     = Column(String, nullable=True)
    habitat         = Column(String, nullable=True)
    distribution    = Column(Text, nullable=True)
    authority       = Column(String, nullable=True)       # taxonomic authority

    source_file = relationship("MarineRecord", back_populates="taxonomy_records")


# ─── Otolith Morphology ───────────────────────────────────────────────────────

class MorphologyRecord(Base):
    __tablename__ = "morphology_records"

    id              = Column(Integer, primary_key=True, index=True)
    source_file_id  = Column(Integer, ForeignKey("marine_records.id"), nullable=False)
    scientific_name = Column(String, nullable=True)
    sample_id       = Column(String, nullable=True)
    # Image metadata
    image_filename  = Column(String, nullable=True)
    image_path      = Column(String, nullable=True)
    # Shape metrics (computed)
    length_mm       = Column(Float, nullable=True)
    width_mm        = Column(Float, nullable=True)
    area_mm2        = Column(Float, nullable=True)
    perimeter_mm    = Column(Float, nullable=True)
    aspect_ratio    = Column(Float, nullable=True)
    circularity     = Column(Float, nullable=True)
    form_factor     = Column(Float, nullable=True)
    rectangularity  = Column(Float, nullable=True)
    roundness       = Column(Float, nullable=True)
    # Biological context
    fish_length_cm  = Column(Float, nullable=True)
    fish_weight_g   = Column(Float, nullable=True)
    estimated_age_yr= Column(Float, nullable=True)
    event_date      = Column(String, nullable=True)
    decimal_latitude = Column(Float, nullable=True)
    decimal_longitude= Column(Float, nullable=True)
    notes           = Column(Text, nullable=True)

    source_file = relationship("MarineRecord", back_populates="morphology_records")


# ─── eDNA / Molecular Biology ────────────────────────────────────────────────

class EDNARecord(Base):
    __tablename__ = "edna_records"

    id              = Column(Integer, primary_key=True, index=True)
    source_file_id  = Column(Integer, ForeignKey("marine_records.id"), nullable=False)
    sample_id       = Column(String, nullable=True, index=True)
    # Location / Darwin Core
    decimal_latitude    = Column(Float, nullable=True)
    decimal_longitude   = Column(Float, nullable=True)
    depth_m             = Column(Float, nullable=True)
    event_date          = Column(String, nullable=True)
    # Sequencing metadata
    target_gene         = Column(String, nullable=True)   # e.g. 12S, COI, 16S
    sequencing_platform = Column(String, nullable=True)   # e.g. Illumina MiSeq
    primer_pair         = Column(String, nullable=True)
    # Taxonomy (assigned)
    scientific_name     = Column(String, nullable=True)
    taxon_rank          = Column(String, nullable=True)
    # Abundance / diversity
    read_count          = Column(Integer, nullable=True)
    relative_abundance  = Column(Float, nullable=True)
    # Quality
    sequence_quality    = Column(Float, nullable=True)
    detection_status    = Column(Boolean, default=True)
    notes               = Column(Text, nullable=True)

    source_file = relationship("MarineRecord", back_populates="edna_records")


# Create all tables
Base.metadata.create_all(bind=engine)