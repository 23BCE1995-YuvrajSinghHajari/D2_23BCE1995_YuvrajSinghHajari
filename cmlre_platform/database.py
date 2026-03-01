from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

# Create the database file locally
SQLALCHEMY_DATABASE_URL = "sqlite:///./marine_data.db"

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# This class defines the "MarineRecord" table in our database
class MarineRecord(Base):
    __tablename__ = "marine_records"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String)
    upload_date = Column(DateTime, default=datetime.datetime.utcnow)
    species_count = Column(Integer)
    avg_temp = Column(Float)
    standard_type = Column(String, default="Darwin Core")

# Create the tables
Base.metadata.create_all(bind=engine)