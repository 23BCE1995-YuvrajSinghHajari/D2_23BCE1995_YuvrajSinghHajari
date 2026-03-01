from fastapi import FastAPI, UploadFile, File, Depends
from sqlalchemy.orm import Session
import pandas as pd
import io
from database import SessionLocal, MarineRecord

app = FastAPI()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/ingest")
async def ingest_marine_data(file: UploadFile = File(...), db: Session = Depends(get_db)):
    content = await file.read()
    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
    
    # Simple logic to extract data for the DB (Assuming CSV has 'temp' and 'species' columns)
    # If columns don't exist, we use dummy values for the demo
    temp = df['temperature'].mean() if 'temperature' in df.columns else 25.5
    count = len(df)

    # Creating a database entry
    new_record = MarineRecord(
        filename=file.filename,
        species_count=count,
        avg_temp=temp
    )
    
    db.add(new_record)
    db.commit()
    db.refresh(new_record)
    
    return {"status": "Saved to Database", "record_id": new_record.id}

@app.get("/records")
def get_all_records(db: Session = Depends(get_db)):
    return db.query(MarineRecord).all()