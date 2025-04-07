from fastapi import FastAPI, HTTPException
import joblib
import pandas as pd
import os
from pydantic import BaseModel

# Chemin du modèle dans le container Docker
MODEL_PATH = os.path.join("models", "random_forest.pkl")

# Vérifier que le modèle existe
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Le fichier du modèle est introuvable à l'emplacement : {MODEL_PATH}")

# Charger le modèle
model = joblib.load(MODEL_PATH)

# FastAPI init
app = FastAPI(title="API de Prédiction de Retard de Vols")

class FlightData(BaseModel):
    DepartureDayOfWeekNumber: str
    DepartureAirportCode: str
    ArrivalAirportCode: str
    AircraftCode: str
    FlightStatusCode: str
    DaysOfOperation: str
    ActualDepartureDayOfWeekNumber: str
    ActualArrivalDayOfWeekNumber: str
    ScheduledDepartureDayOfWeekNumber: str
    ScheduledArrivalDayOfWeekNumber: str
    DurationMinutes: float  

@app.post("/predict")
def predict_delay(data: FlightData):
    try:
        df_input = pd.DataFrame([data.dict()])
        prediction = model.predict(df_input)
        return {"prediction": int(prediction[0])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la prédiction: {str(e)}")
