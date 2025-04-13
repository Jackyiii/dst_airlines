from fastapi import FastAPI, HTTPException
import joblib
import pandas as pd
import os
from pydantic import BaseModel

# Chemins des modèles
MODEL_DEPART_PATH = os.path.join("models", "random_forest_depart.pkl")
MODEL_ARRIVEE_PATH = os.path.join("models", "random_forest_arrivee.pkl")

# Vérification des fichiers modèles
if not os.path.exists(MODEL_DEPART_PATH):
    raise FileNotFoundError(f"Modèle 'random_forest_depart.pkl' introuvable à : {MODEL_DEPART_PATH}")
if not os.path.exists(MODEL_ARRIVEE_PATH):
    raise FileNotFoundError(f"Modèle 'random_forest_arrivee.pkl' introuvable à : {MODEL_ARRIVEE_PATH}")

# Chargement des modèles
model_depart = joblib.load(MODEL_DEPART_PATH)
model_arrivee = joblib.load(MODEL_ARRIVEE_PATH)

# Initialisation de l'API
app = FastAPI(title="API Prédiction Retards Vols")

# Schéma d'entrée
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
def predict_delays(data: FlightData):
    try:
        df_input = pd.DataFrame([data.dict()])
        
        pred_depart = model_depart.predict(df_input)[0]
        pred_arrivee = model_arrivee.predict(df_input)[0]

        return {
            "departure_delay_prediction": int(pred_depart),
            "arrival_delay_prediction": int(pred_arrivee)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la prédiction : {str(e)}")
