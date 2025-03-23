from fastapi import FastAPI, HTTPException
import joblib
import pandas as pd
import os
from pydantic import BaseModel

# Charger le modèle depuis le fichier sauvegardé
MODEL_PATH = os.path.join(os.path.expanduser("~"), "Documents", "random_forest.pkl")  
model = joblib.load(MODEL_PATH)

# Initialisation de l'application FastAPI
app = FastAPI(title="API de Prédiction de Retard de Vols")

# Définition du format des données d'entrée à l'API
class FlightData(BaseModel):
    DepartureDayOfWeekNumber: str  # Jour de départ en semaine (ex: "1" pour lundi)
    DepartureAirportCode: str  # Code de l'aéroport de départ (ex: "JFK")
    ArrivalAirportCode: str  # Code de l'aéroport d'arrivée (ex: "LAX")
    AircraftCode: str  # Code de l'avion utilisé (ex: "A320")
    FlightStatusCode: str  # Code de statut du vol (ex: "LD")
    DaysOfOperation: str  # Jours d'opération du vol
    ActualDepartureDayOfWeekNumber: str  # Jour réel du départ en semaine
    ActualArrivalDayOfWeekNumber: str  # Jour réel de l'arrivée en semaine
    ScheduledDepartureDayOfWeekNumber: str  # Jour programmé du départ
    ScheduledArrivalDayOfWeekNumber: str  # Jour programmé de l'arrivée
    DurationMinutes: float  
    
# Définition de la route API pour la prédiction du retard des vols
@app.post("/predict")
def predict_delay(data: FlightData):
    try:
        # Conversion des données d'entrée en DataFrame Pandas
        df_input = pd.DataFrame([data.dict()])
        
        # Effectuer la prédiction avec le modèle chargé
        prediction = model.predict(df_input)
        
        # Retourner la prédiction sous forme de JSON
        return {"prediction": int(prediction[0])}
    except Exception as e:
        # Gestion des erreurs et retour d'une réponse HTTP 500 en cas de problème
        raise HTTPException(status_code=500, detail=f"Erreur lors de la prédiction: {str(e)}")
