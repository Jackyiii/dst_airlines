# dst_airlines

# Documentation API FastAPI – Prédiction de Retards de Vol

Cette API permet de prédire si un vol connaîtra un **retard au départ** et/ou à **l’arrivée**, à l’aide de modèles Random Forest entraînés préalablement.
---
# Tech Stack

- **FastAPI** – Serveur d’API léger et rapide
- **Scikit-learn** – Entraînement des modèles ML
- **Joblib** – Chargement des modèles `.pkl`
- **Pandas** – Manipulation des données
- **Docker** – Conteneurisation
---
#  Lancer l’API via Docker (executer les differentes etapes ci dessous dans l'ordre)

- docker compose up
- python api_ml.py
- pip install fastapi uvicorn scikit-learn pandas joblib
- uvicorn api_ml:app --reload


L’API sera accessible ici : [`http://localhost:8000`](http://127.0.0.1:8000/docs#/)

---
# Tester l'endpoint `/predict`

#  POST `/predict`

- **Content-Type** : `application/json`

# Exemple de payload :

```json
{
  "DepartureDayOfWeekNumber": "1",
  "DepartureAirportCode": "CDG",
  "ArrivalAirportCode": "JFK",
  "AircraftCode": "320",
  "FlightStatusCode": "LD",
  "DaysOfOperation": "1234567",
  "ActualDepartureDayOfWeekNumber": "1",
  "ActualArrivalDayOfWeekNumber": "1",
  "ScheduledDepartureDayOfWeekNumber": "1",
  "ScheduledArrivalDayOfWeekNumber": "1",
  "DurationMinutes": 480
}
```
# Réponse attendue :

```json
{
  "departure_delay_prediction": 1,
  "arrival_delay_prediction": 0
}
```




#  Airflow – Orchestration Pipeline DST Airlines (DAG : `airlines_etl_ml_pipeline`)
- les differentes etapes pour faire fonctionner le pipeline dans airflow

# Lancer Airflow avec Docker

# 1. Fichiers requis

- `airline_pipeline.py` (dans le dossier `dags/`)
- `Dockerfile.airflow`
- `docker-compose.airflow.yml`
- `/project_root` : contient `main.py`, `new_dataframe.py`, 

# 2. Démarrer les services

```bash
docker-compose -f docker-compose.airflow.yml up --build
```

Accès à l’interface : http://localhost:8081/

Identifiants par défaut :  
Login: `airflow`  
Password: `airflow`


# Configuration importante

Dans `docker-compose.airflow.yml`, assurez-vous que les volumes sont correctement définis :

```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./project_root:/opt/airflow/project_root
  - ./models:/opt/airflow/models
```

Cela permet :
- à Airflow de lire les DAGs
- d’accéder aux scripts de transformation et ML
- de sauvegarder les modèles `.pkl` pour l’API

---

# Email de notification

Un email est envoyé à `emperatornang@gmail.com` lorsque le pipeline se termine sans erreur.

une notification est egament envoyé si le pipeline ne fonctionne pas correctement jusqu'a la fin.
