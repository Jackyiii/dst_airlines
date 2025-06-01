import psycopg2
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from dateutil.parser import isoparse
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import os
import re

# Configuration de la connexion PostgreSQL
DB_CONFIG = {
    "dbname": "mydatabase",
    "user": "myuser",
    "password": "mypassword",
    "host": "db",
    "port": 5432
}

# Fonction pour exécuter une requête SQL et récupérer les données sous forme de DataFrame
def fetch_data(query):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return None

# Fonction de conversion durée
def duration_to_minutes(duration):
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?', duration)
    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        return hours * 60 + minutes
    return np.nan

# Affichage performances
def plot_model_performance(y_true, y_pred, model_name, labels=["Pas de retard", "Retard"]):
    cm = confusion_matrix(y_true, y_pred)
    plt.figure(figsize=(6, 4))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", xticklabels=labels, yticklabels=labels)
    plt.xlabel("Prédictions")
    plt.ylabel("Vraies valeurs")
    plt.title(f"Matrice de confusion - {model_name}")
    plt.savefig(os.path.join(MODELS_DIR, f"{model_name}_confusion_matrix.png"))
    print(f"Rapport de classification pour {model_name} :\n")
    print(classification_report(y_true, y_pred))

# Fonction principale exécutée dans Airflow
# Exemple de requête pour récupérer les données nécessaires
def run():
    print("Début de l'exécution du pipeline ML.")
    
    query = """
    SELECT 
    fs."FlightNumber",
    fs."DepartureDate",
    
     -- Numéro du jour (1 pour Lundi, 7 pour Dimanche) en ENTIER
    CAST(MOD(EXTRACT(DOW FROM fs."DepartureDate"::DATE) + 6, 7) + 1 AS INTEGER) AS "DepartureDayOfWeekNumber",
    
    fs."ScheduledDepartureLocalTime",
    CAST(MOD(EXTRACT(DOW FROM fs."ScheduledDepartureLocalTime"::TIMESTAMP) + 6, 7) + 1 AS INTEGER) AS "ScheduledDepartureDayOfWeekNumber",

    fs."ActualDepartureLocalTime",
    CAST(MOD(EXTRACT(DOW FROM fs."ActualDepartureLocalTime"::TIMESTAMP) + 6, 7) + 1 AS INTEGER) AS "ActualDepartureDayOfWeekNumber",

    fs."ScheduledArrivalLocalTime",
    CAST(MOD(EXTRACT(DOW FROM fs."ScheduledArrivalLocalTime"::TIMESTAMP) + 6, 7) + 1 AS INTEGER) AS "ScheduledArrivalDayOfWeekNumber",

    fs."ActualArrivalLocalTime",
    CAST(MOD(EXTRACT(DOW FROM fs."ActualArrivalLocalTime"::TIMESTAMP) + 6, 7) + 1 AS INTEGER) AS "ActualArrivalDayOfWeekNumber",
   
    fs."DepartureAirportCode",
    fs."ArrivalAirportCode",
    fs."AircraftCode",
    fs."FlightStatusCode",
    fs."FlightStatusDefinition",
    
    a."UtcOffset" AS "DepartureUtcOffset",
    b."UtcOffset" AS "ArrivalUtcOffset",
    
    c."Duration",
    c."DaysOfOperation",
    c."StopQuantity",

    -- Coalesce pour gérer les valeurs manquantes
    --COALESCE(fs."ActualDepartureLocalTime", fs."ScheduledDepartureLocalTime") AS "FinalDepartureTime1",
    --COALESCE(fs."ActualArrivalLocalTime", fs."ScheduledArrivalLocalTime") AS "FinalArrivalTime1",

    -- faire un Coalesce en ajoutant un cast afin de gérer les valeurs manquantes tout en obtenant un entier
    COALESCE( CAST(MOD(EXTRACT(DOW FROM fs."ActualDepartureLocalTime"::TIMESTAMP) + 6, 7) + 1 AS INTEGER), 
              CAST(MOD(EXTRACT(DOW FROM fs."ScheduledDepartureLocalTime"::TIMESTAMP) + 6, 7) + 1 AS INTEGER)) 
                                                                                                               AS "ActualDepartureDayOfWeekNumber1",

    COALESCE( CAST(MOD(EXTRACT(DOW FROM fs."ActualArrivalLocalTime"::TIMESTAMP) + 6, 7) + 1 AS INTEGER), 
              CAST(MOD(EXTRACT(DOW FROM fs."ScheduledArrivalLocalTime"::TIMESTAMP) + 6, 7) + 1 AS INTEGER) )
                                                                                                              AS "ActualArrivalDayOfWeekNumber1",


    -- Calcul des retards
    CASE
        WHEN fs."ActualDepartureLocalTime" > fs."ScheduledDepartureLocalTime" THEN 1
        ELSE 0
    END AS "DepartureDelay",

    CASE
        WHEN fs."ActualArrivalLocalTime" > fs."ScheduledArrivalLocalTime" THEN 1
        ELSE 0
    END AS "ArrivalDelay"

FROM "status_flight" AS fs
LEFT JOIN "airport" AS a ON fs."DepartureAirportCode" = a."AirportCode"
LEFT JOIN "airport" AS b ON fs."ArrivalAirportCode" = b."AirportCode"
LEFT JOIN "schedules" AS c ON fs."FlightNumber" = c."ScheduleID"
LEFT JOIN "airline" AS d ON c."AirlineID" = d."AirlineID";

"""
# Récupération des données sous forme de DataFrame

    df = fetch_data(query)
    if df is None:
        print("Erreur : aucune donnée chargée.")
        return
        # Affichage des premières lignes pour vérifier

    print(df.head(5))
    # Vérifier la structure des données
    print(df.info())
    print(df.dtypes)
    print(df.shape)
    print(df.describe())
    # Vérifier le nombre de valeurs uniques pour chaque variable catégorielle
    print(df.nunique())
    #voir les modalités de chaque variable
    print(df[['FlightStatusDefinition']].value_counts())
    print(df[['FlightStatusCode']].value_counts())
    print(df[['ArrivalUtcOffset']].value_counts())
    print(df[['AircraftCode']].value_counts())
    print(df[['DaysOfOperation']].value_counts())
    print(df[['Duration']].value_counts())
    print(sorted([int(i) for i in list(df.DaysOfOperation.unique())]))
    print(df[['StopQuantity']].value_counts())
    print(df[['ActualDepartureLocalTime']].value_counts())
    print(df[['ActualArrivalLocalTime']].value_counts())
    print(df['ActualArrivalLocalTime'].unique())
    print(df[['DepartureDayOfWeekNumber']].value_counts())
    print(df[['ScheduledDepartureDayOfWeekNumber']].value_counts())
    print(df[['ActualDepartureDayOfWeekNumber']].value_counts())
    print(df[['ScheduledArrivalDayOfWeekNumber']].value_counts())
    print(df[['ActualArrivalDayOfWeekNumber']].value_counts())

    #remplaçons les modalités non renseignées de la varoable 'FlightStatusDefinition' par Flight Landed

    df.replace({'FlightStatusDefinition': {"No status": "Flight Landed"},
                'FlightStatusCode': {"NA": "LD"}}, inplace=True)
    #verification des valeurs manquantes
    print(df.isnull().sum())
    print((df.isna().sum() / len(df)) * 100)

    #doublons

    print(df.duplicated())
    print(df.duplicated().sum())

    df1 = df.drop_duplicates()
    print((df1.isna().sum() / len(df1)) * 100)

    df1['DurationMinutes'] = df1['Duration'].apply(duration_to_minutes)
    #display(df1[['DurationMinutes']].value_counts())
    #supression des variables inutiles

    df1 = df1.drop(['FlightNumber', 'DepartureDate',
                    'ScheduledDepartureLocalTime', 'ActualDepartureLocalTime',
                    'ActualDepartureDayOfWeekNumber1',
                    'ScheduledArrivalLocalTime', 'ActualArrivalLocalTime',
                    'ActualArrivalDayOfWeekNumber1', 'FlightStatusDefinition',
                    'Duration', 'DepartureUtcOffset', 'ArrivalUtcOffset', 'StopQuantity'], axis=1)

    df1 = df1.dropna(subset=['ActualDepartureDayOfWeekNumber', 'ActualArrivalDayOfWeekNumber'])

    df1[["ActualDepartureDayOfWeekNumber", "ActualArrivalDayOfWeekNumber"]] = df1[["ActualDepartureDayOfWeekNumber", "ActualArrivalDayOfWeekNumber"]].astype(int)
    #convertir mes variables dayofweeknumber en string

    df1[['DepartureDayOfWeekNumber',
         'ActualDepartureDayOfWeekNumber',
         'ActualArrivalDayOfWeekNumber',
         'ScheduledDepartureDayOfWeekNumber',
         'ScheduledArrivalDayOfWeekNumber',
         'DepartureDelay',
         'ArrivalDelay']] = df1[['DepartureDayOfWeekNumber',
                                'ActualDepartureDayOfWeekNumber',
                                'ActualArrivalDayOfWeekNumber',
                                'ScheduledDepartureDayOfWeekNumber',
                                'ScheduledArrivalDayOfWeekNumber',
                                'DepartureDelay',
                                'ArrivalDelay']].astype(str)

    print(df1.info())
    print(df1.head(5))
    print(df1.shape)
	
    
    #separation des données en train et test
    X = df1.drop(["DepartureDelay", "ArrivalDelay"], axis=1)
    y = df1[["DepartureDelay", "ArrivalDelay"]]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
    y_train_dep = y_train["DepartureDelay"]
    y_train_arr = y_train["ArrivalDelay"]
    y_test_dep = y_test["DepartureDelay"]
    y_test_arr = y_test["ArrivalDelay"]

    # Séparation des variables catégorielles et numériques

    cat_features = ["DepartureDayOfWeekNumber", "DepartureAirportCode", "ArrivalAirportCode",
                    "AircraftCode", "FlightStatusCode", "DaysOfOperation",
                    "ActualDepartureDayOfWeekNumber", "ActualArrivalDayOfWeekNumber",
                    'ScheduledDepartureDayOfWeekNumber', 'ScheduledArrivalDayOfWeekNumber']
    num_features = ["DurationMinutes"]

    # Transformation des données

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), num_features),
            ('cat', OneHotEncoder(handle_unknown='ignore'), cat_features)
        ]
    )

    rf_pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('classifier', RandomForestClassifier(n_estimators=100, random_state=42))
    ])
    # Création des pipelines pour chaque modèle

    lr_pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('classifier', LogisticRegression(max_iter=1000))
    ])

    # Modèle RandomForest - Retard au départ
    print("\nModèle RandomForest - Prédiction du retard au départ")
    rf_pipeline.fit(X_train, y_train_dep)
    y_pred_rf_dep = rf_pipeline.predict(X_test)
    print(classification_report(y_test_dep, y_pred_rf_dep))

    # Modèle LogisticRegression - Retard au départ
    print("\nModèle LogisticRegression - Prédiction du retard au départ")
    lr_pipeline.fit(X_train, y_train_dep)
    y_pred_lr_dep = lr_pipeline.predict(X_test)
    print(classification_report(y_test_dep, y_pred_lr_dep))

    # Modèle RandomForest - Retard à l'arrivée
    print("\nModèle RandomForest - Prédiction du retard à l'arrivée")
    rf_pipeline.fit(X_train, y_train_arr)
    y_pred_rf_arr = rf_pipeline.predict(X_test)
    print(classification_report(y_test_arr, y_pred_rf_arr))

    # Modèle LogisticRegression - Retard à l'arrivée
    print("\nModèle LogisticRegression - Prédiction du retard à l'arrivée")
    lr_pipeline.fit(X_train, y_train_arr)
    y_pred_lr_arr = lr_pipeline.predict(X_test)
    print(classification_report(y_test_arr, y_pred_lr_arr))

    # Sauvegarde des modèles
    MODELS_DIR = "/opt/airflow/models"
    os.makedirs(MODELS_DIR, exist_ok=True)
    rf_model_depart_path = os.path.join(MODELS_DIR, "random_forest_depart.pkl")
    rf_model_arrivee_path = os.path.join(MODELS_DIR, "random_forest_arrivee.pkl")
    lr_model_path = os.path.join(MODELS_DIR, "logistic_regression.pkl")

    joblib.dump(rf_pipeline, rf_model_depart_path)
    joblib.dump(rf_pipeline, rf_model_arrivee_path)

    print(f"Modèle RandomForest (retard au départ) enregistré à : {rf_model_depart_path}")
    print(f"Modèle RandomForest (retard à l'arrivée) enregistré à : {rf_model_arrivee_path}")

    # Matrices de confusion
    plot_model_performance(y_test_dep, y_pred_rf_dep, "RandomForest - Retard au départ")
    plot_model_performance(y_test_arr, y_pred_lr_arr, "Logistic Regression - Retard à l'arrivée")

    print("Fin du pipeline ML.")

# Permet d'exécuter ce fichier seul ou depuis Airflow
if __name__ == "__main__":
    run()
