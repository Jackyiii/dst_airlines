import pandas as pd
from sqlalchemy import create_engine

# Configuration de la connexion PostgreSQL avec SQLAlchemy
DB_CONFIG = {
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost",
    "port": 5432,
    "dbname": "mydatabase"
}

# Création de la chaîne de connexion pour SQLAlchemy
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# Fonction pour exécuter une requête SQL et récupérer les données sous forme de DataFrame
def fetch_data(query):
    try:
        # Création de l'engine de connexion avec SQLAlchemy
        engine = create_engine(DATABASE_URL)
        # Exécution de la requête SQL et récupération des données dans un DataFrame
        df = pd.read_sql_query(query, engine)
        return df
    except Exception as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return None

# Exemple de requête pour récupérer les données nécessaires
query = """
    SELECT * FROM
    city
    LIMIT 5
"""

# Récupération des données sous forme de DataFrame
df = fetch_data(query)

# Affichage des premières lignes pour vérifier
if df is not None:
    print(df)
