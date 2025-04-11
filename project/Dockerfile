FROM python:3.11-slim

# Installation des dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Définition du répertoire de travail
WORKDIR /app

# Copie du fichier requirements.txt
COPY requirements.txt .

#  Forcer les versions compatibles avec le modèle .pkl
RUN pip install --no-cache-dir numpy==1.26.0 \
    scikit-learn==1.6.1 \
    pandas==2.1.4 \
    joblib==1.3.2 && \
    pip install --no-cache-dir -r requirements.txt

# Copier le reste du code après l'installation des libs
COPY . .

# Appliquer les permissions AVANT de changer d'utilisateur
COPY start.sh /start.sh
RUN chmod +x /start.sh

# Gestion des permissions et sécurité
RUN adduser --disabled-password --gecos '' appuser
USER appuser
