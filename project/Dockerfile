FROM python:3.11-slim

# Installation des dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Définition du répertoire de travail
WORKDIR /app

# Optimisation du cache Docker : installation des dépendances en premier
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste des fichiers après installation des dépendances
COPY . .

# Appliquer les permissions AVANT de changer d'utilisateur
COPY start.sh /start.sh
RUN chmod +x /start.sh

# Gestion des permissions et sécurité
RUN adduser --disabled-password --gecos '' appuser
USER appuser
