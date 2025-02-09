#!/bin/bash
echo "Starting main.py" > /log/output.log
python main.py >> /log/output.log 2>&1
#####test
export POSTGRES_USER=myuser
export POSTGRES_PASSWORD=mypassword
export POSTGRES_DB=mydatabase
export POSTGRES_PASSWORD=mypassword

# Démarrer PostgreSQL avec Docker (par exemple)
docker run --name postgres-container -e POSTGRES_USER -e POSTGRES_PASSWORD -e POSTGRES_DB -d postgres
