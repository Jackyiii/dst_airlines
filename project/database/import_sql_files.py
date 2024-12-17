import subprocess

def run_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Command succeeded: {command}")
        print(result.stdout)
    else:
        print(f"Command failed: {command}")
        print(result.stderr)

def main():
    # Chemin local des fichiers SQL
    local_sql_path = './'

    # Liste des fichiers SQL à copier et importer dans l'ordre correct
    sql_files = [
        'languages.sql',
        'countries.sql',
        'cities.sql',
        'airports.sql',
        'aircraft.sql',
        'flightstatus.sql',
        'airlines.sql',
        'flightschedules.sql'
    ]

    # Copier les fichiers SQL dans le conteneur Docker
    for file in sql_files:
        command = f"docker cp {local_sql_path}{file} postgres_db:/tmp/{file}"
        run_command(command)

    # Importer les fichiers SQL dans la base de données PostgreSQL dans l'ordre spécifié
    for file in sql_files:
        command = f"docker exec -i postgres_db psql -U myuser -d mydatabase -f /tmp/{file}"
        run_command(command)

if __name__ == "__main__":
    main()
