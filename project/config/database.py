import os
import psycopg2

#from sqlalchemy import create_engine 
#import sqlalchemy 
from sqlalchemy import create_engine
import pandas as pd



#DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@db:5432/{os.getenv('POSTGRES_DB')}"
DATABASE_URL  = 'postgresql://myuser:mypassword@db:5432/mydatabase'
def get_connection():
    try:
        connection = psycopg2.connect(DATABASE_URL)
        print("Connected to PostgreSQL database!")
        return connection
    except Exception as e:
        print("Error connecting to database:", e)
        return None

def test_connection():
    connection = get_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            print("Database version:", db_version)
            cursor.close()
        except Exception as e:
            print("Error executing test query:", e)
        finally:
            connection.close()





