import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd

load_dotenv()

#Variables d'environnement à récupérer
server = os.getenv('AZURE_SQL_SERVER')
database = os.getenv('AZURE_SQL_DATABASE')
username = os.getenv('AZURE_SQL_USERNAME')
password = os.getenv('AZURE_SQL_PASSWORD')


#connection_string = os.getenv("AZURE_SQL_CONNECTIONSTRING")
connection_string = f'Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server},1433;Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;'

#Connexion à la base de données
conn = pyodbc.connect(connection_string)


# création du dossier pour les CSV
os.makedirs("csv", exist_ok = True)


# Requête pour récupérer les noms des tables des schémas Person, Sales et Production

schema_query = """
SELECT TABLE_NAME, TABLE_SCHEMA
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('Person', 'Sales', 'Production') 
  AND TABLE_TYPE = 'BASE TABLE';
"""

#Exécuter la requête pour récupérer les noms des tables

cursor = conn.cursor()
cursor.execute(schema_query)
tables = cursor.fetchall()

# Parcourir chaque table et sauvegarder son contenu dans un CSV

for table_name,schema_name in tables:
    try:
        print(f"Extraction de la table : {schema_name}.{table_name}")
        # Charger les données de la table dans un Dataframe Pandas
        query = f"SELECT * FROM {schema_name}.{table_name};"
        df = pd.read_sql(query, conn)

        # Sauvegarder le Dataframe dans un fichier CSV
        csv_filename = f"csv/{schema_name}_{table_name}.csv"
        df.to_csv(csv_filename, index=False, encoding ='utf-8')
        print(f"Table {schema_name}.{table_name} sauvegardée dans {csv_filename}.")
    except Exception as e:
        print(f"Erreur lors de l'extraction de {schema_name}.{table_name} : {e}")
#Fermer
cursor.close()
conn.close()