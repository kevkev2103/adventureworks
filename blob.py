import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from azure.storage.blob import generate_container_sas, ContainerSasPermissions, ContainerClient

# Je charge les variables d'environnement 
load_dotenv()

#Je récupère les variables d'env

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_ACCOUNT_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")
sas_duration_hours = int(os.getenv("AZURE_SAS_DURATION_HOURS", 1))

def generate_sas_token (account_name, account_key, container_name, duration_hours):

    '''Je génère un SAS token pour un conteneur Blob en utilisant la clé principale.'''

    #Définir la période de validité du SAS
    start_time = datetime.now(timezone.utc)
    expiry_time = start_time + timedelta(hours=duration_hours)

    #Générer le SAS token
    sas_token = generate_container_sas(
        account_name = account_name,
        container_name=container_name,
        account_key = account_key,
        permission= ContainerSasPermissions(read=True, list=True),
        expiry = expiry_time,
        start= start_time

    )
    return sas_token

# Génération de l'URL SAS

sas_token = generate_sas_token(account_name, account_key, container_name, sas_duration_hours)
sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}?{sas_token}"

# Créer un client pour le conteneur
container_client = ContainerClient.from_container_url(sas_url)

#Télécharger tous les blobs dans un dossier local
download_folder = "./downloaded_blobs"
os.makedirs(download_folder, exist_ok=True)

for blob in container_client.list_blobs(name_starts_with='product_eval'):

    #Créer un client blob pour le blob actuel
    blob_name = blob.name
    print('C1', blob_name)
    blob_client = container_client.get_blob_client(blob.name)

    # Créer le chemin local pour enregistrer le fichier
    local_file_path = os.path.join(download_folder, blob.name)
    print('C2', download_folder)
    print('C3', local_file_path)
    os.makedirs(os.path.dirname(download_folder), exist_ok = True)

    #Télécharger le blob
    with open(local_file_path,"wb") as file:
        file.write(blob_client.download_blob().readall())
    print(f"Blob téléchargé: {local_file_path}")

print("Téléchargement terminé.")