from minio import Minio
from minio.error import S3Error
import requests
from tqdm import tqdm
from datetime import datetime
import io

def main():
    # Initialiser le client MinIO
    minioClient = Minio('localhost:9000',
                        access_key='minio',
                        secret_key='minio123',
                        secure=False)

    # Vérifier si le seau existe, sinon le créer
    if not minioClient.bucket_exists("mybucket"):
        minioClient.make_bucket("mybucket")

    # Lien pour les données de l'année 2023
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    year = 2023

    # Initialiser la barre de progression
    months = range(1, 13)
    total_months = len(months)
    bar = tqdm(total=total_months, desc="Downloading")

    for month in months:
        # Construction du lien pour chaque mois de l'année 2023
        month_str = str(month).zfill(2)
        url = f"{base_url}{year}-{month_str}.parquet"

        # Téléchargement du fichier Parquet depuis le lien
        response = requests.get(url)

        if response.status_code == 200:
            # Nom de l'objet dans le bucket MinIO
            object_name = f"{year}-{month_str}.parquet"

            # Enregistrement des données dans le bucket MinIO
            try:
                # Utilisation de io.BytesIO pour envelopper les données bytes
                data_stream = io.BytesIO(response.content)

                minioClient.put_object(
                    "mybucket",
                    object_name,
                    data_stream,
                    len(response.content),
                    content_type='application/octet-stream'
                )
                bar.update(1)  # Mise à jour de la barre de progression
            except S3Error as e:
                print("Error:", e)
        else:
            print(f"Échec du téléchargement du fichier Parquet depuis {url}. Code d'état HTTP : {response.status_code}")

    # Fermer la barre de progression
    bar.close()

if __name__ == "__main__":
    main()
