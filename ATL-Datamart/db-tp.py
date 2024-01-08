from sqlalchemy import create_engine
from minio import Minio
import pyarrow.parquet as pq
import io
import pandas as pd

# Configuration de la connexion à la base de données MySQL
DATABASE_URI = 'mysql+pymysql://root:@localhost/tp_db'
engine = create_engine(DATABASE_URI)

# Connexion au client MinIO
minioClient = Minio('localhost:9000',
                    access_key='minio',
                    secret_key='minio123',
                    secure=False)

try:
    # Année pour laquelle vous souhaitez importer les données
    annee = '2023'

    # Parcours des fichiers .parquet pour chaque mois de l'année spécifiée
    for mois in range(1, 13):
        nom_fichier = f'{annee}-{mois:02}.parquet'  # Formatage du nom du fichier avec zéro-padding pour le mois
        data_from_minio = minioClient.get_object('mybucket', nom_fichier)

        # Lecture du fichier .parquet
        parquet_data = data_from_minio.read()
        buf = io.BytesIO(parquet_data)
        table = pq.read_table(buf)
        df = table.to_pandas()

        # Insérer les données dans une table spécifique associée au mois
        nom_table = f'data_{annee}_{mois:02}'  # Nom de la table avec le mois
        df.to_sql(nom_table, con=engine, if_exists='append', index=False)

except Exception as e:
    print("Une erreur est survenue :", e)
