from sqlalchemy import create_engine
from minio import Minio
import pyarrow.parquet as pq
import io
import pandas as pd

# Configuration de la connexion à la base de données MySQL
DATABASE_URI = 'mysql+pymysql://root:@localhost/tb_db'
engine = create_engine(DATABASE_URI)

# Connexion au client MinIO
minioClient = Minio('localhost:9000',
                    access_key='minio',
                    secret_key='minio123',
                    secure=False)

try:
    # Récupération des données depuis MinIO
    data_from_minio = minioClient.get_object('mybucket', '2023-01.parquet')

    # Lecture du fichier .parquet
    parquet_data = data_from_minio.read()

    # Création d'un buffer pour lire les données avec pyarrow
    buf = io.BytesIO(parquet_data)
    table = pq.read_table(buf)
    df = table.to_pandas()

    # Insérez les données directement dans la base de données
    df.to_sql('nom_table_existante', con=engine, if_exists='append', index=False)

except Exception as e:
    print("Une erreur est survenue :", e)

