import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, inspect

# Configuration de la connexion à la base de données MySQL existante
DATABASE_URI = 'mysql+pymysql://root@localhost:3306/tp_db'
NEW_DATABASE_URI = 'mysql+pymysql://root@localhost:3306/tp_db_f'  # Changer pour la nouvelle base de données

# Création du moteur SQLAlchemy pour les deux bases de données
engine_existing = create_engine(DATABASE_URI)
engine_new = create_engine(NEW_DATABASE_URI)




# Récupération des noms des tables de la base de données existante
inspector = inspect(engine_existing)
table_names = inspector.get_table_names()

# Itération sur chaque table pour créer et insérer les données dans la nouvelle base de données
for table_name in table_names:
    # Chargement des métadonnées de la table existante
    table = Table(table_name, autoload_with=engine_existing)

    # Lecture des données de la table existante
    query = select(table)
    df = pd.read_sql(query, con=engine_existing)

    # Insertion des données dans la nouvelle table de la nouvelle base de données
    df.to_sql(table_name, con=engine_new, if_exists='replace', index=False)
