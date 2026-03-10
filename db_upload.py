import geopandas as gpd
from sqlalchemy import create_engine, text
import pyogrio
import config           # Configurações do projeto
import database_config  # Configurações da base de dados
import logging
import os

logger = logging.getLogger(__name__)

def upload_to_postgres():
    if not os.path.exists(config.OUTPUT_GPKG):
        logger.error("Ficheiro GeoPackage não encontrado para upload.")
        return

    # 1. Criar conexão usando o novo ficheiro de config
    engine = create_engine(database_config.DATABASE_URL)

    try:
        # 2. Garantir que o Schema existe e o PostGIS está ativo
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {database_config.SCHEMA_NAME};"))
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            conn.commit()
            logger.info(f"Schema '{database_config.SCHEMA_NAME}' pronto no Postgres.")

        # 3. Listar camadas no GPKG
        layers = pyogrio.list_layers(config.OUTPUT_GPKG)[:, 0]

        for layer in layers:
            logger.info(f"A carregar camada '{layer}'...")
            gdf = gpd.read_file(config.OUTPUT_GPKG, layer=layer)

            # 4. Upload para o PostGIS
            gdf.to_postgis(
                name=layer,
                con=engine,
                schema=database_config.SCHEMA_NAME,
                if_exists='replace',
                index=False
            )
            logger.info(f"Tabela '{database_config.SCHEMA_NAME}.{layer}' atualizada.")

        logger.info("--- Upload para Base de Dados concluído ---")

    except Exception as e:
        logger.error(f"Erro no upload para a base de dados: {e}")