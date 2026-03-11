import geopandas as gpd
import osmnx as ox
import logging
import os
from datetime import datetime
from sqlalchemy import create_engine, text
import config
import database_config
from shapely.ops import transform
import pyproj

# Configuração do Logging
os.makedirs(os.path.dirname(config.LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def export_to_process():
    start_time = datetime.now()
    logger.info(f"--- Início do Processamento: {config.MUNICIPIO_ALVO} ---")

    try:
        # 1. Gestão da bd
        engine = None
        if getattr(database_config, 'UPLOAD_TO_POSTGRES', False):
            try:
                engine = create_engine(database_config.DATABASE_URL)
                with engine.connect() as conn:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {database_config.SCHEMA_NAME};"))
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
                    conn.commit()
                logger.info(f"Ligação à DB OK. Schema '{database_config.SCHEMA_NAME}' verificado.")
            except Exception as e:
                logger.error(f"Erro ao ligar à DB. O upload será desativado. Erro: {e}")
                database_config.UPLOAD_TO_POSTGRES = False

        # 2. Gestão do Ficheiro Local (Limpar ficheiro antigo se vamos criar um novo)
        if getattr(database_config, 'SAVE_LOCAL_FILE', False):
            if os.path.exists(config.OUTPUT_GPKG):
                os.remove(config.OUTPUT_GPKG)
                logger.info(f"Ficheiro antigo {config.OUTPUT_GPKG} removido para nova escrita.")

        # --- 3. Obter Geometria da CAOP ---
        if not os.path.exists(config.CAOP_PATH):
            logger.error(f"CAOP não encontrada em: {config.CAOP_PATH}")
            return

        # Filtro para apanhar todas as freguesias do município (ex: 0105%)
        filtro_sql = f"{config.COLUNA_DICO} LIKE '{config.MUNICIPIO_ALVO}%'"
        
        # Carregar o GeoDataFrame
        municipio_gdf = gpd.read_file(config.CAOP_PATH, layer=config.TABELA_CAOP, where=filtro_sql)

        if municipio_gdf.empty:
            logger.warning(f"Código {config.MUNICIPIO_ALVO} não encontrado na CAOP.")
            return

        logger.info(f"Geometrias encontradas: {len(municipio_gdf)} polígonos.")

        # mask para recorte
        temp_geometry_3763 = municipio_gdf.to_crs("EPSG:3763").union_all().buffer(50)

        # converter de volta para WGS84 (EPSG:4326) usando um GeoSeries temporário
        municipio_mask = gpd.GeoSeries([temp_geometry_3763], crs="EPSG:3763").to_crs("EPSG:4326").iloc[0]
        
        logger.info("Máscara de recorte (WGS84) gerada com sucesso.")

        # 4. Processar cada camada do OSM
        for layer_name, tags in config.OSM_FILTERS.items():
            logger.info(f"A descarregar do OSM: {layer_name}...")
            
            try:
                gdf_raw = ox.features_from_polygon(municipio_mask, tags)

                if not gdf_raw.empty:
                    # A. Clip pela geometria do município
                    gdf_layer = gpd.clip(gdf_raw, municipio_mask)
                    rename_dict = {'element_type': 'element', 'osmid': 'id'}
                    gdf_layer = gdf_layer.rename(columns={k: v for k, v in rename_dict.items() if k in gdf_layer.columns})

                    # B. Filtro POIs
                    if layer_name == "pois":
                        cols_poi = [c for c in ['name', 'brand', 'operator'] if c in gdf_layer.columns]
                        if cols_poi:
                            gdf_layer = gdf_layer.dropna(subset=cols_poi, how='all')

                    if gdf_layer.empty: continue

                    # C. Seleção de colunas e limpeza de tipos
                    col_mapping = getattr(config, 'COLUNAS_POR_CAMADA', getattr(config, 'LAYER_COLUMNS', {}))
                    colunas_alvo = col_mapping.get(layer_name, ['geometry'])
                    colunas_presentes = [c for c in colunas_alvo if c in gdf_layer.columns]
                    gdf_layer = gdf_layer[colunas_presentes].copy()

                    for col in gdf_layer.columns:
                        if gdf_layer[col].apply(lambda x: isinstance(x, (list, dict))).any():
                            gdf_layer[col] = gdf_layer[col].astype(str)

                    # D. DESTINO 1: DISCO
                    if database_config.SAVE_LOCAL_FILE:
                        os.makedirs(os.path.dirname(config.OUTPUT_GPKG), exist_ok=True)
                        gdf_layer.to_file(config.OUTPUT_GPKG, layer=layer_name, driver="GPKG", engine="pyogrio")
                        logger.info(f"Camada '{layer_name}' guardada no disco.")

                    # E. DESTINO 2: POSTGRESQL
                    if database_config.UPLOAD_TO_POSTGRES and engine:
                        gdf_layer.to_postgis(
                            name=layer_name,
                            con=engine,
                            schema=database_config.SCHEMA_NAME,
                            if_exists='replace',
                            index=False
                        )
                        logger.info(f"Camada '{layer_name}' enviada para o PostgreSQL.")

                else:
                    logger.info(f"Camada '{layer_name}' sem dados no OSM.")

            except Exception as e:
                logger.error(f"Erro na camada {layer_name}: {e}")

        logger.info(f"--- Processo concluído em {datetime.now() - start_time} ---")

    except Exception as e:
        logger.critical(f"Falha crítica: {e}", exc_info=True)

if __name__ == "__main__":
    export_to_process()