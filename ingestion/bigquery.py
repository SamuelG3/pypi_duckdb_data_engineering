import os
import time

import pyarrow as pa
from loguru import logger

from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from ingestion.models import PypiJobParameters

PYPI_PUBLIC_DATASET = "bigquery-public-data.pypi.file_downloads"

def get_bigquery_client(project_name: str) -> bigquery.Client:
    """Get Big Query Client"""
    try:
        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        if service_account_path:
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path
            )
            bigquery_client = bigquery.Client(
                project=project_name, credentials=credentials
            )
            return bigquery_client
        raise EnvironmentError(
            "Não há credenciais válidas encontradas para a autenticação BigQuery."
        )

    except DefaultCredentialsError as creds_error:
        raise creds_error
    
def get_bigquery_result(
    query_str: str, 
    bigquery_client: bigquery.Client 
) -> pa.Table:

    """Retorna o resultado do Bigquery e produz o resultado das linhas como dicionários"""
    try:
        # Começa a medida para o tempo da query
        start_time = time.time()

        # Roda a query e carrega o resultado diretamente em um dataframe
        logger.info(f"Running query: {query_str}")
        
        # dataframe = bigquery_client.query(query_str).to_dataframe(dtypes=FileDownloads().pandas_dtypes)
        pa_tbl = bigquery_client.query(query_str).to_arrow()
        
        # Logga o tempo tomado para a execução da query e o carregamento dos dados
        elapsed_time = time.time() - start_time

        logger.info(f"Query executado e dados carregados em {elapsed_time:.2f} segundos")
        
        return pa_tbl

    except Exception as e:
        logger.error(f"Erro rodando a query: {e}")
        raise

def build_pypi_query(
    params: PypiJobParameters, 
    pypi_public_dataset: str = PYPI_PUBLIC_DATASET
) -> str:
    
    # Query para o dataset público PyPI do BigQuery
    # Os filtros são importantes já que é um dataset muito grande
    
    return f"""
    SELECT *
    FROM
        `{pypi_public_dataset}`
    WHERE
        project = '{params.pypi_project}'
        AND {params.timestamp_column} >= TIMESTAMP("{params.start_date}")
        AND {params.timestamp_column} < TIMESTAMP("{params.end_date}")
    """