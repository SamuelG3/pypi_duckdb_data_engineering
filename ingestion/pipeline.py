from ingestion.bigquery import (
    get_bigquery_client, 
    get_bigquery_result, 
    build_pypi_query
)
import duckdb
import fire
from ingestion.models import PypiJobParameters

def main(params: PypiJobParameters):
    # Carrega os dados do BigQuery
    df = get_bigquery_result(
        query_str=build_pypi_query(params), 
        bigquery_client=get_bigquery_client(project_name=params.gcp_project)
        )
    
    conn = duckdb.connect()
    conn.sql("COPY {SELECT * FROM df} TO 'duckdb.csv' (FORMAT csv, HEADER )")

if __name__ == "__main__":
    # Fire detecta 
    fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))