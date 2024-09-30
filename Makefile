include .env
export

pypi-ingest:
	poetry run py -m ingestion.pipeline \
		--start_date $$START_DATE \ 
		--end_date $$END_DATE \ 
		--pypi_project $$PYPI_PROJECT \ 
		--table_name $$TABLE_NAME \ 
		--gcp_project $$GCP_PROJECT \ 
		--timestamp_column $$TIMESTAMP_COLUMN \ 
		--destination $$DESTINATION \
		--s3_path $$S3_PATH \ 
		--aws_profile $$AWS_PROFILE \ 

.PHONY: pypi-ingest format

format: 
	ruff format .

test:
	pytest tests