import pyarrow as pa
from datetime import datetime
from pydantic import BaseModel, Field
from typing import (
    Type, 
    List, 
    Optional, 
    Union, 
    Annotated
    )

class PypiJobParameters(BaseModel):
    start_date: str = "2019-04-01"
    end_date: str = "2023-11-30"
    pypi_project: str = "duckdb"
    table_name: str
    gcp_project: str
    timestamp_column: str ="timestamp"
    destination: Annotated[
        Union[List[str], str], Field(default=["local"])
    ]
    s3_path: Optional[str]
    aws_profile: Optional[str]

class ValidationError(Exception):
    """Custom exception for Table validation errors."""
    pass

class TableValidationError(Exception):
    """Custom exception for Table validation errors."""

    pass

def validate_table(table: pa.Table, model: Type[BaseModel]):
    
    """
    Valida cada linha de uma tabela em PyArrow com um modelo do Pydantic.
    Levanta o erro TableValidationError se qualquer linha de validação falhar.

    :param table: PyArrow Table para validação.
    :param model: Pydantic model para validar.
    :raises: TableValidationError
    """

    errors = []

    for i in range(table.num_rows):
        row = {column: table[column][i].as_py() for column in table.column_names}
        try:
            model(**row)
        except ValidationError as e:
            errors.append(f"Linha {i} falhou na validação: {e}")

    if errors:
        error_message = "\n".join(errors)
        raise TableValidationError(
            f"Validação da tabela falhou com os seguintes erros: \n{error_message}"
        )