import pandas as pd
from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import DoubleType
from sqlalchemy import create_engine
from kedro.io import DataCatalog


def write_to_db(df: pd.DataFrame) -> None:
    return df

def clean_patients_data(catalog: DataCatalog) -> pd.DataFrame:
    
    credentials = catalog.load("db_credentials")
    
    engine = create_engine(f"postgresql://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}")

    # SQL
    cleaning_sql = """
    SELECT *
    FROM t_patients
    WHERE birthdate IS NOT NULL
      AND birthdate <> '9999-99-99'
      AND deathdate IS NULL
    """

    cleaned_df = pd.read_sql_query(cleaning_sql, con=engine, if_exists='replace')

    return cleaned_df