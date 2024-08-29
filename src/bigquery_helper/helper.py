import logging
from typing import List, Dict, Any, Optional, Union

import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import GoogleCloudError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQueryHelper:
    """
    Helper class for BigQuery operations.
    """
    def __init__(self, project_id: str, credentials: Union[str, dict, service_account.Credentials]):
        """
        Initialize BigQueryHelper with project ID and credentials.

        :param project_id: The Google Cloud project ID.
        :param credentials: Either a path to a service account JSON file,
                            a dictionary containing service account info,
                            or a Credentials object.
        """
        self.project_id = project_id
        self.credentials = self._get_credentials(credentials)
        self.client = bigquery.Client(project=self.project_id, credentials=self.credentials)

    def set_project_id(self, project_id: str):
        """
        Set the project ID.

        :param project_id: The Google Cloud project ID.
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=self.project_id, credentials=self.credentials)

    @staticmethod
    def _get_credentials(credentials):
        if isinstance(credentials, str):
            return service_account.Credentials.from_service_account_file(
                credentials,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
        elif isinstance(credentials, dict):
            return service_account.Credentials.from_service_account_info(
                credentials,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
        elif isinstance(credentials, service_account.Credentials):
            return credentials
        else:
            raise ValueError("Invalid credentials type. Must be a file path, dict, or Credentials object.")

    def execute_query(self, query: str) -> Optional[bigquery.table.RowIterator]:
        """
        Execute a query on the BigQuery project.

        :param query: SQL query to execute.
        :return: RowIterator containing query results, or None if the query fails.
        """
        try:
            query_job = self.client.query(query)
            return query_job.result()
        except GoogleCloudError as e:
            logger.error(f"Failed to execute query: {e}")
            return None

    def insert_rows(self, table_id: str, rows_to_insert: List[Dict[str, Any]]) -> None:
        """
        Insert rows into a BigQuery table.

        :param table_id: The ID of the table to insert rows into.
        :param rows_to_insert: List of dictionaries representing rows to insert.
        :raises GoogleCloudError: If the insertion fails.
        """
        try:
            errors = self.client.insert_rows_json(table_id, rows_to_insert)
            if not errors:
                logger.info(f"{table_id}: {len(rows_to_insert)} new rows have been added.")
            else:
                logger.error(f"Encountered errors while inserting rows: {errors}")
                raise GoogleCloudError(f"Failed to insert rows: {errors}")
        except GoogleCloudError as e:
            logger.error(f"Failed to insert rows: {e}")
            raise

    def query_to_dataframe(self, query: str) -> Optional[pd.DataFrame]:
        """
        Execute a query and return the results as a pandas DataFrame.

        :param query: SQL query to execute.
        :return: DataFrame containing query results, or None if the query fails.
        """
        result = self.execute_query(query)
        return result.to_dataframe() if result else None

    def dataframe_to_table(self, df: pd.DataFrame, table_id: str, schema: Optional[List[Dict[str, str]]] = None, if_exists: str = 'append') -> None:
        """
        Upload a DataFrame to a BigQuery table.

        :param df: DataFrame to upload.
        :param table_id: Full table ID in the format `project_id.dataset_id.table_id`.
        :param schema: Optional schema for the table.
        :param if_exists: Behavior when the table exists, default is 'append'.
        """
        if schema:
            df = self._prepare_dataframe(df, schema)

        pandas_gbq.to_gbq(
            df,
            table_id,
            project_id=self.project_id,
            credentials=self.credentials,
            if_exists=if_exists,
            table_schema=schema,
        )
        logger.info(f"Successfully uploaded {len(df)} rows to {table_id}")

    def _prepare_dataframe(self, df: pd.DataFrame, schema: List[Dict[str, str]]) -> pd.DataFrame:
        """
        Prepare DataFrame according to the provided schema.

        :param df: Input DataFrame.
        :param schema: List of dictionaries representing the schema.
        :return: Prepared DataFrame.
        """
        schema_dict = {field['name']: field['type'] for field in schema}
        columns_to_keep = [col for col in df.columns if col in schema_dict]
        df = df[columns_to_keep]

        for column_name, column_type in schema_dict.items():
            if column_name in df.columns:
                df[column_name] = self._cast_column(df[column_name], column_type)

        return df

    @staticmethod
    def _cast_column(column: pd.Series, column_type: str) -> pd.Series:
        """
        Cast a column to the specified BigQuery data type.

        :param column: Input column.
        :param column_type: BigQuery data type.
        :return: Casted column.
        """
        if column_type == 'INTEGER':
            return pd.to_numeric(column, errors='coerce').astype('Int64')
        elif column_type == 'FLOAT':
            return pd.to_numeric(column, errors='coerce').astype(float)
        elif column_type == 'STRING':
            return column.astype(str)
        elif column_type == 'BOOLEAN':
            return column.astype(bool)
        elif column_type == 'TIMESTAMP':
            return pd.to_datetime(column, errors='coerce')
        else:
            return column

    def update_column(self, table_id: str, column_to_check: str, new_value_column: str, values_to_match: List[Union[str, int]]) -> None:
        """
        Update specific columns in a BigQuery table.

        :param table_id: Full table ID in the format `project_id.dataset_id.table_id`.
        :param column_to_check: Column to check for matching values.
        :param new_value_column: Column to update with new values.
        :param values_to_match: List of values to match in the column_to_check.
        """
        values_str = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in values_to_match])
        dml_statement = f"""
        UPDATE `{table_id}`
        SET `{new_value_column}` = @new_value
        WHERE `{column_to_check}` IN ({values_str})
        """
        query_job = self.client.query(dml_statement)
        query_job.result()
        logger.info(f"Updated {query_job.num_dml_affected_rows} rows in {table_id}")

    def run_query(self, query: str) -> None:
        """
        Run a custom query.

        :param query: The query to run.
        """
        query_job = self.client.query(query)
        query_job.result()
        logger.info(f"Query affected {query_job.num_dml_affected_rows} rows.")

    def get_schema(self, table_ref: str) -> List[Dict[str, Any]]:
        """
        Get the schema of a BigQuery table.

        :param table_ref: Full table reference in the format `project_id.dataset_id.table_id`.
        :return: List of dictionaries representing the table schema.
        """
        try:
            table = self.client.get_table(table_ref)
            return [{"name": field.name, "type": field.field_type, "mode": field.mode} for field in table.schema]
        except GoogleCloudError as e:
            logger.error(f"Failed to get schema for {table_ref}: {e}")
            return []