import unittest
from unittest.mock import patch, MagicMock
from google.cloud import bigquery
from google.oauth2 import service_account
from bigquery_helper import BigQueryHelper

class TestBigQueryHelper(unittest.TestCase):

    def setUp(self):
        self.project_id = "test-project"
        self.credentials = MagicMock(spec=service_account.Credentials)

    @patch('bigquery_helper.helper.bigquery.Client')
    def test_init(self, mock_client):
        helper = BigQueryHelper(self.project_id, self.credentials)
        mock_client.assert_called_once_with(project=self.project_id, credentials=self.credentials)
        self.assertIsInstance(helper.client, bigquery.Client)

    @patch('bigquery_helper.helper.bigquery.Client')
    def test_execute_query(self, mock_client):
        helper = BigQueryHelper(self.project_id, self.credentials)
        mock_query_job = MagicMock()
        mock_client.return_value.query.return_value = mock_query_job
        
        query = "SELECT * FROM table"
        result = helper.execute_query(query)
        
        mock_client.return_value.query.assert_called_once_with(query)
        mock_query_job.result.assert_called_once()
        self.assertEqual(result, mock_query_job.result.return_value)

    @patch('bigquery_helper.helper.bigquery.Client')
    def test_insert_rows(self, mock_client):
        helper = BigQueryHelper(self.project_id, self.credentials)
        mock_client.return_value.insert_rows_json.return_value = []
        
        table_id = "project.dataset.table"
        rows = [{"col1": "val1"}, {"col1": "val2"}]
        helper.insert_rows(table_id, rows)
        
        mock_client.return_value.insert_rows_json.assert_called_once_with(table_id, rows)

    # Add more tests for other methods...

if __name__ == '__main__':
    unittest.main()