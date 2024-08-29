import unittest
from unittest.mock import patch, MagicMock
from bigquery_helper import BigQueryHelper
from .test_helper import create_mock_bq_helper, create_sample_dataframe, mock_query_job

class TestBigQueryHelper(unittest.TestCase):
    def setUp(self):
        self.bq_helper = create_mock_bq_helper()
        
    def test_execute_query(self):
        mock_rows = [MagicMock(id=1, name='Alice'), MagicMock(id=2, name='Bob')]
        self.bq_helper.client.query.return_value = mock_query_job(mock_rows)
        
        result = self.bq_helper.execute_query("SELECT * FROM test_table")
        
        self.bq_helper.client.query.assert_called_once_with("SELECT * FROM test_table")
        self.assertEqual(len(list(result)), 2)

    def test_bq2df(self):
        mock_rows = [MagicMock(id=1, name='Alice'), MagicMock(id=2, name='Bob')]
        self.bq_helper.client.query.return_value = mock_query_job(mock_rows)
        
        df = self.bq_helper.bq2df("SELECT * FROM test_table")
        
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), ['id', 'name'])

    def test_df2bq(self):
        df = create_sample_dataframe()
        
        with patch('bigquery_helper.core.pandas_gbq.to_gbq') as mock_to_gbq:
            self.bq_helper.df2bq(df, "test_dataset.test_table")
            
        mock_to_gbq.assert_called_once()

    # Add more tests for other methods

if __name__ == '__main__':
    unittest.main()