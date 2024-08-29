# BigQuery Helper

BigQuery Helper is a Python package that simplifies working with Google BigQuery. It provides an easy-to-use interface for common BigQuery operations such as executing queries, inserting rows, and working with DataFrames.

## Installation

You can install BigQuery Helper using pip:

```
pip install bigquery-helper
```

## Usage

To use BigQuery Helper, you'll need a Google Cloud project and credentials. Here's how to get started:

1. Set up a Google Cloud project and enable the BigQuery API.
2. Create a service account and download the JSON key file.
3. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your JSON key file:

   ```
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"
   ```

   Alternatively, you can pass the credentials directly when initializing the BigQueryHelper.

Here's a quick example of how to use BigQuery Helper:

```python
from bigquery_helper import BigQueryHelper

# Initialize BigQueryHelper
bq_helper = BigQueryHelper(project_id="your-project-id", credentials="/path/to/your/keyfile.json")

# Execute a query and get results as a DataFrame
query = "SELECT * FROM `your_dataset.your_table` LIMIT 10"
df = bq_helper.query_to_dataframe(query)
print(df)

# Insert rows into a table
rows_to_insert = [
    {"column1": "value1", "column2": 123},
    {"column1": "value2", "column2": 456}
]
bq_helper.insert_rows("your_dataset.your_table", rows_to_insert)

# Upload a DataFrame to BigQuery
import pandas as pd
df_to_upload = pd.DataFrame({"column1": ["a", "b"], "column2": [1, 2]})
bq_helper.dataframe_to_table(df_to_upload, "your_dataset.new_table", if_exists="replace")

# Update a column in a table
bq_helper.update_column("your_dataset.your_table", "id_column", "status_column", [1, 2, 3])

# Get schema of a table
schema = bq_helper.get_schema("your_dataset.your_table")
print(schema)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.