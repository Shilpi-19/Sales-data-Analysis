# Data Extraction [ extract data from csv file]

import pandas as pd
sales_data=pd.read_csv('company-sales.csv')

# Data Transformation [ this part uses SQL ]

CREATE DATABASE sales_db;
CREATE TABLE sales (
    order_id INTEGER PRIMARY KEY,
    product_id INTEGER,
    order_date DATE,
    quantity INTEGER,
    price DECIMAL(10, 2)
);
.mode csv
.import sales_data.csv sales
-- Aggregate sales by product and month
SELECT
    strftime('%Y-%m', order_date) AS month,
    product_id,
    SUM(quantity) AS total_quantity,
    SUM(price * quantity) AS total_revenue
FROM sales
GROUP BY month, product_id;

#Data Load [Load the transformed data into a warehouse using Google BigQuery ]

from google.cloud import bigquery
client = bigquery.Client()
schema = [
    bigquery.SchemaField('month', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('product_id', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('total_quantity', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('total_revenue', 'FLOAT', mode='REQUIRED'),
]
table_id = 'your-project-id.dataset.sales_summary'
job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_TRUNCATE')
job = client.load_table_from_dataframe(sales_summary_df, table_id, job_config=job_config)
job.result()
