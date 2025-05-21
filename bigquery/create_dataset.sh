#!/bin/bash

#SetGCP project ID
PROJECT_ID="fleet-parity-458321-e4"
DATASET="order_dataset"
TABLE="order_table"

#create BigQuery dataset
bq --location=US mk --dataset "${PROJECT_ID}:${DATASET}"

#create BigQuery table with order schema
bq mk --table "${PROJECT_ID}:${DATASET}.${TABLE}" order_id:STRING,user_id:STRING,product_id:STRING,category:STRING,price:FLOAT,quantity:INTEGER,total_amount:FLOAT,payment_method:STRING,order_timestamp:TIMESTAMP
