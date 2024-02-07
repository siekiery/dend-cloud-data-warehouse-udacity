Project: Data Warehouse
Cloud Data Warehouses / Data Engineer Nanodegree
Udacity
Author: Jakub Pitera

This projects concerns building an ETL pipeline for a database hosted on Redshift. It applies AWS, ETL and DWH knowledge learned in a course. 

Pipeline steps: 
0) Connect to Redshift cluster
1) Create 2 staging tables and 5 analytics tables
2) Extract songs and logs data from S3 into staging tables
3) Transform data and load into 5 analytic tables

In order to run the pipeline run all cells in etl.ipynb notebook

Files description:
etl.ipynb - notebook file to run the pipeline
create_tables.py - python script for creating and dropping tables
dwh.cfg - config file for AWS Redshift credentials
etl.py - python script for ETL pipeline instructions
README.md - documentation
sql_queries.py - List of all sql statements