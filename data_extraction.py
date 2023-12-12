#Python script to extract data from different data sources: 
#CSV, API, and S3 bucket

#import all dependencies
from sqlalchemy import inspect
import pandas as pd
import tabula

#All functions related to data extraction
class DataExtractor:
	def __init__(self, DatabaseConnector):
#create an instance of the DatabaseConnector class (from database_utils)
		dc = DatabaseConnector()
#function to list all the tables in the database
	def list_db_tables():
		engine = dc.init_db_creds()
		#create an inspector
		inspector = inspect(engine)
		table_names = []
		#Add table names to a list and print
		table_names += inspector.get_table_names()
		return table_names
#Function to extract a db table to a pandas df
	def read_rds_table(DatabaseConnector, table_name):
		DatabaseConnector = dc
		engine = dc.init_db_creds()
		table_name = table_name
		table_df = pd.read_sql_table(table_name, engine)
		table_df.head()
		return table_df
#Function to extract pdf data to a pandas data frame
#The function takes as input a link to a pdf
	def retrieve_pdf_data(pdf_path):
		pdf_path = pdf_path
		dfs = pd.concat(tabula.read_pdf(pdf_path, lattice=True,pages='all', multiple_tables=True))
		return dfs