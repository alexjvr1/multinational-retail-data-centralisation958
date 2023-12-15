#Python script to extract data from different data sources: 
#CSV, API, and S3 bucket

#import all dependencies
from sqlalchemy import inspect
import pandas as pd
import tabula
from database_utils import DatabaseConnector

#All functions related to data extraction
class DataExtractor:
	def __init__(self):
		self = self
		
#function to list all the tables in the database. engine = dc.init_db_creds(). Scripts linked together in main.py
	def list_db_tables(self, engine):
		#create an inspector
		inspector = inspect(engine)
		table_names = []
		#Add table names to a list and print
		table_names += inspector.get_table_names()
		return table_names
		

#Function to extract a db table to a pandas df
	def read_rds_table(self, table_name, engine):
		con=engine.connect()
		table_df = pd.read_sql_table(table_name, con=con)
		return table_df

#Function to extract pdf data to a pandas data frame
#The function takes as input a link to a pdf
	def retrieve_pdf_data(pdf_path):
		pdf_path = pdf_path
		dfs = pd.concat(tabula.read_pdf(pdf_path, lattice=True,pages='all', multiple_tables=True))
		return dfs