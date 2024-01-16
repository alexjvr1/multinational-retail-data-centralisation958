#Python script to extract data from different data sources: 
#CSV, API, and S3 bucket

#import all dependencies
from sqlalchemy import inspect
import pandas as pd
import tabula
from database_utils import DatabaseConnector
import requests
import boto3
import json
import re

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
	def retrieve_pdf_data(self, pdf_path):
		dfs = pd.concat(tabula.read_pdf(pdf_path, lattice=True,pages='all', multiple_tables=True))
		return dfs
	
#Function to return the number of stores that there are data for at an API end point
	def list_number_of_stores(self, number_of_stores_endpoint, api_header_dict):
		url = number_of_stores_endpoint
		header_dict = api_header_dict
		response=requests.get(url, headers=header_dict).json()
		number_of_stores = response["number_stores"]
		return number_of_stores

#Function to return the store data for the stores found in list_number_of_stores
	def retrieve_stores_data(self, retrieve_stores_endpoint, api_header_dict, number_of_stores):
		url=retrieve_stores_endpoint  #url without the store number
		header_dict=api_header_dict  #authentication key for access to the end point
		store_data=[]   #create an empty list where all the store date will be stored
		#loop through the urls to retrieve store data for each store and appends as a json object to the list
		for i in range(0,number_of_stores):  
			store_data.append(requests.get(url=(url+str(i)), headers=header_dict).json())
		#convert to pandas df
		store_data_df = pd.DataFrame.from_records(store_data)
		return store_data_df
	
#Function to retrieve csv data from an S3 bucket on AWS
	def extract_from_s3(self, s3_address):
		df = pd.read_csv(s3_address)
		return df

#Function to extract json data from an S3 bucket on AWS
	def extract_json_from_s3(self, s3_address):
		#extract information from the s3_address variable using ?P to label the captured variables
		match = re.search(r'w*\:\/\/(?P<bucket>\w.*).s3.*\/(?P<object_key>\w.*)', s3_address)
		bucket = match.group('bucket')
		object_key = match.group('object_key')
		#create an instance of the s3 client. Use client
		s3 = boto3.resource('s3')
		content_object = s3.Object(bucket, object_key)
		file_content = content_object.get()['Body'].read().decode('utf-8')
		json_content = json.loads(file_content)
		df = pd.DataFrame(json_content)
		return df
		