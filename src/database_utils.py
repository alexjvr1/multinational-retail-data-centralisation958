#Python script to connect with and upload data to a database

#import all dependencies
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

class DatabaseConnector:
	def __init__(self, data):
			self.data=data

#Read credentials from yaml file and return a dictionary called credentials_for_url
	def read_db_creds():
		db_creds = "db_creds.yaml"
		with open(db_creds, "r") as file:
			credentials = yaml.safe_load(file) 
			#Rename the keys and create a dictionary we can use for create_engine
			keyMapping = {
			"RDS_USER":"username",
			"RDS_PASSWORD":"password",
			"RDS_HOST":"host",
			"RDS_PORT":"port",
			"RDS_DATABASE":"database",
			}
			credentials_for_url = {keyMapping.get(k,k): v for k,v in credentials.items()}
			#Add drivername to dict
			credentials_for_url["drivername"]="postgresql"
		return credentials_for_url

#Function to use the db credentials returned by read_db_creds. Initialise and return an sqlalchemy db engine
	def init_db_creds():
		credentials = dc.read_db_creds()
		url = URL.create(**credentials)
		engine = create_engine(url, echo=True)
		return engine

#Function to upload a pandas dataframe to SQL database
	def upload_to_db(pd_dataframe, sql_table_name):
		pd_df = pd_dataframe
		postgreSQLtable = sql_table_name
		engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/sales_data')
		postgreSQLConnection    = engine.connect()
		try:
			pd_df.to_sql(pd_df, postgreSQLConnection, if_exists="fail");
		except ValueError as vx:
			print(vx)
		except Exception as ex:
			print(ex)
		else:
			print("PostgreSQL Table %s has been created successfully."%postgreSQLtable)