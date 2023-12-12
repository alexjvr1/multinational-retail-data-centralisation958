#Script to clean data from CSV, api, and S3 bucket

#import all dependencies
from datetime import datetime
import pandas as pd

#All functions related to data cleaning
class DataCleaning: 
	def __init__():
		def clean_user_data(dataframe):
			df = dataframe
		#convert all dates to datetime and to the same format
			df["date_of_birth"]=pd.to_datetime(df["date_of_birth"], format='%Y-%m-%d', errors='coerce')
			df["join_date"]=pd.to_datetime(df["join_date"], format='%Y-%m-%d', errors='coerce')		
		#Clean phone numbers
		#remove rows with NA
			df_cleaned = df.dropna()
			return df_cleaned
