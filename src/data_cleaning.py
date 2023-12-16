#Script to clean data from CSV, api, and S3 bucket

#import all dependencies
from datetime import datetime
import pandas as pd

#All functions related to data cleaning
class DataCleaning: 
	def __init__(self):
		self = self
		
	def clean_user_data(self, dataframe):
		df = dataframe
		#convert all dates to datetime and to the same format
		df["date_of_birth"]=pd.to_datetime(df["date_of_birth"], format='%Y-%m-%d', errors='coerce')
		df["join_date"]=pd.to_datetime(df["join_date"], format='%Y-%m-%d', errors='coerce')		
		#Clean phone numbers
		#remove all country codes (+44 and +49) and non-numeric symbols
		r1 = '[^0-9]+'
		df["phone_number"]=(df["phone_number"].apply(lambda x: ''.join([i for i in x if str.isnumeric(i)])))
		#shorten to the correct number of digits based on the country code
		#df["phone_number"]=df.apply(df["phone_number"][-9:] if df["country_code"]=="DE" else df["phone_number"][-11:], axis=1)
		#remove rows with NA
		df_cleaned = df.dropna()
		return df_cleaned
