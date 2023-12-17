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

	def clean_card_data(self, dataframe):
		df=dataframe
		#Check that card number is digits only. 
		df["card_number"]=pd.to_numeric(df["card_number"], errors="coerce")
		#Check dates of expiry_date and date_payment_confirmed
		df["expiry_date"]=pd.to_datetime(df["expiry_date"], format='%m/%y', errors="coerce").dt.strftime("%m/%y")
		df["date_payment_confirmed"]=pd.to_datetime(df["date_payment_confirmed"], format='%Y-%m-%d', errors='coerce')
		#remove rows with NA
		df_cleaned = df.dropna()
		return df_cleaned
	
	def clean_store_data(self, dataframe):
		df=dataframe
		#remove empty lat column. A complete latitude column is available
		df = df.drop(["lat"], axis=1)
		#remove nonsense addresses defined as rows with just a single string (no "\n")
		df = df[df["address"].str.contains("\n")]
		#Clean longitude and latitude to include only numbers. 
		df["longitude"] = df["longitude"].apply(pd.to_numeric, errors="coerce")
		df["latitude"] = df["latitude"].apply(pd.to_numeric, errors="coerce")
		#Locality: Remove nonsense submissions
		df = df[df["locality"].str.contains(r"^[a-zA-Z\s-]*$", regex=True)]
		#Staff_numbers: remove rows that aren't integers
		df["staff_numbers"] = df["staff_numbers"].apply(pd.to_numeric, errors="coerce")
		#Ensure all store codes include a hyphen ("-"). Remove rows that do not
		df = df[df["store_code"].str.contains("-")]
		#Store type: remove anything that isn't one of ['Local', 'Super Store', 'Mall Kiosk', 'Outlet']
		df = df[df["store_type"].str.contains("Local|Super Store|Mall Kiosk|Outlet")]
		#country code: remove anything that isn't ['GB', 'DE', 'US']
		df = df[df["country_code"].str.contains("GB|DE|US")]
		#continent: Remove type ("ee" at start of word). And remove anything that isn't ["Europe", "America"]
		df["continent"] = df["continent"].str.replace('^ee', '', regex=True)
		df = df[df["continent"].str.contains("Europe|America")]
		#convert opening_date to datetime
		df["opening_date"]=pd.to_datetime(df["opening_date"], format='%Y-%m-%d', errors='coerce')
		#remove rows with NA
		df_cleaned = df.dropna()
		return df_cleaned