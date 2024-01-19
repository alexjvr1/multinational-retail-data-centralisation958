#Script to clean data from CSV, api, and S3 bucket

#import all dependencies
from datetime import datetime
import pandas as pd
import numpy as np

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
		df["country_code"]=df["country_code"].replace("GGB", "GB")
		#Remove all nonsense uuid
		df = df[df["user_uuid"].str.contains("-")]
		#remove rows with NA
		#df_cleaned = df.dropna()  #step removed because it removes too many useful entries
		df_cleaned= df
		return df_cleaned

	def clean_card_data(self, dataframe):
		df=dataframe
		#Check that card number is digits only. 
		#df["card_number"]=pd.to_numeric(df["card_number"], errors="coerce").astype("Int64") #Some of the correct card numbers contain special characters which just need to be stripped. This removes them from the dataset
		df["card_number"] = df["card_number"].astype(str) #it's easier to work with the str dtype than object dtype
		df["card_number"]=df["card_number"].str.replace("?", "") #Removes any "?"
		df["card_number"] = np.where(df["card_number"].str.contains(r'\w*[A-Z]\w*', regex=True), np.nan, df["card_number"]) #Replaces nonsense entries that are just capital letters
		#Check dates of expiry_date and date_payment_confirmed
		df["expiry_date"]=pd.to_datetime(df["expiry_date"], format='%m/%y', errors="coerce").dt.strftime("%m/%y")
		df["date_payment_confirmed"]=pd.to_datetime(df["date_payment_confirmed"], format='%Y-%m-%d', errors='coerce')
		#remove rows with NA
		#df_cleaned = df.dropna() #removed step because it removes too many entries
		df_cleaned = df
		return df_cleaned
	
	def clean_store_data(self, dataframe):
		df=dataframe
		#remove empty lat column. A complete latitude column is available
		#df = df.drop(["lat"], axis=1)  #removed this drop because the course requires us to merge the lat columns in SQL
		#remove nonsense addresses defined as rows with just a single string (no "\n")
		#df = df[df["address"].str.contains("\n", regex=True)]  #removes the web orders
		df["address"] = df["address"].astype(str) #it's easier to work with str data
		df.address = np.where(~df.address.str.contains("\n", regex=True), np.nan, df.address)
		#Clean longitude and latitude to include only numbers. 
		df["longitude"] = df["longitude"].astype(str) #it's easier to work 
		df["longitude"] = np.where(~df.longitude.str.contains(r"[0-9]\.", regex=True), np.nan, df.longitude)
		df["latitude"] = df["latitude"].astype(str) #it's easier to work 
		df["latitude"] = np.where(~df.latitude.str.contains(r"[0-9]\.", regex=True), np.nan, df.latitude)
		df["lat"] = df["lat"].astype(str) #it's easier to work 
		df["lat"] = np.where(~df.lat.str.contains(r"[0-9]\.", regex=True), np.nan, df.lat) #All the entries are nonsense or missing
		#Locality: Remove nonsense submissions
		#df = df[df["locality"].str.contains(r"^[a-zA-Z\s-]*$", regex=True)]  #This removes the web orders
		df["locality"] = df["locality"].astype(str)
		df.locality = np.where(~df.locality.str.contains(r"^[a-zA-Z\s-]*$", regex=True), np.nan, df.locality)
		#Staff_numbers: Clean nonsense entries. And strip letters inserted into sensible staff number entries
		df["staff_numbers"] = df["staff_numbers"].astype(str) #it's easier to work 
		df["staff_numbers"] = np.where(df.staff_numbers.str.contains(r'[A-Z]{2}', regex=True), np.nan, df.staff_numbers)
		df["staff_numbers"] = df["staff_numbers"].astype(str) #it's easier to work 
		df["staff_numbers"] = df["staff_numbers"] = df.staff_numbers.str.replace('[A-Za-z]', '', regex=True)
		#Ensure all store codes include a hyphen ("-"). Remove rows that do not
		df["store_code"] = np.where(df["store_code"].str.contains("-"), df["store_code"], np.nan)
		#Store type: remove anything that isn't one of ['Local', 'Super Store', 'Mall Kiosk', 'Outlet']
		df["store_type"] = np.where(df["store_type"].str.contains("Local|Super Store|Mall Kiosk|Outlet"), df["store_type"], np.nan)
		#country code: remove anything that isn't ['GB', 'DE', 'US']
		df["country_code"] = np.where(df["country_code"].str.contains("GB|DE|US"),df["country_code"], np.nan)
		#continent: Remove type ("ee" at start of word). And remove anything that isn't ["Europe", "America"]
		df["continent"] = df["continent"].str.replace('^ee', '', regex=True)
		df["continent"] = np.where(df["continent"].str.contains("Europe|America"), df["continent"], np.nan)
		#convert opening_date to datetime
		df["opening_date"]=pd.to_datetime(df["opening_date"], format='%Y-%m-%d', errors='coerce')
		#remove rows where store_code is NA
		df_cleaned = df.dropna(subset=['store_code'])
		#df_cleaned = df.dropna() #removed this so that we can keep the empty lat column
		return df_cleaned
	
#Function takes a products pd.df and converts all weights to kg. It returns a pd.df
	def convert_product_weights(self, products_df):
		#split weights column to have units in a new column
		pattern = '(\\D*$)'  #find any letters at the end of the string
		products_df["units"] = products_df["weight"].str.extract(pattern, expand=False)
		#Replace weight column by excluding units
		products_df["weight"] = products_df["weight"].str.replace(pattern, '', regex=True)
		#For all cells containing x (multiplication), calculate the final weight
			#First split all cells by x and create a new column. Replace all empty cells with 1 for multiplication
		temp = products_df["weight"].str.split("x", expand=True).replace(np.nan, 1)
			#Remove all non numeric rows
		temp[0] = temp[0].apply(pd.to_numeric, errors="coerce")
		temp[1] = temp[1].apply(pd.to_numeric, errors="coerce")
			#Create a third new column with the final product
		products_df["final_weight"] = (pd.to_numeric(temp[0]))*(pd.to_numeric(temp[1]))
		#change all ml to g. 
		products_df["units"] = products_df["units"].str.replace("ml", "g")
		#change all l to kg
		products_df["units"] = products_df["units"].str.replace("l", "kg")
		#convert all g to kg 
		products_df.loc[products_df["units"]=="g", "final_weight"] = products_df["final_weight"]/1000
		#change all numbers to float with 2 decimal places.
		products_df["final_weight"] = products_df["final_weight"].astype(float)
		#Remove "weight" column and rename "final_weight" column to "weight"
		products_df.pop("weight")
		products_df.rename(columns={"final_weight":"weight"})
		products_df_kg = products_df.dropna()
		return products_df
	
	def clean_products_data(self, product_df_kg):
		df = product_df_kg
		#drop column that repeats index
		df.pop("Unnamed: 0")
		#Remove nonsense entries in product_price
		df = df[df["product_price"].str.contains(r"^£", na=False)]
		#Remove nonsense entries in category
		df = df[df["category"].str.contains("toys-and-games|sports-and-leisure|pets|homeware|health-and-beauty|food-and-drink|diy", na=False)]
		#Change date added to datetime
		df["date_added"]=pd.to_datetime(df["date_added"], format='%Y-%m-%d', errors='coerce')
		#Remove all missing data
		#df_cleaned = df.dropna() #Remove command because it removes too many useful entries
		df_cleaned= df
		return df_cleaned
	
	def clean_orders_table(self, orders_table):
		df = orders_table
		#remove three columns
		df = df.drop(["level_0","first_name", "last_name", "1"], axis=1)
		#Remove nonsense entries in each column to match other data_frames
		#df = df[~df["date_uuid"].str.contains(r'\w*[A-Z]\w*', regex=True)] #changed this to assign to a specific column rather than subset the df 
		df["date_uuid"] = np.where(df["date_uuid"].str.contains(r'\w*[A-Z]\w*', regex=True), np.nan, df["date_uuid"]) #match dim_date_times
		#df["card_number"]=pd.to_numeric(df["card_number"], errors="coerce").astype("Int64") #match dim_card_details. Removed because some card numbers contain special characters and just need to be stripped not replaced with Nan
		df["card_number"] = df["card_number"].astype(str) #it's easier to work with the str dtype than object dtype
		df["card_number"]=df["card_number"].str.replace("?", "") #match dim_card_details
		df["card_number"] = np.where(df["card_number"].str.contains(r'\w*[A-Z]\w*', regex=True), np.nan, df["card_number"])
		df = df.dropna(subset="card_number", axis=0) #remove any missing data from this column that will be used as a primary key
		#df["product_code"] = #match dim_products. Nothing done to product_code
		df["store_code"] = np.where(df["store_code"].str.contains("-"), df["store_code"], np.nan) #match dim_store_details
		df["user_uuid"] = np.where(df["user_uuid"].str.contains("-"), df["user_uuid"], np.nan) #match dim_users_table
		#df_cleaned = df.dropna() #remove step as it excludes too many entries with useful information
		df_cleaned = df
		return df_cleaned
	
	def clean_date_details(self, date_details):
		df = date_details
		#day, month and year contains only digits
		df["day"]=pd.to_numeric(df["day"], errors="coerce").astype('Int64')
		df["month"]=pd.to_numeric(df["month"], errors="coerce").astype('Int64')
		df["year"]=pd.to_numeric(df["year"], errors="coerce").astype('Int64')
		#Time-period: remove anything that isn't one of ['Morning', 'Midday', 'Late_Hours', 'Evening']
		df["time_period"] = np.where(df["time_period"].str.contains("Morning|Midday|Evening|Late_Hours"), df["time_period"], np.nan)
		#uuid: keep only lines that do not contain capital letters
		df["date_uuid"] = np.where(df["date_uuid"].str.contains(r'\w*[A-Z]\w*', regex=True), np.nan, df["date_uuid"])
		#df_cleaned = df.dropna() #remove line because it removes too many useful entries
		df_cleaned= df
		return df_cleaned