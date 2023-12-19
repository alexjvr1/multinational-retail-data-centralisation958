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
		return products_df_kg
	
	def clean_products_data(self, products_df_kg):
		df = products_df_kg
		#drop column that repeats index
		df.pop("Unnamed: 0")
		#Remove nonsense entries in product_price
		df = df[df["product_price"].str.contains(r"^£", na=False)]
		#Remove nonsense entries in category
		df = df[df["category"].str.contains("toys-and-games|sports-and-leisure|pets|homeware|health-and-beauty|food-and-drink|diy", na=False)]
		#Change date added to datetime
		df["date_added"]=pd.to_datetime(df["date_added"], format='%Y-%m-%d', errors='coerce')
		#Remove all missing data
		df_cleaned = df.dropna()
		return df_cleaned