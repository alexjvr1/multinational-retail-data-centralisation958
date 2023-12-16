from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

#Initiate instances of each Class
###credentials = path to yaml file that contains the database credentials
dc = DatabaseConnector(credentials="../db_creds.yaml")
de = DataExtractor()
dcln = DataCleaning()

#Step1: read the database credentials and return a db engine that can be initialised later
engine=dc.init_db_creds()
print(engine)

#Step2: list all the tables in the RDS database
table_names = de.list_db_tables(engine=engine)
print(table_names)

#Step3: extract the user table to a pandas dataframe
users_table = de.read_rds_table(engine=engine, table_name="legacy_users")
users_table.info()

#Step4: clean the user table
user_table_cleaned = dcln.clean_user_data(users_table)

#Step5: Upload to SQL database
#def execute_main():




#if __name__ == "__main__":  # Condition to ensure module is executed not imported. 
#    execute_main()