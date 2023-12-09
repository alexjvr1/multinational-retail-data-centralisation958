#Python script to connect with and upload data to a database
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

class DatabaseConnector: 
	#def __init__(self)
#Function to read database credentials and return a dictionary of the inputs.
#The function needs an input yaml called db_creds.yaml in the local directory
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
			credentials_for_url = {keyMapping.get(k,k): v for k,v in 
credentials.items()}
			#Add drivername to dict
			credentials_for_url["drivername"]="postgresql"
		return credentials_for_url

#Function to use the db credentials returned by read_db_creds
#and initialise and return an sqlalchemy db engine
        def init_db_creds():
		credentials = read_db_creds()
		url = URL.create(**credentials)
		engine = create_engine(url, echo=True)
		return engine
