# multinational-retail-data-centralisation958

## Project Description

Aim: Set up a single SQL database of all sales data for Multinational Retail Co. 

This is a pipeline to collate and clean data pertaining to the running of a company. Data are extracted from different sources (AWS database, AWS s3 bucket, and API). The data are uploaded to an SQL database. The code for this part of the project is all written in python. 

Next, the database is curated and a star-based database schema is created. Finally, the database is queried to determine various interesting things about the running of the business. The code for this part of the project is written in postgresql. 


## Quick start

Clone the repository: 
```
git clone https://github.com/alexjvr1/multinational-retail-data-centralisation958/tree/main
```

Data extraction, cleaning, and uploading to SQL requirements: 
```
Python 3.9.5
 
```

The pipeline has been tested in Python 3.9.5 


## Usage instructions

The src/ directory contains all the Python scripts. The step by step pipeline is documented in __main__.py. Run this script to duplicate the steps used in this project. 

database_utils.py: modules to connect with and upload data to a database 

data_cleaning.py: modules to clean each dataset

data_extraction.py: modules to download data from the various sources. 



## Licence information

GNU GPLv3
