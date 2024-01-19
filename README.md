# multinational-retail-data-centralisation958

## Project Description

Aim: Set up a single SQL database of all sales data for Multinational Retail Co. 

This is a pipeline to collate and clean data pertaining to the running of a company from different sources (AWS database, AWS s3 bucket, and API). The data are then stored in an SQL database. 

## Quick start

The pipeline is written in Python 3.9.5 


## Usage instructions

The src/ directory contains all the Python scripts. The step by step pipeline is documented in __main__.py. Run this script to duplicate the steps used in this project. 

database_utils.py: modules to connect with and upload data to a database 

data_cleaning.py: modules to clean each dataset

data_extraction.py: modules to download data from the various sources. 



## Licence information

GNU GPLv3
