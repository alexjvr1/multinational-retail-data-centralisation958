def read_db_creds():
                import yaml
                db_creds = input("name of file storing data")
                with open(db_creds, "r") as file:
                        credentials = yaml.safe_load(file)
                print(credentials)

read_db_creds()

