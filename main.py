from csv_process import sqlLoad
from configparser import ConfigParser

# reading the configuration file containing the postgres credentials
config = ConfigParser()
config.read("pg_creds.cfg")

database=config.get("postgres", "DATABASE"),
user=config.get("postgres", "USERNAME"),
password=config.get("postgres", "PASSWORD"),
host=config.get("postgres", "HOST")
port=config.get("postgres", "PORT")

param_dic = {
    "host"      : host,
    "database"  : database[0],
    "user"      : user[0],
    "password"  : password[0],
}


if __name__=='__main__':
    sqlLoad(param_dic)