import configparser

from pymongo import MongoClient

# read config.ini
config = configparser.ConfigParser()
config.read('.\\src\\config.ini')

# connect to mongodb
url = config['mongodb']['url']
client = MongoClient(url)
