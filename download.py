from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
from lxml import html
import requests
import json
import csv
import unidecode
from pymongo import MongoClient
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import gridfs
from gridfs import GridFSBucket

#uri = "mongodb://ArmiraDATA:ArmiraDATA1@ds145895.mlab.com:45895/armira"
#client = MongoClient(uri)
#db = client['armira']
client = MongoClient('localhost', 27017)
db = client['Projet']
fs = GridFSBucket(db )
file = open('myfile','wb')
fs.download_to_stream_by_name("1219032", file)