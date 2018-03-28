import requests
import datetime
import xml.etree.ElementTree as ET
from dateutil.relativedelta import relativedelta
import pandas as pd 
import pathlib
import os 
from bs4 import BeautifulSoup

ENDPOINT = "https://ssl.orpak.com/api40/TrackTecPublic/PublicService.asmx/ExecuteCommand"

cur_date = datetime.datetime.now().date()
start_date = cur_date -relativedelta(years=1) 

params = """
          <Paramaters>
          <ClientID></ClientID>
          <CommandName>GetEventsHistory</CommandName>
          <ResultType>DEFAULT</ResultType>
          <DeviceIDs>81B16GBD5D00251</DeviceIDs>
          <SourceIDs>9,10,11,12,49,48,52</SourceIDs>
          <StartDate>2018/02/15 00:00:00</StartDate>
          <EndDate>2018/02/16 00:00:00</EndDate>
          <PageIndex>1</PageIndex>
          <PageSize>10000</PageSize>
          </Paramaters>
         """

def querylist_builder():
    ret = [] # make an empty list to start throwing stuff onto
    q_start_date = start_date
    while q_start_date < cur_date:
        query_date = q_start_date.strftime("%Y/%m/%d") + " 00:00:00"
        end_date = (q_start_date + relativedelta(days=1)).strftime("%Y/%m/%d") + " 00:00:00"
        ret.append(params.format(query_date, end_date))
        q_start_date += relativedelta(days=1)
    return ret

def extract():
    """
    Extracts and saves info for all queries in querylist_builder
    to a /tmp folder
    """
    queries = querylist_builder()
    
    pathlib.Path('/tmp/street_data').mkdir(parents=True, exist_ok=True) 
    for i,q in enumerate(queries):
        print("running extract query")
        url = ENDPOINT + "?CommandData=" + q
        print(url)
        r = requests.get(url)
        text_file = open("/tmp/street_data/" + str(i) + ".xml", 'w')
        data = r.text
        print(data)
        text_file.write(data)   
        print("data saved for {}".format(str(i)))
        text_file.close()

def parse():
    """
    extract all the lat longs and elements, return as list 

    """
    values = []
    for file in os.listdir('/tmp/street_data/'): 
        with open('/tmp/street_data/' + file, 'r') as f:
            data = f.readlines()
        data = ''.join(data)
        soup = BeautifulSoup(data)
        tables = soup.findAll('table') 
        for table in tables:
            print(table)
            time = table.find('eventtime')
            lat = table.find('latitude')
            long = table.find('longitude')
            values.append({'lat': lat, 'long': long, 'time': time})
            print(lat,long,time)
    return values

def load(values):
    """
    load into some format for later study
    
    params = a list of values to laod
    """
    import sqlite3
    conn = sqlite3.connect('./example.db')
    df = pd.DataFrame(values)
    df.to_sql('observations', conn)

    


if '__name__' == '__main__':
    if not os.path.exists('/tmp/street_data'): 
        extract()
    values = parse()
    load(values)

