import requests
import csv
import psycopg2
import pandas as pd
import json 
import numpy as np
import psycopg2.extras as extras

#URL= 'https://valencia.opendatasoft.com/api/records/1.0/search/?dataset=hospitales&q=&facet=nombre&facet=financiaci&facet=tipo&facet=fecha&facet=barrio'
  
#Obtenemos el paquete/caja que nos viene de ahi con el get'

URL= 'https://valencia.opendatasoft.com/explore/dataset/hospitales/download/?format=json&timezone=Europe/Madrid&lang=es'
#URL2 = 'https://valencia.opendatasoft.com/explore/dataset/barris-barrios/download/?format=json&timezone=Europe/Berlin&lang=es'
df = pd.read_json(URL)
#df2 = pd.read_json(URL2)
#Obtenemos el paquete/caja que nos viene de ahi con el get'
print(df)
respuesta = requests.get(url=URL)
#respuesta2 = requests.get(url=URL2)

datos=respuesta.json()
#datos2=respuesta2.json()

df = pd.json_normalize(datos)
#df2 = pd.json_normalize(datos2)
df.to_numpy().tolist()
#df2.to_numpy().tolist()

#CONEXION A POSTGREESQL

connection = psycopg2.connect(user="postgres", password="Welcome01",host="postgres", port="5432", database="postgres")
cursor = connection.cursor()

cursor.execute(
  """
    CREATE TABLE IF NOT EXISTS hospitales(
    nombre varchar(50),
    coddistrit varchar(50),
    geo_point_2d varchar(100));
    
  """
)

'''cursor.execute(
  """
    CREATE TABLE IF NOT EXISTS barrios(
    coddistrit varchar(50),
    geo_shape varchar(2000));
    
  """
)'''

for i in range(len(df)):  
 
  postgres_insert_query = """ INSERT INTO hospitales (nombre,coddistrit,geo_point_2d) VALUES (%s,%s,%s)"""
  record_to_insert = (df['fields.barrio'][i],df['fields.coddistrit'][i],df['fields.geo_point_2d'][i])
  cursor.execute(postgres_insert_query, record_to_insert)

'''for i in range(len(df2)):  
 
  postgres_insert_query = """ INSERT INTO barrios (coddistrit,geo_shape) VALUES (%s,%s)"""
  record_to_insert = (df['fields.coddistrit'][i],df['fields.geo_shape'][i])
  cursor.execute(postgres_insert_query, record_to_insert)'''

connection.commit()