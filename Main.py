# Alkemy Data Analytics Challenge
# Autor: Sergio Miranda
# Fecha: 30-Apr-22

import requests
import datetime as dt
from os import makedirs
import pandas as pd
import numpy as np


############## DESCARGA ARCHIVOS ###########################
# URL´s Archivos (.csv)
# Museos
URL_Museos = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv'
# Salas de Cine
URL_Cines = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv'
# Bibliotecas Populares
URL_Bibliotecas = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'

Fuentes = {'Museos' : URL_Museos , 'Cines' : URL_Cines , 'Bibliotecas' : URL_Bibliotecas}
Dir_Archivo = dict()


def Descarga (Categoria,URL):
    #Ruta Carpeta de Archivo
    Hoy = dt.datetime.now()  #Fecha
    #Mes en texto:
    if Hoy.month == 1 : Mes = "Enero"
    elif Hoy.month == 2 : Mes = "Febrero"
    elif Hoy.month == 3 : Mes = "Marzo"
    elif Hoy.month == 4 : Mes = "Abril"
    elif Hoy.month == 5 : Mes = "Mayo"
    elif Hoy.month == 6 : Mes = "Junio"
    elif Hoy.month == 7 : Mes = "Julio"
    elif Hoy.month == 8 : Mes = "Agosto"
    elif Hoy.month == 9 : Mes = "Septiembre"
    elif Hoy.month == 10 : Mes = "Octubre"
    elif Hoy.month == 11 : Mes = "Noviembre"
    else: Mes == "Diciembre"

    Carpeta = str(Hoy.year) + "-" + Mes  # Carpeta de mes
    Ruta = Categoria + "/" + Carpeta # Ruta completa de directorio
    Archivo = Categoria + "-" + str(Hoy.day) + "-" + str(Hoy.month) + "-" + str(Hoy.year) + ".csv"  # Nombre archivo
    makedirs(Ruta, exist_ok=True)  # Crea el directorio. Ignora mensaje si existiese
    Direccion = Ruta + "/" + Archivo # dirección completa del archivo

    # Descarga Archivos
    with requests.get(URL) as r:
        with open(Direccion, "wb") as f: # nombre de descarga del archivo
            f.write(r.content)

    return Direccion

# Descarga de archivos
for keys in Fuentes:
    Dir_Archivo[keys] = Descarga(keys,Fuentes[keys])  #Baja archivos y retorna dirección completa al diccionario


################# PROCESAMIENTO #################################

#Lectura de Archivos en Pandas
Data_Museos = pd.read_csv(Dir_Archivo['Museos'])
Data_Cines = pd.read_csv(Dir_Archivo['Cines'])
Data_Bibliotecas = pd.read_csv(Dir_Archivo['Bibliotecas'])

# Renombrado Campos para normalizado
Data_Museos.columns = (['cod_localidad', 'id_provincia', 'id_departamento', 'Observaciones','categoria', 'subcategoria', 'provincia', 'localidad', 'nombre',
       'domicilio', 'piso', 'codigo_postal', 'cod_area', 'numero_telefono', 'mail', 'web', 'Latitud', 'Longitud', 'TipoLatitudLongitud', 'Info_adicional',
       'fuente', 'jurisdiccion', 'año_inauguracion', 'actualizacion'])

Data_Cines.columns = (['cod_localidad', 'id_provincia', 'id_departamento', 'Observaciones', 'categoria', 'provincia', 'Departamento', 'localidad', 'nombre',
       'domicilio', 'Piso', 'codigo_postal', 'cod_area', 'numero_telefono', 'mail', 'web', 'Información adicional', 'Latitud', 'Longitud', 'TipoLatitudLongitud',
       'Fuente', 'tipo_gestion', 'Pantallas', 'Butacas', 'espacio_INCAA', 'año_actualizacion'])

Data_Bibliotecas.columns = (['cod_localidad', 'id_provincia', 'id_departamento', 'Observacion', 'categoria', 'Subcategoria', 'provincia', 'Departamento', 'localidad', 'nombre',
       'domicilio', 'Piso', 'codigo_postal', 'Cod_tel', 'numero_telefono', 'mail', 'web', 'Información adicional', 'Latitud', 'Longitud',
        'TipoLatitudLongitud', 'Fuente', 'Tipo_gestion', 'año_inicio', 'Año_actualizacion'])

# Generación Archivo Normalizado
Campos = ['cod_localidad','id_provincia','id_departamento','categoria','provincia','localidad','nombre','domicilio','codigo_postal','numero_telefono','mail','web']

Data_Normalizada = Data_Museos.filter(items = Campos, axis = 1)  # Crea archivo inicial normalizado con datos de Museos
Data_Normalizada = pd.concat([Data_Normalizada, Data_Cines], join = "inner")  # Agrega datos Cines
Data_Normalizada = pd.concat([Data_Normalizada, Data_Bibliotecas], join = "inner")  # Agrega datos Bibliotecas

# Arreglo nombres incorrectos provincias:
Data_Normalizada = Data_Normalizada.replace(to_replace = '^Neuqu.*', value = 'Neuquén',regex = True)  #Arregla Neuquén
Data_Normalizada = Data_Normalizada.replace(to_replace = '^Santa F.*', value = 'Santa Fe',regex = True)  #Arregla Santa Fe
Data_Normalizada = Data_Normalizada.replace(to_replace = '^Tierra del Fueg.*', value = 'Tierra del Fuego, Antártida e Islas del Atlántico Sur',regex = True)  #Arregla Tierra del Fuego

# PROCESADO INFORMACIÓN
# PROCESADO INFORMACIÓN

Registros_Categoria = Data_Normalizada.count()  # Cuenta de registros por categoría

Fuentes = Data_Normalizada.categoria.unique()  # Array de fuentes

Registros_Fuentes = Data_Normalizada.groupby(['categoria']).categoria.count() #Cant. Registros Fuentes


Registros_Categoria = Data_Normalizada.count()  # Cuenta de registros por categoría

Fuentes = Data_Normalizada.categoria.unique()  # Array de fuentes

Registros_Fuentes = Data_Normalizada.groupby(['categoria']).categoria.count() #Cant. Registros Fuentes







#####
Data_Normalizada.groupby(['provincia','categoria']).agg({'categoria':['count']})
