# Alkemy Data Analytics Challenge
# Autor: Sergio Miranda
# Fecha: 30-Apr-22
#SQL Deploy script for Database creation

import psycopg2
from db_config import config

# Lectura de parametros y conección a la base de datos
params = config()
conn = psycopg2.connect(**params)
cur = conn.cursor()


cur.execute('DROP TABLE IF EXISTS Registros')
cur.execute('DROP TABLE IF EXISTS Cines')

cur.execute('''CREATE TABLE registros ( 
            Registro CHAR(30),
            Tipo CHAR(30), 
            Total INT, 
            Fecha CHAR (10) 
            )''')
print("TABLA Registros CREADA EN FORMA CORRECTA")

cur.execute('''CREATE TABLE cines ( 
            Provincia CHAR(30), 
            Pantallas INT, 
            Butacas INT, 
            Espacios_INCAA INT, 
            Fecha CHAR (10) 
            )''')
print("TABLA Cines CREADA EN FORMA CORRECTA")

conn.commit()
conn.close()

