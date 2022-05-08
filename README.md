# Data-Analytics-Challenge
Alkemy Data Analytics Challenge

## Detalles del desafío
Ver Challenge Data Analytics con Python.pdf para los detalles y requerimientos

## Dependencias necesarias
para el correcto funcionamiento deben instalarse los siguientes paquetes con PIP:
    
    SQLAlchemy
    configparser
    numpy
    pandas
    psycopg2
    pytz
    requests

## Detalle Base Datos PostgreSQL
Los datos de la base de datos SQL Postgre deben setearse desde el archivo Database.ini
Los parámetros del archivo son tomados por la función db_config definida en db_config.py

Se incluye el archivo Database_dummy.ini para modificar con los datos del servidor y posteriormente guardar como Database.ini
 
El deploy de la base de datos debe realizarse a través del script SQL_Deploy.py


