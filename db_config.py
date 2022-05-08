# Configuración de Base de Datos

from configparser import ConfigParser

def config(filename='Database.ini', section='postgresql'):

    parser = ConfigParser()
    parser.read(filename) # Lectura archivo de parámetros

    # Adquisición de parámetros de base de datos [section]
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Sección {0} no encontrada en el archivo {1} '.format(section, filename))

    return db