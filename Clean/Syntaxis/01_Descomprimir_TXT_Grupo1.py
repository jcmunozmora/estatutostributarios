"""
Created on Sun Jul 13 19:21:52 2025
@author: danie
"""

# Importación de librerías necesarias
from pathlib import Path    # Para manejar rutas de forma más segura y multiplataforma
import os                   # Para operaciones con el sistema de archivos
import zipfile              # Para trabajar con archivos ZIP
import re                   # Para trabajar con expresiones regulares

# Definición de la ruta base del proyecto (dos carpetas hacia arriba desde este archivo)
BASE_DIR = Path(__file__).resolve().parent.parent

# Ruta donde se encuentran los archivos ZIP originales
ruta_input = BASE_DIR / "Inputs" / "Process data" / "Historicos OVC TXT"

# Cambiar el directorio de trabajo a la carpeta donde están los archivos
os.chdir(ruta_input)

# Listar todos los archivos y carpetas en la ruta de entrada
Total_archivos = os.listdir()

# Filtrar solo los archivos que terminan en .zip (ignorando mayúsculas/minúsculas)
zips = [f for f in os.listdir() if f.lower().endswith('.zip')]

# Comparar si el número de elementos totales es igual al número de archivos ZIP
Compara = len(Total_archivos) == len(zips)

print(f"¿El número de archivos ZIPS es igual al número de carpetas?: {Compara}")

# Extraer el año (4 dígitos) de cada nombre de archivo ZIP usando expresiones regulares
Anios = [
    re.search(r"\d{4}", nom).group() if re.search(r"\d{4}", nom) else None
    for nom in zips
]

# Extraer el nombre del municipio a partir de los nombres de archivo ZIP
# Patrón esperado: 'Anexos_Incrementos_Anuales_AAAA Municipio.zip'
municipios = [
    re.search(r'Anexos_Incrementos_Anuales_\d{4} (.+)\.zip', archivo).group(1)
    for archivo in zips
]

# Carpeta destino donde se extraerán los archivos descomprimidos
carpeta_destino = BASE_DIR / "Inputs" / "Route data" / "Historicos OVC TXT"

# Iterar sobre cada archivo ZIP junto con su municipio y año
for archivo_zip, municipio, anio in zip(zips, municipios, Anios):
    # Crear la carpeta destino (año/municipio) si no existe
    os.makedirs(carpeta_destino / anio / municipio, exist_ok=True)
    
    # Abrir el archivo ZIP y extraer todo en la carpeta correspondiente
    with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
        zip_ref.extractall(carpeta_destino / anio / municipio)

print(f"Archivos extraídos a: {carpeta_destino}")



