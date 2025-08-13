"""
Created on Sun Jul 13 19:21:52 2025

@author: danie
"""

from pathlib import Path
import os
import zipfile         # Para manipular archivos ZIP
import re              # Para expresiones regulares

# Define la ruta base dos niveles arriba del archivo actual
BASE_DIR = Path(__file__).resolve().parent.parent


# Define la ruta donde están las carpetas con archivos ZIP
ruta_input = BASE_DIR / "Inputs" / "Process data" / "TXT Grupo3"

# Cambia el directorio actual a la ruta de entrada
os.chdir(ruta_input)

# Lista todas las carpetas dentro de la ruta de entrada
Carpetas = os.listdir()

# Itera sobre cada carpeta (por ejemplo, carpetas por año o categoría)
for carpeta in Carpetas:
    # Cambia el directorio a la carpeta específica
    os.chdir(ruta_input / carpeta)

    # Lista todos los archivos ZIP dentro de esta carpeta (case-insensitive)
    zips = [f for f in os.listdir() if f.lower().endswith('.zip')]

    # Diccionario para almacenar listas de archivos agrupados por año
    Listado_archivos = {}

    # Itera sobre cada archivo ZIP
    for arch in zips:
        # Extrae el año (4 dígitos) que aparece en el nombre del archivo
        Anios = re.search(r"\d{4}", arch).group()

        # Si el año no está en el diccionario, inicializa la lista
        if Anios not in Listado_archivos:
            Listado_archivos[Anios] = []

        # Agrega el archivo al listado correspondiente al año extraído
        Listado_archivos[Anios].append(arch)

    # Itera sobre cada año en el diccionario
    for anios in Listado_archivos.keys():
        # Itera sobre cada archivo para ese año
        for archivo in Listado_archivos[anios]:
            # Define la carpeta destino donde se extraerán los archivos ZIP
            carpeta_destino = BASE_DIR / "Inputs" / "Route data" / "TXT Grupo3"

            # Extrae el contenido del archivo ZIP en la ruta destino organizada por año y carpeta
            with zipfile.ZipFile(archivo, 'r') as zip_ref:
                zip_ref.extractall(carpeta_destino / anios / carpeta)

                # Imprime confirmación con la ruta donde se extrajeron los archivos
                print(f"Archivos extraídos a: {carpeta_destino}")

        





















