"""
Created on Sun Jul 13 19:21:52 2025
@author: danie
"""
from pathlib import Path
import os
import pandas as pd
import zipfile
import numpy as np
import re
import shutil

# Carpeta raíz del proyecto (dos niveles arriba del script)
BASE_DIR = Path(__file__).resolve().parent.parent

# Carpeta de entrada (contiene subcarpetas con ZIPs)
ruta_input = BASE_DIR / "Inputs" / "Process data" / "TXT Grupo5"
os.chdir(ruta_input)

# Lista de subcarpetas dentro de TXT Grupo5
Carpetas = os.listdir()

# --- Recorre cada carpeta ---
for carpeta in Carpetas:
    os.chdir(ruta_input / carpeta)  # se mete en la carpeta del municipio/grupo
    
    # Lista todos los ZIP en la carpeta
    zips = [f for f in os.listdir() if f.lower().endswith('.zip')]
    
    # Diccionario {año: [lista de archivos zip]}
    Listado_archivos = {}
    for arch in zips:
        Anios = re.search(r"\d{4}", arch).group()  # extrae el año del nombre
        if Anios not in Listado_archivos:
            Listado_archivos[Anios] = []
        Listado_archivos[Anios].append(arch)
    
    # --- Extrae cada ZIP en carpeta destino organizada por año ---
    for anios in Listado_archivos.keys():
        for archivo in Listado_archivos[anios]:
            carpeta_destino = BASE_DIR / "Inputs" / "Route data" / "TXT Grupo5"
            with zipfile.ZipFile(archivo, 'r') as zip_ref:
                zip_ref.extractall(carpeta_destino / anios / carpeta)
                print(f"Archivos extraídos a: {carpeta_destino}")


ruta_input = BASE_DIR / "Inputs" / "Process data" / "TXT Grupo5"
os.chdir(ruta_input)

# --- Copiar archivos TXT sueltos a carpetas organizadas ---
for carpeta in Carpetas:
    txts = [f for f in os.listdir(ruta_input/carpeta) if f.lower().endswith('.txt')]
    if txts !=[]:
        for archivo in txts:
             anio = re.search(r"\d{4}", archivo).group()
             carpeta_destino = BASE_DIR / "Inputs" / "Route data" / "TXT Grupo5"
             os.makedirs(carpeta_destino / anio / carpeta, exist_ok=True)
             shutil.copy2(ruta_input /carpeta/archivo, carpeta_destino / anio / carpeta / archivo)

    




    

    
    
    













