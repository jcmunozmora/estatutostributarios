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


BASE_DIR = Path(__file__).resolve().parent.parent

# Ruta relativa al archivo
ruta_input = BASE_DIR /"Inputs"/"Process data"/"Information_2015_2023"

os.chdir(ruta_input)
zips = [f for f in os.listdir() if f.lower().endswith('.zip')]
Nombres = [re.search(r"\d{4}", nom).group() if re.search(r"\d{4}", nom) else None for nom in zips]


carpeta_destino = BASE_DIR /"Inputs"/"Route data"/"Information_2015_2023"


for archivo_zip,anio in zip(zips,Nombres):
    os.makedirs(carpeta_destino/anio, exist_ok=True)
    with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
         zip_ref.extractall(carpeta_destino/anio)
print(f"Archivos extraídos a: {carpeta_destino}")


carpeta_destino = BASE_DIR /"Inputs"/"Route data"/"Information_2015_2023"
for archivo_zip,anio in zip(zips,Nombres):
    for nuevo in os.listdir(carpeta_destino/anio): 
         with zipfile.ZipFile(carpeta_destino/anio/nuevo, 'r') as zip_ref:
             zip_ref.extractall(carpeta_destino/anio)

print(f"Archivos extraídos a: {carpeta_destino}")




