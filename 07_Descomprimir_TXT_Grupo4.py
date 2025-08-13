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

# Ruta base del proyecto (2 niveles por encima del archivo actual)
BASE_DIR = Path(__file__).resolve().parent.parent

# Ruta donde se encuentran los datos procesados (ZIP o TXT) para Grupo4
ruta_input = BASE_DIR / "Inputs" / "Process data" / "TXT Grupo4"
os.chdir(ruta_input)

# Lista de carpetas en la ruta de entrada
Carpetas = os.listdir()

# Lista de archivos ZIP en el directorio actual
zips = [f for f in os.listdir() if f.lower().endswith('.zip')]

# Extrae los años presentes en los nombres de los ZIP (4 dígitos)
Anios = [re.search(r"\d{4}", nom).group() if re.search(r"\d{4}", nom) else None for nom in zips]

# Lista todos los archivos en el directorio actual
Archivos = os.listdir()

# --- Renombrar archivos para quitar guiones bajos en nombres específicos ---
for archivo in Archivos:
    nuevo_archivo = archivo.replace("La_Union", "LaUnion").replace("Puerto_Triunfo", "PuertoTriunfo")
    if archivo != nuevo_archivo:
        print(f"Renombrando: {archivo} -> {nuevo_archivo}")
        os.rename(archivo, nuevo_archivo)
    else:
        print(f"No se requiere renombrar: {archivo}")

# --- Clasificación de archivos ZIP por "base" y "zona" ---
Listado_archivos = {}
for archivo in zips:
    # Patrón para capturar: base (año y municipio) y zona opcional
    patron = r'^(Anexos_Incrementos_Anuales_\d{4}_[^_]+)(?:_([A-Za-z]+))?\.zip$'
    match = re.match(patron, archivo)
    if match:
        base = match.group(1)  # Ej: 'Anexos_Incrementos_Anuales_2024_Turbo'
        zona = match.group(2) if match.group(2) else ''  # Ej: 'Urbana', '' si no existe
        original = archivo
        if base not in Listado_archivos:
            Listado_archivos[base] = []
        Listado_archivos[base].append(original)
    else:
        print(archivo)
        print("El archivo no coincide con el patrón esperado.")

# --- Extraer archivos ZIP a carpetas organizadas por año y municipio ---
carpeta_destino = BASE_DIR / "Inputs" / "Route data" / "TXT Grupo4"
for archivo_key in Listado_archivos.keys():
    archivos = Listado_archivos[archivo_key]
    for archivo in archivos:
        anio = re.search(r"\d{4}", archivo).group()
        municipio = re.search(
            r'Anexos_Incrementos_Anuales_\d{4}_([A-Za-zÁÉÍÓÚÑáéíóúñüÜ]+)',
            archivo_key
        ).group(1)
        os.makedirs(carpeta_destino / anio / municipio, exist_ok=True)
        with zipfile.ZipFile(archivo, 'r') as zip_ref:
            zip_ref.extractall(carpeta_destino / anio / municipio)
            print(f"Archivos extraídos a: {carpeta_destino}")

# --- Copiar archivos TXT sueltos a carpetas organizadas ---
txts = [f for f in os.listdir() if f.lower().endswith('.txt')]
for archivo in txts:
    anio = re.search(r"\d{4}", archivo).group()
    municipio = re.search(
        r'Anexos_Incrementos_Anuales_\d{4}_([A-Za-zÁÉÍÓÚÑáéíóúñüÜ]+)',
        archivo
    ).group(1)
    os.makedirs(carpeta_destino / anio / municipio, exist_ok=True)
    shutil.copy2(ruta_input / archivo, carpeta_destino / anio / municipio / archivo)


    
    
    
    
    













