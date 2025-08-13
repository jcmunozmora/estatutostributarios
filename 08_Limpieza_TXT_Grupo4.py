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
import pickle

# Directorio raíz (dos niveles arriba del script actual)
BASE_DIR = Path(__file__).resolve().parent.parent

# Cambia a la carpeta "Syntaxis" para poder importar funciones compartidas
os.chdir(BASE_DIR / "Syntaxis")
from Funciones_Compartidas import cargar_txt  # función para cargar un TXT a DataFrame

# Ruta de entrada: datos TXT ya organizados por año y municipio
ruta_input = BASE_DIR / "Inputs" / "Route data" / "TXT Grupo4"
os.chdir(ruta_input)

# Lista de años (subcarpetas)
Anios = os.listdir()

# --- Recorre todos los años ---
for anio in Anios:
    municipios = os.listdir(ruta_input / anio)  # subcarpetas por municipio

    # --- Recorre todos los municipios dentro del año ---
    for munpio in municipios:
        # Lista todos los .txt en esa carpeta
        archivos = [
            f for f in os.listdir(ruta_input / anio / munpio)
            if f.lower().endswith('.txt')
        ]

        Concatenar = pd.DataFrame({})  # DataFrame acumulador

        # --- Carga y concatena todos los TXT ---
        for archiv in archivos:
            Temporal = cargar_txt(ruta_input / anio / munpio / archiv)
            Concatenar = pd.concat([Concatenar, Temporal])

        # --- Crea carpeta de salida para ese año ---
        os.makedirs(BASE_DIR / "Outputs" / anio, exist_ok=True)

        # Cambia el directorio de trabajo a la carpeta de salida
        os.chdir(BASE_DIR / "Outputs" / anio)

        # Resetea índices del DataFrame concatenado
        Concatenar.reset_index(inplace=True)

        # Etiqueta del archivo Excel (ej: "05002.xlsx")
        etiqueta = "05" + Concatenar.MUNICIPIO[0]
        Concatenar.drop(columns="index",inplace=True)

        # Exporta el DataFrame a Excel
        Concatenar.to_parquet(f"{etiqueta}.parquet", index=False)