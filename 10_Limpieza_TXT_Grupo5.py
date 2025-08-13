"""
Created on Sun Jul 13 19:21:52 2025
@author: danie
"""
from pathlib import Path
import os
import pandas as pd
import re

# Ruta base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# Cambia a la carpeta Syntaxis para importar funciones propias
os.chdir(BASE_DIR / "Syntaxis")
from Funciones_Compartidas import cargar_txt

# Ruta de entrada: TXT extraídos previamente del Grupo5
ruta_input = BASE_DIR / "Inputs" / "Route data" / "TXT Grupo5"
os.chdir(ruta_input)

# Lista de carpetas por año
Anios = os.listdir()

# --- Recorre cada año ---
for anio in Anios:
    municipios = os.listdir(ruta_input / anio)  # subcarpetas con nombre de municipio
    for munpio in municipios:
        
        # Lista todos los .txt del municipio
        archivos = [
            f for f in os.listdir(ruta_input / anio / munpio)
            if f.lower().endswith('.txt')
        ]
        
        Concatenar = pd.DataFrame({})
        
        if archivos != []:
            # --- Lee y concatena todos los TXT ---
            for archiv in archivos:
                Temporal = cargar_txt(ruta_input / anio / munpio / archiv)
                Concatenar = pd.concat([Concatenar, Temporal])
            
            # --- Crea carpeta de salida ---
            os.makedirs(BASE_DIR / "Outputs" / anio, exist_ok=True)
            os.chdir(BASE_DIR / "Outputs" / anio)
            
            # Reinicia índice
            Concatenar.reset_index(inplace=True)
            
            # Etiqueta de archivo: "05" + código de municipio
            etiqueta = "05" + Concatenar.MUNICIPIO[0]
            Concatenar.drop(columns="index",inplace=True)
            # Guarda en Excel
            Concatenar.to_parquet(f"{etiqueta}.parquet", index=False)

            
        





