# -*- coding: utf-8 -*-
"""
Script para procesar archivos TXT organizados por año y municipio, 
concatenarlos y exportarlos como archivos Excel.
@author: danie
"""

from pathlib import Path
import os
import pandas as pd

# Ruta base del proyecto (dos niveles arriba del script actual)
BASE_DIR = Path(__file__).resolve().parent.parent

# Cambiar directorio a la carpeta "Syntaxis"
os.chdir(BASE_DIR / "Syntaxis")

# Importar función personalizada para cargar archivos TXT
from Funciones_Compartidas import cargar_txt

# Ruta donde se encuentran los archivos TXT organizados por año y municipio
ruta_input = BASE_DIR / "Inputs" / "Route data" / "TXT Grupo2"

# Cambiar directorio de trabajo a la ruta de entrada
os.chdir(ruta_input)

# Listar las carpetas de años disponibles (ejemplo: ["2023", "2024", ...])
Anios = os.listdir()

# Iterar sobre cada carpeta de año
for anio in Anios:
    # Listar los municipios dentro de la carpeta del año
    municipios = os.listdir(ruta_input / anio)

    # Iterar sobre cada municipio
    for munpio in municipios:
        # Filtrar únicamente los archivos que terminen en .txt (sin importar mayúsculas)
        archivos = [f for f in os.listdir(ruta_input / anio / munpio) if f.lower().endswith('.txt')]

        # DataFrame vacío donde se concatenarán los datos de todos los TXT del municipio
        Concatenar = pd.DataFrame({})

        # Iterar sobre cada archivo TXT del municipio
        for archiv in archivos:
            # Cargar el archivo TXT usando la función personalizada
            Temporal = cargar_txt(ruta_input / anio / munpio / archiv)

            # Concatenar el DataFrame cargado al acumulador
            Concatenar = pd.concat([Concatenar, Temporal])

        # Crear carpeta de salida para el año, si no existe
        os.makedirs(BASE_DIR / "Outputs" / anio, exist_ok=True)

        # Cambiar directorio a la carpeta de salida del año
        os.chdir(BASE_DIR / "Outputs" / anio)

        # Reiniciar índice del DataFrame concatenado
        Concatenar.reset_index(inplace=True)

        # Generar etiqueta del archivo Excel (prefijo "05" + código de municipio de la primera fila)
        etiqueta = "05" + Concatenar.MUNICIPIO[0]
        Concatenar.drop(columns="index",inplace=True)
        # Guardar el DataFrame en formato Excel
        Concatenar.to_parquet(f"{etiqueta}.parquet", index=False)

        





