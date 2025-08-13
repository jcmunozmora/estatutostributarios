"""
Created on Sun Jul 13 19:21:52 2025

@author: danie
"""

from pathlib import Path
import os
import pandas as pd


# Define la ruta base del proyecto (dos niveles arriba del archivo actual)
BASE_DIR = Path(__file__).resolve().parent.parent

# Cambia el directorio de trabajo a la carpeta "Syntaxis"
os.chdir(BASE_DIR / "Syntaxis")

# Importa la función personalizada para cargar archivos TXT
from Funciones_Compartidas import cargar_txt

# Define la ruta donde están organizados los archivos TXT por año y municipio
ruta_input = BASE_DIR / "Inputs" / "Route data" / "TXT Grupo3"

# Cambia el directorio de trabajo a la ruta de entrada
os.chdir(ruta_input)

# Obtiene la lista de carpetas dentro de ruta_input (probablemente carpetas por año)
Anios = os.listdir()

# Itera sobre cada carpeta año
for anio in Anios:
    # Obtiene la lista de carpetas dentro del año (probablemente municipios)
    municipios = os.listdir(ruta_input / anio)

    # Itera sobre cada municipio
    for munpio in municipios:
        # Lista archivos con extensión .txt (case-insensitive) dentro del municipio
        archivos = [f for f in os.listdir(ruta_input / anio / munpio) if f.lower().endswith('.txt')]

        # Inicializa un DataFrame vacío para concatenar los datos
        Concatenar = pd.DataFrame({})

        # Solo continúa si la lista de archivos no está vacía
        if archivos != []:
            # Itera sobre cada archivo TXT
            for archiv in archivos:
                # Carga el archivo TXT usando la función personalizada
                Temporal = cargar_txt(ruta_input / anio / munpio / archiv)

                # Concatena los datos cargados con el DataFrame acumulador
                Concatenar = pd.concat([Concatenar, Temporal])

            # Crea la carpeta de salida para el año si no existe
            os.makedirs(BASE_DIR / "Outputs" / anio, exist_ok=True)

            # Cambia el directorio a la carpeta de salida del año
            os.chdir(BASE_DIR / "Outputs" / anio)

            # Reinicia el índice del DataFrame concatenado (añade columna 'index')
            Concatenar.reset_index(inplace=True)

            # Crea una etiqueta para el archivo de salida: "05" + código del municipio 
            etiqueta = "05" + Concatenar.MUNICIPIO[0]
            Concatenar.drop(columns="index",inplace=True)
     
            # Exporta el DataFrame concatenado a un archivo Excel, sin índice, usando openpyxl
            Concatenar.to_parquet(f"{etiqueta}.parquet", index=False)

            
        
        

