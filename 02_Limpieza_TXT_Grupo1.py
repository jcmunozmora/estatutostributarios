"""
Created on Sun Jul 13 19:21:52 2025
@author: danie
"""
# ==== Importación de librerías ====
from pathlib import Path       # Manejo de rutas de forma multiplataforma
import os                      # Operaciones con el sistema de archivos
import pandas as pd            # Manejo y análisis de datos

# ==== Definición de rutas base ====
# Ruta del directorio raíz del proyecto (dos niveles arriba desde este archivo)
BASE_DIR = Path(__file__).resolve().parent.parent

# Cambiar el directorio de trabajo a la carpeta "Syntaxis"
os.chdir(BASE_DIR / "Syntaxis")

# Importar función personalizada para cargar archivos .txt
# Debe estar definida en el módulo "Funciones_Compartidas.py"
from Funciones_Compartidas import cargar_txt

# Ruta donde se encuentran los datos TXT organizados por año y municipio
ruta_input = BASE_DIR / "Inputs" / "Route data" / "Historicos OVC TXT"
os.chdir(ruta_input)

# Listar los años disponibles (carpetas dentro de ruta_input)
Anios = os.listdir()

# ==== Iterar sobre cada año ====
for anio in Anios:
    # Listar los municipios para ese año (subcarpetas)
    municipios = os.listdir(ruta_input / anio)
    
    # ==== Iterar sobre cada municipio ====
    for munpio in municipios:
        # Listar los archivos TXT dentro del municipio
        archivos = [
            f for f in os.listdir(ruta_input / anio / munpio) 
            if f.lower().endswith('.txt')
        ]
        
        # DataFrame vacío donde se concatenarán todos los TXT
        Concatenar = pd.DataFrame({})
        
        # ==== Cargar y concatenar cada archivo TXT ====
        for archiv in archivos:
            Temporal = cargar_txt(ruta_input / anio / munpio / archiv)
            Concatenar = pd.concat([Concatenar, Temporal])
        
        # ==== Guardar resultados ====
        # Crear carpeta de salida para ese año si no existe
        os.makedirs(BASE_DIR / "Outputs" / anio, exist_ok=True)
        
        # Cambiar el directorio de trabajo a la carpeta de salida
        os.chdir(BASE_DIR / "Outputs" / anio)
        
        # Reiniciar el índice del DataFrame
        Concatenar.reset_index(inplace=True)
        
        # Etiqueta del archivo Excel:
        # "05" + código de municipio de la primera fila del DataFrame
        etiqueta = "05" + Concatenar.MUNICIPIO[0]
        #Eliminamos la columna cuyo nombre es index
        Concatenar.drop(columns=["index"], inplace=True)
        # Guardar como Excel (usa motor openpyxl)
        Concatenar.to_parquet(f"{etiqueta}.parquet", index=False)
        
