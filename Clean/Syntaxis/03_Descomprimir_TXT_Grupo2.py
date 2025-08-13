"""
Created on Sun Jul 13 19:21:52 2025
@author: danie
"""

# ==== Importación de librerías ====
from pathlib import Path   # Manejo de rutas multiplataforma
import os                  # Operaciones con el sistema de archivos
import pandas as pd        # (No se usa aquí, pero útil para manejo de datos)
import zipfile             # (No se usa aquí, pero útil para trabajar con ZIPs)
import numpy as np         # (No se usa aquí)
import re                  # Expresiones regulares

# ==== Definición de rutas ====
BASE_DIR = Path(__file__).resolve().parent.parent

# Ruta a la carpeta donde están los archivos ZIP del grupo 2
ruta_input = BASE_DIR / "Inputs" / "Process data" / "TXT Grupo2"
os.chdir(ruta_input)

# ==== Listar archivos ====
Total_archivos = os.listdir()

# Filtrar solo archivos ZIP (ignorando mayúsculas/minúsculas)
zips = [f for f in os.listdir() if f.lower().endswith('.zip')]

# Comparar si todos los elementos en la carpeta son ZIPs
Compara = len(Total_archivos) == len(zips)
print(f"¿El número de archivos ZIPS es igual al número de carpetas?: {Compara}")

# ==== Extraer años de los nombres de los ZIP ====
Anios = [
    re.search(r"\d{4}", nom).group() if re.search(r"\d{4}", nom) else None
    for nom in zips
]

# ==== Buscar archivos que NO cumplen el patrón esperado ====
# Patrón esperado: Anexos_Incrementos_Anuales_AAAA_Municipio.zip
no_cumplen = [
    archivo for archivo in zips
    if not re.search(r'Anexos_Incrementos_Anuales_\d{4}_(.+)\.zip', archivo)
]

print("Archivos que NO cumplen con el patrón:")

# Corregir nombres cambiando "anuales" por "Anuales"
for archivo in no_cumplen:
    print(archivo)
    nuevo_archivo = archivo.replace("anuales", "Anuales")
    os.rename(archivo, nuevo_archivo)

# ==== Corregir nombre específico ====
# Reemplazar "Carmen_Viboral" por "CarmenViboral"
for archivo in zips:
    nuevo_archivo = archivo.replace("Carmen_Viboral", "CarmenViboral")
    os.rename(archivo, nuevo_archivo)

# ==== Verificación posterior a los cambios ====
no_cumplen = [
    archivo for archivo in zips
    if not re.search(r'Anexos_Incrementos_Anuales_\d{4}_(.+)\.zip', archivo)
]
print(no_cumplen)

# ==== Recargar lista de ZIPs y extraer años nuevamente ====
zips = [f for f in os.listdir() if f.lower().endswith('.zip')]
Anios = [
    re.search(r"\d{4}", nom).group() if re.search(r"\d{4}", nom) else None
    for nom in zips
]

# ==== Diccionario para agrupar archivos por base (sin zona) ====
Listado_archivos = {}

# Iterar sobre cada archivo ZIP encontrado
for archivo in zips:
    # Patrón:
    # 1. Empieza con "Anexos_Incrementos_Anuales_"
    # 2. Sigue con 4 dígitos (año)
    # 3. Luego un guion bajo y el nombre del municipio (sin guion bajo dentro)
    # 4. Opcionalmente, otro guion bajo y la zona (solo letras, ej. "Urbana" o "Rural")
    # 5. Termina con ".zip"
    patron = r'^(Anexos_Incrementos_Anuales_\d{4}_[^_]+)(?:_([A-Za-z]+))?\.zip$'
    match = re.match(patron, archivo)

    if match:
        base = match.group(1)                     # Ej: 'Anexos_Incrementos_Anuales_2024_Turbo'
        zona = match.group(2) if match.group(2) else ''  # Ej: 'Urbana' o '' si no existe
        original = archivo                         # Guardar nombre original

        # Si la base no existe en el diccionario, crear la lista
        if base not in Listado_archivos:
            Listado_archivos[base] = []

        # Agregar el archivo a la lista correspondiente
        Listado_archivos[base].append(original)
    else:
        # Si no coincide con el patrón, informar
        print(archivo)
        print("El archivo no coincide con el patrón esperado.")

# ==== Definir carpeta destino para extracción ====
carpeta_destino = BASE_DIR / "Inputs" / "Route data" / "TXT Grupo2"

# Iterar sobre las claves (base) del diccionario
for archivo_key in Listado_archivos.keys():
    archivos = Listado_archivos[archivo_key]

    # Iterar sobre todos los archivos asociados a esa base
    for archivo in archivos:
        # Extraer el año del nombre del archivo
        anio = re.search(r"\d{4}", archivo).group()

        # Extraer el nombre del municipio de la clave base
        municipio = re.search(
            r'Anexos_Incrementos_Anuales_\d{4}_([A-Za-zÁÉÍÓÚÑáéíóúñüÜ]+)', 
            archivo_key
        ).group(1)

        # Crear la carpeta de destino (año/municipio)
        os.makedirs(carpeta_destino / anio / municipio, exist_ok=True)

        # Abrir el ZIP y extraerlo en la carpeta correspondiente
        with zipfile.ZipFile(archivo, 'r') as zip_ref:
            zip_ref.extractall(carpeta_destino / anio / municipio)
            print(f"Archivos extraídos a: {carpeta_destino}")















