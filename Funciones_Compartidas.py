# -*- coding: utf-8 -*-
"""
Created on Thu Jul 24 15:41:13 2025

@author: danie
"""

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




import pandas as pd
import numpy as np

def cargar_txt(archivo):
    with open(archivo, "r", encoding="latin") as f:
         filas = [line.strip().split("|") for line in f]

    # Encontrar la longitud máxima (mayor número de columnas)
    max_columnas = max(len(fila) for fila in filas)

    # Rellenar con valores vacíos las filas que tienen menos columnas
    filas_normalizadas = [fila + [''] * (max_columnas - len(fila)) for fila in filas]

    # Crear DataFrame

    Temporal = pd.DataFrame(filas_normalizadas)

    columnas1 = [
        "IDREG", "VIGENCIA", "RES", "FICHA", "MUNICIPIO", "SECTOR", "CORREGIMIENTO",
        "BARRIO", "MZ", "PRED", "EDFI", "UNI", "MEJORA", "NRO ANT", "MPIO ANT",
        "SECA", "CORA", "BAA", "MZAA", "PRDA", "EDIFAQ", "UNIDADA", "MEJORAA",
        "DESTINO", "DIRECCIÓN", "DESPLAZADS", "NOVEDAD", "CLASE DEMUTACIONUTA",
        "FECHA DE INSCRIP", "NPN", "TIPO DE TRAMITE"
    ]

    columnas2 = [
        "IDREG", "FICHA", "VIGEAVALU", "AREA LT COMUN", "AREA LT PRIVADA",
        "AREA CONST COMUN", "AREA CONT PRIVADA", "$LT COMUN", "$LT PRIVADO",
        "$CONT COMUN", "$CONST PRIVADA", "$AVALUO", "AUTOESTIMACION"
    ]

    columnas3 = [
        "IDREG", "FICHA", "NOVEDAD", "TIPODOC", "DOCUMENTO", "TIPROP", "1APELLIDO",
        "2APELLIDO", "NOMBRE", "RAZON SOCIAL", "NOTARIA", "ESCRITURA",
        "FECHA ESCRITURA", "DERECHO", "LITIGIO", "%LITIGIO", "GRAVABLE",
        "CIRCULO", "MATRICULA", "TOMO", "LIBRO", "CODIGO FIDEICOMISO"
    ]

    # Lectura y separación
    #Temporal = pd.read_csv(archivo, sep="|", on_bad_lines='skip', encoding="latin", header=None)

    Temporal1 = Temporal[Temporal[0].astype(int) == 1]
    Temporal1 = Temporal1[list(range(0,31))]
    #Temporal1.drop(columns=[31], inplace=True)
    Temporal1.columns = columnas1

    Temporal2 = Temporal[Temporal[0].astype(int) == 2]
    Temporal2 = Temporal2[list(range(0,13))]
    #Temporal2.drop(columns=[13], inplace=True)
    Temporal2.columns = columnas2

    Temporal3 = Temporal[Temporal[0].astype(int) == 3]
    Temporal3 = Temporal3[list(range(0,22))]
    #Temporal3.drop(columns=[22, 23], inplace=True)
    Temporal3.columns = columnas3

    # Uniones
    Temporal4 = Temporal1.merge(Temporal2, on="FICHA", how="left")
    Temporal5 = Temporal3.merge(Temporal4, on="FICHA", how="left")

    # Selección de columnas clave
    Seleccionadas = [
        "MUNICIPIO", "FICHA", "MATRICULA", "DIRECCIÓN", "TIPODOC", "DOCUMENTO",
        "AREA LT PRIVADA", "AREA LT COMUN", "AREA CONST COMUN", "AREA CONT PRIVADA",
        "$LT PRIVADO", "$CONST PRIVADA", "$AVALUO", "AUTOESTIMACION", "VIGEAVALU",
        "DESTINO", "DERECHO", "NPN", "SECTOR", "GRAVABLE"
    ]
    Temporal5 = Temporal5[Seleccionadas]

    # Transformaciones
    Temporal5["A. TERRENO"] = Temporal5["AREA LT PRIVADA"].replace('', np.nan) .fillna(0).astype(int) + Temporal5["AREA LT COMUN"].fillna(0).astype(int)
    Temporal5["A. CONSTRUCCIÓN"] = Temporal5["AREA CONST COMUN"].replace('', np.nan) .fillna(0).astype(int) + Temporal5["AREA CONT PRIVADA"].fillna(0).astype(int)
    Temporal5["GRAVABLE"] = np.where(Temporal5["GRAVABLE"].replace('', np.nan) .fillna(1).astype(int) == 1, "Si", "No")
    Temporal5["AUTOESTIMACION"] = np.where(Temporal5["AUTOESTIMACION"].replace('', np.nan).fillna(0).astype(int) == 1, "Si", "No")
    Temporal5["DERECHO"] = Temporal5["DERECHO"].replace('', np.nan).fillna(0).astype(int) / 100000000

    # Renombrar columnas
    cambio_campos = {
        'MUNICIPIO': "MUNICIPIO",
        "DESTINO": "DESTINO_ECONOMICO",
        "A. TERRENO": "AREA_TERRENO",
        "A. CONSTRUCCIÓN": "AREA_CONSTRUIDA",
        '$AVALUO': "AVALUO",
        'VIGEAVALU': "VIGENCIA",
        "NPN": "NUMERO_PREDIAL_NACIONAL",
        "DERECHO": "DERECHO",
        'FICHA': "FICHA",
        'MATRICULA': "MATRICULA",
        'TIPODOC': 'TIPO_DOCUMENTO',
        'DOCUMENTO': 'NUMERO_DOCUMENTO',
        'SECTOR': "ZONA",
        "$CONST PRIVADA": "AVALUO_CONSTRUCCION",
        "$LT PRIVADO": "AVALUO_TERRENO",
        'GRAVABLE': 'GRAVABLE',
        'AUTOESTIMACION': 'AUTOESTIMACION'
    }

    Temporal5 = Temporal5[cambio_campos.keys()]
    Temporal5.rename(columns=cambio_campos, inplace=True)

    # Reordenar columnas
    orden = [
        "MUNICIPIO", "NUMERO_PREDIAL_NACIONAL", "FICHA", "MATRICULA", "TIPO_DOCUMENTO", "NUMERO_DOCUMENTO",
        "DESTINO_ECONOMICO", "ZONA", "AREA_TERRENO", "AREA_CONSTRUIDA",
        "AVALUO_CONSTRUCCION", "AVALUO_TERRENO", "AVALUO", "VIGENCIA",
        "DERECHO", "GRAVABLE", "AUTOESTIMACION"
    ]
    Temporal5 = Temporal5[orden]

    # Zonas
    Temporal5["ZONA"] = np.where(Temporal5["NUMERO_PREDIAL_NACIONAL"].astype(str).str[5:7] == "00", "Rural", "Urbana")

    # Diccionario de destinos económicos
    homologacion_codigos = {
        "004": "024",
        "015": "019",
        "016": "001",
        "017": "030",
        "020": "030",
        "021": "001",
        "022": "001",
        "023": "024",
        "031": "024"
    }

    destinos_economicos = {
        "001": "Habitacional",
        "002": "Industrial",
        "003": "Comercial",
        "004": "Agropecuario",
        "005": "Mineros_hidrocarburos",
        "006": "Cultural",
        "007": "Recreacional",
        "008": "Salubridad",
        "009": "Institucional",
        "012": "Lote urbanizado no construido",
        "013": "Lote urbanizable no urbanizado",
        "014": "Lote no urbanizable",
        "019": "Uso publico",
        "024": "Agricola",
        "025": "Pecuario",
        "027": "Educativo",
        "028": "Agroindustrial",
        "029": "Religioso",
        "030": "Forestal",
        "060": "Acuicola",
        "061": "Agroforestal",
        "062": "Infraestructura asociada produccion agropecuaria",
        "063": "Infraestructura hidraulica",
        "064": "Infraestructura saneamiento basico",
        "065": "Infraestructura transporte",
        "066": "Servicios funerarios",
        "067": "Infraestructura seguridad",
        #Destinos que ya no existen
        "010": "Mixto",
        "011":"Otros",
        "026":"Servicios Especiales",
        "040":"Por Definir Catastro"
    }
    Temporal5["DESTINO_ECONOMICO"]=Temporal5["DESTINO_ECONOMICO"].replace(homologacion_codigos)
    Temporal5["DESTINO_ECONOMICO"] = Temporal5["DESTINO_ECONOMICO"].replace(destinos_economicos)
    columnas_a_convertir = [
    "FICHA",
    "MATRICULA",
    "NUMERO_DOCUMENTO",
    "AVALUO_CONSTRUCCION",
    "AVALUO_TERRENO",
    "AVALUO",
    "VIGENCIA"
               ]
    for col in columnas_a_convertir:
        Temporal5[col] = pd.to_numeric(Temporal5[col], errors="coerce")
    return Temporal5
