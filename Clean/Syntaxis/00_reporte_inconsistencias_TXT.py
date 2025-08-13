# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 22:09:04 2025
@author: danie
"""
from pathlib import Path
import os
import pandas as pd
import zipfile
import numpy as np
import re

BASE_DIR = Path(__file__).resolve().parent.parent


ruta_input = BASE_DIR /"Outputs"
Anios=os.listdir(ruta_input)
Resultado={
    "AÑO":[],
    "CODIGO":[],
    "TEST1":[],
    "TEST2":[],
    "TEST3":[],
    "TEST4":[],
    "TEST5":[],
    "TEST6":[],
    "TEST7":[],
    "TEST8":[],
    "TEST9":[],
    "TEST10":[]

}


Destinos=["Habitacional", "Industrial", "Comercial", "Agropecuario", "Mineros_hidrocarburos",
"Cultural", "Recreacional", "Salubridad", "Institucional", 
"Lote urbanizado no construido", "Lote urbanizable no urbanizado", "Lote no urbanizable",
"Uso publico", "Agricola", "Pecuario", "Educativo", "Agroindustrial", "Religioso", 
"Forestal", "Acuicola", "Agroforestal", 
"Infraestructura asociada produccion agropecuaria",
"Infraestructura hidraulica", "Infraestructura saneamiento basico",
"Infraestructura transporte", "Servicios funerarios", "Infraestructura seguridad",
"Mixto", "Otros", "Servicios Especiales", "Por Definir Catastro"]


for anio in Anios[1:]:
    for munpio in [f for f in os.listdir(ruta_input/anio) if f.lower().endswith('.parquet')]:
        
        Temporal = pd.read_parquet(ruta_input/anio/munpio)

        # Conversión una sola vez
        mun_str  = Temporal["MUNICIPIO"].astype(str)
        pred_str = Temporal["NUMERO_PREDIAL_NACIONAL"].astype(str)
        destino  = Temporal["DESTINO_ECONOMICO"]
        vigencia = pd.to_numeric(Temporal["VIGENCIA"], errors="coerce").fillna(pd.NA).astype("Int64")

        # Test1
        Test1 = (
            (mun_str.dropna().str.len() == 3).all() and
            (mun_str.dropna().nunique() == 1)
        )

        # Test2
        pred_str_nonnull = pred_str.dropna()
        Test2 = (
            (pred_str_nonnull.str.len() == 30).all() and
            (pred_str_nonnull.str[:2] == "05").all() and
            (pred_str_nonnull.str[:2].nunique() == 1) and
            (pred_str_nonnull.str[2:5] == mun_str.dropna()).all()
        )

        # Test3
        Test3 = destino.dropna().isin(Destinos).all()

        # Test4
        Test4 = (vigencia.dropna() == int(anio)).all()

        # Test5
        Temporal["DERECHO"] = pd.to_numeric(Temporal["DERECHO"], errors="coerce")
        Test5 = (
            Temporal.dropna(subset=["DERECHO", "FICHA"])
            .groupby("FICHA", observed=True)["DERECHO"]
            .sum()
            .eq(1)
            .all()
        )

        # Test6-9
        Temporal["AREA_TERRENO"] = pd.to_numeric(Temporal["AREA_TERRENO"], errors="coerce")
        Temporal["AREA_CONSTRUIDA"] = pd.to_numeric(Temporal["AREA_CONSTRUIDA"], errors="coerce")
        Temporal["AVALUO_CONSTRUCCION"] = pd.to_numeric(Temporal["AVALUO_CONSTRUCCION"], errors="coerce")
        Temporal["AVALUO_TERRENO"] = pd.to_numeric(Temporal["AVALUO_TERRENO"], errors="coerce")

        Test6 = ~(
            (Temporal["AREA_TERRENO"].notna()) &
            (Temporal["AVALUO_TERRENO"].notna()) &
            (Temporal["AREA_TERRENO"] > 0) &
            (Temporal["AVALUO_TERRENO"] == 0)
        ).any()

        Test7 = ~(
            (Temporal["AREA_CONSTRUIDA"].notna()) &
            (Temporal["AVALUO_CONSTRUCCION"].notna()) &
            (Temporal["AREA_CONSTRUIDA"] > 0) &
            (Temporal["AVALUO_CONSTRUCCION"] == 0)
        ).any()

        Test8 = ~(
            (Temporal["AREA_TERRENO"].notna()) &
            (Temporal["AVALUO_TERRENO"].notna()) &
            (Temporal["AREA_TERRENO"] == 0) &
            (Temporal["AVALUO_TERRENO"] > 0)
        ).any()

        Test9 = ~(
            (Temporal["AREA_CONSTRUIDA"].notna()) &
            (Temporal["AVALUO_CONSTRUCCION"].notna()) &
            (Temporal["AREA_CONSTRUIDA"] == 0) &
            (Temporal["AVALUO_CONSTRUCCION"] > 0)
        ).any()

        # Test10
        Test10 = {"Urbana", "Rural"}.issubset(set(Temporal["ZONA"].dropna()))

        if not (Test1 and Test2 and Test3 and Test4 and Test5 and Test6 and Test7 and Test8 and Test9):
            Resultado["AÑO"].append(anio)
            Resultado["CODIGO"].append(munpio.replace(".parquet", ""))
            Resultado["TEST1"].append(Test1)
            Resultado["TEST2"].append(Test2)
            Resultado["TEST3"].append(Test3)
            Resultado["TEST4"].append(Test4)
            Resultado["TEST5"].append(Test5)
            Resultado["TEST6"].append(Test6)
            Resultado["TEST7"].append(Test7)
            Resultado["TEST8"].append(Test8)
            Resultado["TEST9"].append(Test9)
            Resultado["TEST10"].append(Test10)








Resultados_NA = {
    "AÑO":[],
    "CODIGO":[],
    "MUNICIPIO": [],
    "NUMERO_PREDIAL_NACIONAL": [],
    "FICHA": [],
    "MATRICULA":[],
    "TIPO_DOCUMENTO": [],
    "NUMERO_DOCUMENTO": [],
    "DESTINO_ECONOMICO": [],
    "ZONA": [],
    "AREA_TERRENO": [],
    "AREA_CONSTRUIDA": [],
    "AVALUO_CONSTRUCCION": [],
    "AVALUO_TERRENO": [],
    "AVALUO": [],
    "VIGENCIA": [],
    "DERECHO": [],
    "GRAVABLE": [],
    "AUTOESTIMACION": []
}

for anio in Anios[1:]:
    for munpio in [f for f in os.listdir(ruta_input/anio) if f.lower().endswith('.parquet')]:
        Temporal = pd.read_parquet(ruta_input/anio/munpio)
        Temporal.isna().sum(axis=1)
        Resultados_NA["AÑO"].append(anio)
        Resultados_NA["CODIGO"].append(re.sub(r'\.parquet', '', munpio))
        Salida=Temporal.isna().sum(axis=0)
        for xxx in Salida.keys():
            Resultados_NA[xxx].append(Salida[xxx])
            
Resultado=pd.DataFrame(Resultado)
Resultados_NA=pd.DataFrame(Resultados_NA)           
        
Resultado[['TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6',
       'TEST7', 'TEST8', 'TEST9', 'TEST10']]=np.where(Resultado[['TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6',
       'TEST7', 'TEST8', 'TEST9', 'TEST10']],1,0)        
        
        
                                                                 
datos=pd.read_excel("C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Documentación/Priorización.xlsx")
datos["Codigo"] = datos["Codigo"].astype(str).str.zfill(5)
Resultados_NA = Resultados_NA.merge(datos,right_on="Codigo",left_on="CODIGO", how="left")
Resultado = Resultado.merge(datos, right_on="Codigo", left_on="CODIGO", how="left")




Orden1=['Codigo','MUNICIPIO','GRUPO','PROCESO ','AÑO', 'TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6','TEST7', 'TEST8', 'TEST9', 'TEST10']
Orden2=['Codigo','MUNICIPIO_y', 'GRUPO','PROCESO ','AÑO', 'MUNICIPIO_x', 'NUMERO_PREDIAL_NACIONAL', 'FICHA',
       'MATRICULA', 'TIPO_DOCUMENTO', 'NUMERO_DOCUMENTO', 'DESTINO_ECONOMICO',
       'ZONA', 'AREA_TERRENO', 'AREA_CONSTRUIDA', 'AVALUO_CONSTRUCCION',
       'AVALUO_TERRENO', 'AVALUO', 'VIGENCIA', 'DERECHO', 'GRAVABLE',
       'AUTOESTIMACION' ]


Resultado[Orden1].to_excel(BASE_DIR/"Inconsistencias.xlsx",index=False)
Resultados_NA[Orden2].to_excel(BASE_DIR/"Datos_faltantes.xlsx",index=False)        
