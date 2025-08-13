Autor: Cristian Daniel Obando Arbeláez.
Objetivo General: Limpiar y estandarizar los R1 desde 2015 al 2024

# Introducción.
Este proyecto está organizado en varios scripts .py que juntos automatizan la gestión, limpieza y organización de datos provenientes de archivos TXT comprimidos en ZIP, divididos en diferentes grupos.
- 00_iniciar.py: Define la carpeta principal del proyecto para que los demás scripts puedan ejecutarse desde ahí sin problema.
- 00_reporte_limpieza_TXT.py: Revisa qué archivos Excel ya están procesados por año y genera un reporte indicando cuáles faltan, basándose en una lista priorizada.
- Scripts de descompresión (01, 03, 05, 07, 09): Cada uno valida y descomprime los archivos ZIP de diferentes grupos (Grupo1, Grupo2, etc.), organizando los archivos extraídos en carpetas ordenadas por año y municipio o carpeta original.
- Scripts de limpieza (02, 04, 06, 08, 10): Cada uno lee los archivos TXT descomprimidos de cada grupo, concatena los datos por municipio y año, limpia y estructura la información, y finalmente exporta los datos consolidados en formato Excel dentro de una carpeta Outputs, también organizada por año.
- Funciones_Compartidas.py: Contiene funciones comunes, en especial cargar_txt, que carga y procesa los archivos TXT delimitados por |, organizando los datos heterogéneos en un solo DataFrame limpio y listo para análisis o exportación.




# Requisitos y Dependencias.
Para ejecutar los scripts de este proyecto, es necesario tener instaladas las siguientes librerías y herramientas en tu entorno de Python:
Librerías de Python
pandas (versión recomendada: >=1.3.0) — para manejo y análisis de datos
numpy (versión recomendada: >=1.20.0) — para operaciones numéricas
openpyxl (versión recomendada: >=3.0.0) — para exportar archivos Excel
pathlib (incluida en Python 3.4+) — para manejo de rutas
re (módulo estándar) — para expresiones regulares
zipfile (módulo estándar) — para manipulación de archivos ZIP


# Orden recomendado de ejecución.
El orden recomendado para ejecutar el código es el siguiente:
1. Descomprimir archivos ZIP organizados por grupos y años.
2. Limpiar y consolidar los archivos TXT de cada grupo.
3. Generar reportes para saber qué datos ya se procesaron y cuáles faltan.
Todo está organizado para trabajar con múltiples años y municipios, manteniendo un orden claro de carpetas y archivos.
Nota: solo es necesario que se respete el orden de los pasos 1 y 2 para cada grupo. Además el paso 3 se hace una vez se hayan realizado los pasos 1 y 2 sobre todos los grupos.


# Scripts

## `00_iniciar.py`

Ubica la carpeta raíz del proyecto y luego la establece como carpeta de trabajo. Es útil para poder correr fagmentos de código de los otros .py

## `00_reporte_limpieza_TXT.py`

Analiza la disponibilidad de archivos Excel por año en Outputs, cruza esa información con una lista priorizada y genera un reporte de archivos faltantes.

## `01_Descomprimir_TXT_Grupo1.py`

Valida y descomprime archivos ZIP desde Inputs/Process data/Historicos OVC TXT hacia Inputs/Route data/Historicos OVC TXT, manteniendo una estructura ordenada por año y municipio.

## `02_Limpieza_TXT_Grupo1.py`

Carga y consolida archivos TXT desde Inputs/Route data/Historicos OVC TXT y guarda resultados en Excel en la carpeta Outputs organizada por año.

## `03_Descomprimir_TXT_Grupo2.py`

Valida, organiza y descomprime archivos ZIP desde Inputs/Process data/TXT Grupo2 hacia Inputs/Route data/TXT Grupo2 en una estructura ordenada por año y municipio.


## `04_Limpieza_TXT_Grupo2.py`

Concatena archivos TXT por municipio y año desde Inputs/Route data/TXT Grupo2  exporta los datos consolidados en Excel dentro de Outputs organizados por año.

## `05_Descomprimir_TXT_Grupo3.py`

Agrupa y descomprime archivos ZIP desde Inputs/Process data/TXT Grupo3 hacia Inputs/Route data/TXT Grupo3, manteniendo una estructura organizada por año y carpeta original.


## `06_Limpieza_TXT_Grupo3.py`

Concatena archivos TXT por municipio y año desde Inputs/Route data/TXT Grupo3  exporta los datos consolidados en Excel dentro de Outputs organizados por año.

## `07_Descomprimir_TXT_Grupo4.py`

Agrupa y descomprime archivos ZIP desde Inputs/Process data/TXT Grupo4 acia Inputs/Route data/TXT Grupo4 manteniendo una estructura organizada por año y carpeta original.


## `08_Limpieza_TXT_Grupo4.py`

Concatena archivos TXT por municipio y año desde Inputs/Route data/TXT Grupo4 y exporta los datos consolidados en Excel dentro de Outputs organizados por año.

## `09_Descomprimir_TXT_Grupo5.py`

Agrupa y descomprime archivos ZIP desde Inputs/Process data/TXT Grupo5 hacia Inputs/Route data/TXT Grupo5, manteniendo una estructura organizada por año y carpeta original.

## `10_Limpieza_TXT_Grupo5.py`

Concatena archivos TXT por municipio y año desde Inputs/Route data/TXT Grupo5 y exporta los datos consolidados en Excel dentro de Outputs organizados por año.


## `Funciones_Compartidas.py` 
Contiene una función cargar_txt. Esta función lee un archivo .txt delimitado por | que contiene datos heterogéneos organizados en tres tipos de registros identificados por un código (1, 2 o 3). Los procesa para tener un solo DataFrame con datos limpios, columnas estandarizadas, y campos calculados, listos para análisis o exportación.



Para mas detalle revise las carpetas con los scripts.
