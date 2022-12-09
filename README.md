# PI01

Este es el primer proyecto individual de la cohorte 5 de Soy Henry. 
En él realicé la limpieza y carga de datos provistos por los profesores del curso a través de un repositorio en el cual se explicaba las consignas del proyecto a realizar.

## Organización del proyecto

Organicé el trabajo separando en 3 carpetas lo más relevante: api, db y etl. En cada una de ellas se encuentra un archivo Dockerfile con las importaciones necesarias para cada parte del
proyecto. Además un docker compose donde organizo las dependencias de la api, db y el proceso de etl.

## Ingesta de datos

En el archivo etl.py utilicé la librería Pandas para leer los archivos: tres archivos csv y un json. Luego, creé un diccionario donde fui almacenando la información teniendo en cuenta no repetir titulos,
y corrigiendo errores que surgían en algunas columnas. Por ejemplo: quitar "interviews with" de la columna referida al cast de un titulo.
Luego, realicé la conexión a MySql y con la base de datos creada para comenzar a ingestar la data en las tablas creadas: films, platforms, categories, countries, directors, actors, films_x_categories,
films_x_platforms, films_x_actors, films_x_countries y films_x_directors.

## Creación de api

En el archivo Dockerfile de esta sección importé mysql-connector-python,Pydantic,Uvicorn y Fastapi, utilizados en el archivo api.py donde creé la api y realicé las queries requeridas para el proyecto.
