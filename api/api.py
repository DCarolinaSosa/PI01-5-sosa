from fastapi import FastAPI
import mysql.connector as mysql
import sys

cursor = None
try:
    print('trying to connect...')
    connection = mysql.connect(
        host='db', # si se levanta local usar localhost, si no, db
        user='root',
        password='root',
        database='db',
        port=3306 # si se levanta local usar 33066, si no, 3306
    )
    print('connection ready')
    cursor = connection.cursor()
except Exception as e:
    print("Error in the connection: " + str(e))
    sys.exit()

app = FastAPI()

@app.get("/get_max_duration/{year}/{platform}/{duration_type}")
def get_max_duration(year, platform, duration_type):
    cursor.execute(f'''
        select f.title from films f
            join films_x_platforms fxp on fxp.id_film = f.id_film
            join platforms p on p.id_platform = fxp.id_platform
        where f.duration_type = "{duration_type}"
            and p.platform_name = "{platform}"
            and f.release_year = "{year}"
        order by f.duration DESC
        LIMIT 1
    ''')
    try:
        result = cursor.fetchall()[0][0]
        return { "title": result }
    except:
        return "not found"

@app.get("/get_count_plataform/{platform}")
def get_max_duration(platform):
    cursor.execute(f'''
        select p.platform_name, f.film_type, COUNT(*) from films f
            join films_x_platforms fxp ON fxp.id_film = f.id_film 
            join platforms p ON fxp.id_platform = p.id_platform
        where p.platform_name  = "{platform}" 
        group by f.film_type
    ''')
    try:
        result = cursor.fetchall()
        print(result)
        return { 
            "platform": platform, 
            f"{result[1][1]}": result[1][2], 
            f"{result[0][1]}": result[0][2] 
        }
    except:
        return "not found"

@app.get("/get_listedin/{category}")
def get_max_duration(category):
    cursor.execute(f'''
        select p.platform_name, COUNT(*) as quantity from films f 
            join films_x_categories fxc ON fxc.id_film = f.id_film 
            join categories c ON fxc.id_category = c.id_category 
            join films_x_platforms fxp ON fxp.id_film = f.id_film 
            join platforms p ON fxp.id_platform = p.id_platform
        where c.category_name = "{category}"
        group by p.platform_name
        order by quantity DESC
        limit 1
    ''')
    try:
        result = cursor.fetchall()[0]
        return { "platform": result[0], "quantity": result[1] }
    except:
        return "not found"
    
@app.get("/get_actor/{platform}/{year}")
def get_max_duration(platform, year):
    cursor.execute(f'''
        select a.actor_name, COUNT(*) as quantity from films f
            join films_x_actors fxa on f.id_film = fxa.id_film
            join actors a on a.id_actor = fxa.id_actor 
            join films_x_platforms fxp ON fxp.id_film = f.id_film 
            join platforms p ON fxp.id_platform = p.id_platform
        where p.platform_name = "{platform}"
        and f.release_year = "{year}"
        group by a.actor_name
        order by quantity DESC
        limit 1
    ''')
    try:
        result = cursor.fetchall()[0]
        print(result)
        return { "platform": platform, "quantity": result[1], "actor_name": result[0] }
    except:
        return "not found"
