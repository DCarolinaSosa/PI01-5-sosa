import pandas as pd
import mysql.connector as mysql
import sys

def is_not_null(value):
  return value and value != None and value != "NaN" and value != "nan" and str(value) != "nan"

def insert_new_film_cleared(row, platform_name):
  duration = None
  duration_type = None
  if is_not_null(row["duration"]):
    duration = int(float(str(row["duration"]).lower().split(" ")[0].strip()))
    duration_type = str(row["duration"]).lower().split(" ")[1].strip()
  return {
    "title": str(row["title"]).lower().strip(),
    "type": str(row["type"]).lower().strip(),
    "release_year": int(row["release_year"]),
    "rating": str(row["rating"]).upper().strip(),
    "duration": duration,
    "duration_type": "seasons" if duration_type == "season" else duration_type,
    "description": row["description"],
    "platforms": [
        { "name": platform_name, "date_added": str(row["date_added"]).lower().strip()}
    ],
    "directors": [y for y in [x.strip() for x in str(row["director"]).lower().split(",")] if y and not("validcapi" in y)] if row["director"] else [],
    "countries": [y for y in [x.strip() for x in str(row["country"]).lower().split(",")] if y] if row["country"] else [],
    "actors": [y for y in [x.strip() for x in str(row["cast"]).lower().split(",")] if y] if row["cast"] else [],
    "categories": [y for y in [x.strip() for x in str(row["listed_in"]).lower().split(",")] if y] if row["listed_in"] else [],
  }

def add_new_films_dataset(platform_name, data_frame, base_dictionary, films_count):
  total_count_in_platform = 0
  repet_count = 0
  new_films_count = 0
  for i in range(len(data_frame)):
    row = data_frame.iloc[i]
    try:
      film = base_dictionary[row["title"].lower().strip()]
      repet_count += 1
      try:
        film["platforms"].append({ "name": platform_name, "date_added": row["date_added"]})
        if is_not_null(row["release_year"]):
          film["duration"] = int(row["release_year"])
        if is_not_null(row["duration"]):
          film["duration"] = int(float(str(row["duration"]).lower().split(" ")[0].strip()))
          duration_type = str(row["duration"]).lower().split(" ")[1].strip()
          film["duration_type"] = "seasons" if duration_type == "season" else duration_type
        if is_not_null(row["rating"]):
          film["rating"] = str(row["rating"]).upper().strip()
        if is_not_null(row["cast"]):
          film["actors"] = [y for y in [x.strip() for x in str(row["cast"]).lower().replace("interviews with: ", "").split(",")] if y]
        if is_not_null(row["director"]):
          film["directors"] = [y for y in [x.strip() for x in str(row["director"]).lower().split(",")] if y and not("validcapi" in y)]
        if is_not_null(row["country"]):
          film["countries"] = [y for y in [x.strip() for x in str(row["country"]).lower().split(",")] if y]
        if is_not_null(row["listed_in"]):
          film["categories"] = [y for y in [x.strip() for x in str(row["listed_in"]).lower().split(",")] if y]
      except Exception as e:
        print(f"error: {e}")
    except:
      base_dictionary.update([(row["title"].lower().strip(), insert_new_film_cleared(row, platform_name))])
      films_count += 1
      new_films_count += 1
    total_count_in_platform += 1
  print("---")
  print(f"{platform_name}: {total_count_in_platform} total")
  print(f"{platform_name}: {repet_count} repetidos")
  print(f"{platform_name}: {new_films_count} nuevos")
  print(f"{platform_name}: {films_count} acumulados")

## LOAD ##

url1 = "https://raw.githubusercontent.com/HX-FAshur/PI01_DATA05/main/Datasets/amazon_prime_titles.csv"
data_amazon_prime = pd.read_csv(url1)
films_count = 0
films_dictionary = {}
for index in range(len(data_amazon_prime)):
  films_count += 1
  row = data_amazon_prime.iloc[index]
  films_dictionary[row["title"].lower().strip()] = insert_new_film_cleared(row, "amazon prime")

print(f"amazon prime: {films_count} total")
print(f"films_dictionary: {len(films_dictionary.keys())} total")

url2 = "https://raw.githubusercontent.com/HX-FAshur/PI01_DATA05/main/Datasets/disney_plus_titles.csv"
data_disney_plus = pd.read_csv(url2)
add_new_films_dataset("disney plus", data_disney_plus, films_dictionary, films_count)
print(f"films_dictionary: {len(films_dictionary.keys())} total")

url3 = "https://raw.githubusercontent.com/HX-FAshur/PI01_DATA05/main/Datasets/hulu_titles.csv"
data_hulu = pd.read_csv(url3)
add_new_films_dataset("hulu", data_hulu, films_dictionary, films_count)
print(f"films_dictionary: {len(films_dictionary.keys())} total")

url4 = "https://raw.githubusercontent.com/HX-FAshur/PI01_DATA05/main/Datasets/netflix_titles.json"
data_netflix = pd.read_json(url4)
add_new_films_dataset("netflix", data_netflix, films_dictionary, films_count)
print(f"films_dictionary: {len(films_dictionary.keys())} total")

print("---")
print("START MYSQL CONNECTION")

cursor = None
try:
    print('trying to connect...')
    connection = mysql.connect(
        host='db',
        user='root',
        password='root',
        database='db',
        port=3306
    )
    print('connection ready')
    cursor = connection.cursor()
except Exception as e:
    print("Error in the connection: " + str(e))
    sys.exit()

def null_if_not_have_info(value):
  return None if (value == "" or str(value).lower() == "nan") else value

print("start to insert...")
platforms_dictionary = {}
countries_dictionary = {}
actors_dictionary = {}
categories_dictionary = {}
directors_dictionary = {}
for key in films_dictionary.keys():
  row = films_dictionary[key]
  query = "INSERT INTO films (film_type, title, release_year, rating, duration, duration_type, film_description) VALUES (%s, %s, %s, %s, %s, %s, %s)"
  values = (
    null_if_not_have_info(row["type"]), 
    null_if_not_have_info(row["title"]), 
    null_if_not_have_info(row["release_year"]), 
    null_if_not_have_info(row["rating"]), 
    null_if_not_have_info(row["duration"]), 
    null_if_not_have_info(row["duration_type"]), 
    null_if_not_have_info(row["description"])
  )
  cursor.execute(query, values)
  connection.commit()
  id_film_inserted = cursor.lastrowid

  for platform in row["platforms"]:
    try:
      if platforms_dictionary[platform["name"]]:
        id_platform = platforms_dictionary[platform["name"]]
        query = "INSERT INTO films_x_platforms (id_film, id_platform, date_added) VALUES (%s, %s, %s)"
        values = (
          null_if_not_have_info(id_film_inserted), 
          null_if_not_have_info(id_platform), 
          null_if_not_have_info(platform["date_added"]), 
        )
        cursor.execute(query, values)
        connection.commit()
    except:
      query = "INSERT INTO platforms (platform_name) VALUES (%s)"
      values = (null_if_not_have_info(platform["name"]),)
      cursor.execute(query, values)
      connection.commit()
      id_platform_inserted = cursor.lastrowid
      platforms_dictionary[platform["name"]] = id_platform_inserted
      query = "INSERT INTO films_x_platforms (id_film, id_platform, date_added) VALUES (%s, %s, %s)"
      values = (
        id_film_inserted, 
        id_platform_inserted, 
        null_if_not_have_info(platform["date_added"]), 
      )
      cursor.execute(query, values)
      connection.commit()

  for country in row["countries"]:
    try:
      if countries_dictionary[country]:
        id_country = countries_dictionary[country]
        query = "INSERT INTO films_x_countries (id_film, id_country) VALUES (%s, %s)"
        values = (
          id_film_inserted, 
          id_country, 
        )
        cursor.execute(query, values)
        connection.commit()
    except:
      query = "INSERT INTO countries (country_name) VALUES (%s)"
      values = (null_if_not_have_info(country),)
      cursor.execute(query, values)
      connection.commit()
      id_country_inserted = cursor.lastrowid
      countries_dictionary[country] = id_country_inserted
      query = "INSERT INTO films_x_countries (id_film, id_country) VALUES (%s, %s)"
      values = (
        id_film_inserted, 
        id_country_inserted, 
      )
      cursor.execute(query, values)
      connection.commit()

  for actor in row["actors"]:
    try:
      if actors_dictionary[actor]:
        id_actor = actors_dictionary[actor]
        query = "INSERT INTO films_x_actors (id_film, id_actor) VALUES (%s, %s)"
        values = (
          id_film_inserted, 
          id_actor, 
        )
        cursor.execute(query, values)
        connection.commit()
    except:
      query = "INSERT INTO actors (actor_name) VALUES (%s)"
      values = (null_if_not_have_info(actor),)
      cursor.execute(query, values)
      connection.commit()
      id_actor_inserted = cursor.lastrowid
      actors_dictionary[actor] = id_actor_inserted
      query = "INSERT INTO films_x_actors (id_film, id_actor) VALUES (%s, %s)"
      values = (
        id_film_inserted, 
        id_actor_inserted, 
      )
      cursor.execute(query, values)
      connection.commit()

  for director in row["directors"]:
    try:
      if directors_dictionary[director]:
        id_director = directors_dictionary[director]
        query = "INSERT INTO films_x_directors (id_film, id_director) VALUES (%s, %s)"
        values = (
          null_if_not_have_info(id_film_inserted), 
          null_if_not_have_info(id_director), 
        )
        cursor.execute(query, values)
        connection.commit()
    except:
      query = "INSERT INTO directors (director_name) VALUES (%s)"
      values = (null_if_not_have_info(director),)
      cursor.execute(query, values)
      connection.commit()
      id_director_inserted = cursor.lastrowid
      directors_dictionary[director] = id_director_inserted
      query = "INSERT INTO films_x_directors (id_film, id_director) VALUES (%s, %s)"
      values = (
        null_if_not_have_info(id_film_inserted), 
        null_if_not_have_info(id_director_inserted), 
      )
      cursor.execute(query, values)
      connection.commit()

  for category in row["categories"]:
    try:
      if categories_dictionary[category]:
        id_category = categories_dictionary[category]
        query = "INSERT INTO films_x_categories (id_film, id_category) VALUES (%s, %s)"
        values = (
          id_film_inserted, 
          id_category, 
        )
        cursor.execute(query, values)
        connection.commit()
    except:
      query = "INSERT INTO categories (category_name) VALUES (%s)"
      values = (null_if_not_have_info(category),)
      cursor.execute(query, values)
      connection.commit()
      id_category_inserted = cursor.lastrowid
      categories_dictionary[category] = id_category_inserted
      query = "INSERT INTO films_x_categories (id_film, id_category) VALUES (%s, %s)"
      values = (
        id_film_inserted, 
        id_category_inserted, 
      )
      cursor.execute(query, values)
      connection.commit()

print("finish inserted process")
