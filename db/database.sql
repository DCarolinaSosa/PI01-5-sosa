DROP DATABASE db;
CREATE DATABASE db;
use db;

CREATE TABLE films (
    id_film int not null AUTO_INCREMENT,
    film_type varchar(30),
    title varchar (200),
    release_year int not null,
    rating varchar (50),
    duration int,
    duration_type varchar (20),
    film_description TEXT,
    PRIMARY KEY (id_film)
);

CREATE TABLE platforms (
    id_platform int not null AUTO_INCREMENT,
    platform_name varchar(100),
    PRIMARY KEY (id_platform)
);

CREATE TABLE categories (
    id_category int not null AUTO_INCREMENT,
    category_name varchar(100),
    PRIMARY KEY(id_category)
);

CREATE TABLE countries (
    id_country int not null AUTO_INCREMENT,
    country_name varchar(100),
    PRIMARY KEY(id_country)
);

CREATE TABLE directors (
    id_director int not null AUTO_INCREMENT,
    director_name varchar(200),
    PRIMARY KEY(id_director)
);

CREATE TABLE actors (
    id_actor int not null AUTO_INCREMENT,
    actor_name varchar(100),
    PRIMARY KEY (id_actor)
);

CREATE TABLE films_x_categories (
    id_film int not null,
    id_category int not null,
    FOREIGN KEY (id_film) REFERENCES films(id_film),
    FOREIGN KEY (id_category) REFERENCES categories(id_category)
);

CREATE TABLE films_x_platforms (
    id_film int not null,
    id_platform int not null,
    date_added varchar(50),
    FOREIGN KEY (id_film) REFERENCES films(id_film),
    FOREIGN KEY (id_platform) REFERENCES platforms(id_platform)
);

CREATE TABLE films_x_actors (
    id_film int not null,
    id_actor int not null,
    FOREIGN KEY (id_film) REFERENCES films(id_film),
    FOREIGN KEY (id_actor) REFERENCES actors(id_actor)
);

CREATE TABLE films_x_countries (
    id_film int not null,
    id_country int not null,
    FOREIGN KEY (id_film) REFERENCES films(id_film),
    FOREIGN KEY (id_country) REFERENCES countries(id_country)
);

CREATE TABLE films_x_directors (
    id_director int not null,
    id_film int not null,
    FOREIGN KEY (id_film) REFERENCES films(id_film),
    FOREIGN KEY (id_director) REFERENCES directors(id_director)
);
