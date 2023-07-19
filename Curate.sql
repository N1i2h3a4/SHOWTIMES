CREATE or replace TABLE m_table (
  id INT primary key,
  title VARCHAR(255),
  language VARCHAR(255),
  release_date DATE
);
INSERT INTO m_table (id, title, language, release_date)
SELECT 
  j_d:id::INT,
  j_d:title::VARCHAR,
  j_d:language::VARCHAR,
  j_d:release_date::DATE
FROM raw_tab;
SELECT * FROM m_table;


CREATE or replace TABLE details (
  id INT PRIMARY KEY,
  language VARCHAR(255),
  title VARCHAR(255),
  director VARCHAR(255),
  tagline VARCHAR(255),
  cast VARCHAR(255),
  storyline STRING,
  FOREIGN KEY (id) REFERENCES m_table(id)
);
INSERT INTO details (id, language, title, director, tagline, cast, storyline)
SELECT 
  j_d:id::INT,
  j_d:language::VARCHAR,
  j_d:title::VARCHAR,
  j_d:director::VARCHAR,
  j_d:tagline::VARCHAR,
  j_d:cast::VARCHAR,
  j_d:storyline::STRING
FROM raw_tab;
select * from details;


CREATE or replace TABLE collections (
  id INT PRIMARY KEY,
  name VARCHAR(255),
  slug VARCHAR(255),
  movies VARCHAR(255),
  watches INT,
  duration INT,
  active_screens INT,
  FOREIGN KEY (id) REFERENCES m_table(id)
);
INSERT INTO collections (id, name, slug, movies, watches, duration, active_screens)
SELECT 
  j_d:id::INT,
  j_d:name::VARCHAR,
  j_d:slug::VARCHAR,
  j_d:movies::VARCHAR,
  j_d:watches::INT,
  j_d:duration::INT,
  j_d:active_screens::INT
FROM raw_tab;
select * from collections;


CREATE or replace TABLE vote_score (
  id INT,
  avg INT,
  score INT,
  total INT,
  FOREIGN KEY (id) REFERENCES collections(id)
);
INSERT INTO vote_score (id, avg, score, total)
SELECT 
  j_d:id::INT,
  j_d:vote_score:avg::INT,
  j_d:vote_score:score::INT,
  j_d:vote_score:total::INT
FROM raw_tab;
select * from vote_score;

CREATE or replace TABLE fav_movie (
  id INT,
  star BOOLEAN,
  follow BOOLEAN,
  watched BOOLEAN,
  stars INT,
  FOREIGN KEY (id) REFERENCES collections(id)
);
INSERT INTO fav_movie (id, star, follow, watched, stars)
SELECT 
  j_d:id::INT,
  j_d:fav_movie:star::BOOLEAN,
  j_d:fav_movie:follow::BOOLEAN,
  j_d:fav_movie:watched::BOOLEAN,
  j_d:fav_movie:stars::INT
FROM raw_tab;
select * from fav_movie;


CREATE  or replace TABLE images ( 
  id INT PRIMARY KEY,
  order_n INT,
  url VARCHAR(255),
  type VARCHAR(255),
  thumbnail VARCHAR(255),
  collection_id INT,
  FOREIGN KEY (collection_id) REFERENCES collections(id)
);
INSERT INTO images (id,order_n, url, type, thumbnail, collection_id)
SELECT 
  j_d:id::INT,
  j_d:order::INT,
  j_d:url::VARCHAR,
  j_d:type::VARCHAR,
  j_d:thumbnail::VARCHAR,
  j_d:collection_id::INT
FROM raw_tab ;
select * from images;

CREATE  or replace TABLE feeders (
  id INT,
  name VARCHAR(255),
  url VARCHAR(255)
);
INSERT INTO feeders (id, name, url)
SELECT 
  j_d:id::INT,
  j_d:name::INT,
  j_d:url::VARCHAR
  FROM raw_tab ;
select * from feeders;
