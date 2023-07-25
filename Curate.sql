
create schema ingest;
use schema ingest;


CREATE or replace TABLE ingest.m_table (
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
-------------------------------------------------------

CREATE or replace TABLE ingest.details (
  id INT ,
  language VARCHAR(255),
  title VARCHAR(255),
  director VARCHAR(255),
  tagline VARCHAR(255),
  cast VARCHAR(255),
  storyline STRING,
  FOREIGN KEY (id) REFERENCES m_table(id)
);
INSERT INTO ingest.details (id, language, title, director, tagline, cast, storyline)
SELECT 
  j_d:id::INT,
  j_d:details[0]:language::VARCHAR,
  j_d:details[0]:title::VARCHAR,
  j_d:details[0]:director::VARCHAR,
  j_d:details[0]:tagline::VARCHAR,
  j_d:details[0]:cast::VARCHAR,
  j_d:details[0]:storyline::STRING
FROM ing.raw_tab;
select * from ingest.details;
-----------------------------------------------------------------------

CREATE or replace TABLE ingest.collections (
  id INT ,
  name VARCHAR(255),
  slug VARCHAR(255),
  movies VARCHAR(255),
  watches INT,
  duration INT,
  active_screens INT,
 FOREIGN KEY (id) REFERENCES m_table(id)
);
INSERT INTO ingest.collections (id, name, slug, movies, watches, duration, active_screens)
SELECT 
  j_d:collections[0]:id::INT,
  j_d:collections[0]:name::VARCHAR,
  j_d:collections[0]:slug::VARCHAR,
  j_d:collections[0]:movies::VARCHAR,
  j_d:watches::INT,
  j_d:duration::INT,
  j_d:active_screens::INT
FROM ing.raw_tab;
select * from ingest.collections;

---------------------------------------------------------------------
CREATE or replace TABLE ingest.vote_score (
  id INT PRIMARY KEY,
  avg INT,
  score INT,
  total INT
 
);
INSERT INTO ingest.vote_score (id, avg, score, total)
SELECT 
  j_d: id::INT,
  j_d:vote_score:avg::INT,
  j_d:vote_score:score::INT,
  j_d:vote_score:total::INT
FROM ing.raw_tab;
select * from ingest.vote_score;
---------------------------------------------------------------------
CREATE or replace TABLE ingest.fav_movie (
  id INT,
  star BOOLEAN,
  follow BOOLEAN,
  watched BOOLEAN,
  stars INT

);
INSERT INTO ingest.fav_movie (id, star, follow, watched, stars)
SELECT 
  j_d:id::INT,
  j_d:fav_movie:star::BOOLEAN,
  j_d:fav_movie:follow::BOOLEAN,
  j_d:fav_movie:watched::BOOLEAN,
  j_d:stars::INT
FROM ing.raw_tab;
select * from ingest.fav_movie;
-------------------------------------------------------------------------------------------
CREATE  or replace TABLE ingest.images ( 
  id INT,
  order_n INT,
  type VARCHAR(255)
);

INSERT INTO ingest.images (id,order_n, type)
SELECT 
  j_d:images[0]:id::INT,
  j_d:images[0]:order::INT,
  j_d:images[0]:type::VARCHAR
  
FROM raw_tab ;

select * from ingest.images;
-----------------------------------------------------------------------------------------------------
CREATE  or replace TABLE ingest.feeders (
  id INT,
  name1 VARCHAR(255),
  url1 VARCHAR(255),
  name2 VARCHAR(255),
  url2 VARCHAR(255)
);
INSERT INTO ingest.feeders (id, name1, url1 , name2, url2)
SELECT 
  j_d:id::INT,
  j_d:feeders[0]:name::VARCHAR,
  j_d:feeders[0]:url::VARCHAR,
  j_d:feeders[1]:name::VARCHAR,
  j_d:feeders[1]:url::VARCHAR
  FROM raw_tab ;
  
select * from ingest.feeders;
