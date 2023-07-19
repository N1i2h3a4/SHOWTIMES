
 # Trend of the movie count released in multiple language per month/year
SELECT
  EXTRACT(YEAR FROM m.release_date) AS year,
  EXTRACT(MONTH FROM m.release_date) AS month,
  COUNT(*) AS movie_count
FROM m_table m
GROUP BY year, month;


#Trend of the movie count release in english only per month/year

SELECT
  EXTRACT(YEAR FROM m.release_date) AS year,
  EXTRACT(MONTH FROM m.release_date) AS month,
  COUNT(*) AS movie_count
FROM m_table m
WHERE m.language = 'en'
GROUP BY year, month;


#create a report with top 4 artists with number of movie release per each year?(also consider unique movie names only)

SELECT
  d.director AS artist,
  EXTRACT(YEAR FROM m.release_date) AS year,
  COUNT(DISTINCT m.title) AS movie_count
FROM m_table m
JOIN details d ON m.id = d.id
GROUP BY artist, year
HAVING COUNT(DISTINCT m.title) > 0
ORDER BY year, movie_count DESC;



#Give me the top 4 month with most number of releases?

SELECT month, movie_count
FROM (
  SELECT
    EXTRACT(MONTH FROM m_table.release_date) AS month,
    COUNT(*) AS movie_count,
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS row_num
  FROM m_table
  GROUP BY month
) subquery
WHERE row_num <= 4;


