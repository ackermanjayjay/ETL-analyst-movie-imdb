# ETL-analyst-movie-imdb


# Objective
[Data source](https://www.kaggle.com/datasets/amanbarthwal/imdb-movies-data)

* Retrive title movie for kids in certificate 7 and certificate 12 with interval year 2000 to 2009
* Retrive and count best movie for kid all year this data from 1971 - 2021

# Scope
* Analyst data
* Build pipeline service 
* transform data and cleaning data
* load to SQL
* Analyst data

# Result
## **Retrive count title movie for kids in certificate 7 and certificate 12 with interval year 2000 to 2009**
<pre>with movie_kid_count AS(
SELECT year,count(title) movie_kid
FROM movie_list 
WHERE (certificate = '7' OR certificate = '12') 
  AND year BETWEEN '2000' AND '2009'
 group by year
)
select * FROM movie_kid_count </pre>
|title|year|genre|
|-----|----|-----|
|Murder Mystery|2004|Action, Comedy, Crime|
|We Can Be Heroes|2006|Action, Comedy, Drama|
|Jodaeiye Nader az Simin|2000|Drama|
|Storks|2002|Animation, Adventure, Comedy|
|Chicken Little|2009|Animation, Adventure, Comedy|
|My Octopus Teacher|2006|Documentary, Drama|

### Total  movie for kid
<pre>select sum(movie_kid) as total_movie_kid FROM movie_kid_count 
</pre>
|total_movie_kid|
|---------------|
|6|

## Retrive best movie kid with rating > 8
<pre>SELECT title, year, genre, rating
FROM movie_list 
WHERE (certificate = '7' OR certificate = '12') AND rating > 8</pre>
|title|year|genre|rating|
|-----|----|-----|------|
|Jagten|2012|Drama|8.3|
|Jodaeiye Nader az Simin|2000|Drama|8.3|
|My Octopus Teacher|2006|Documentary, Drama|8.1|
|Klaus|2020|Animation, Adventure, Comedy|8.2|

### Total best movie for kid
<pre>SELECT COUNT(title) as count_best_rating_movie_for_kid
FROM movie_list 
WHERE (certificate = '7' OR certificate = '12') AND rating > 8</pre>
|count_best_rating_movie_for_kid|
|-------------------------------|
|4|




