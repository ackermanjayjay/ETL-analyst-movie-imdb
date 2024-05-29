# ETL-analyst-movie-imdb


# Objective
[Data source](https://www.kaggle.com/datasets/amanbarthwal/imdb-movies-data)

* Retrive title movie for kids in certificate 7 and certificate 12 with interval year 2000 to 2009
* <pre> To be continued</pre>

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


### total  movie for kid
<pre>select sum(movie_kid) as total_movie_kid FROM movie_kid_count 
</pre>
|total_movie_kid|
|---------------|
|6|




