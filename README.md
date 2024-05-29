# ETL-analyst-movie-imdb


# Objective
[Data source](https://www.kaggle.com/datasets/amanbarthwal/imdb-movies-data)

* Retrive count title movie for kids in certificate 7 and certificate 12 with interval year 2000 to 2009
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
select year,count(title) as title_movie_for_kid 
from movie_list 
where certificate= '7' AND certificate ='12' or year>='2000' AND year<='2009'
group by year
)
select * FROM movie_kid_count </pre>
|year|title_movie_for_kid|
|----|-------------------|
|2008|112|
|2009|106|
|2003|64|
|2005|147|
|2000|39|
|2007|170|
|2002|184|
|2004|105|
|2001|101|
|2006|180|

### total  movie
<pre>select sum(title_movie_for_kid) as total_movie_kid FROM movie_kid_count 
</pre>
|total_movie_kid|
|---------------|
|1208|


