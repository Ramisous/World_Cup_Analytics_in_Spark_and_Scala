![GitHub](https://img.shields.io/github/license/Thomas-George-T/MoviesLens-Analytics-in-Spark-and-Scala?style=flat)
![GitHub top language](https://img.shields.io/github/languages/top/Thomas-George-T/MoviesLens-Analytics-in-Spark-and-Scala?style=flat)
![GitHub language count](https://img.shields.io/github/languages/count/Thomas-George-T/MoviesLens-Analytics-in-Spark-and-Scala?style=flat)
![GitHub last commit](https://img.shields.io/github/last-commit/Thomas-George-T/MoviesLens-Analytics-in-Spark-and-Scala?style=flat)
![ViewCount](https://views.whatilearened.today/views/github/Thomas-George-T/Movies-Analytics-in-Spark-and-Scala.svg?cache=remove)

# Overview
Solving analytical questions on the [World Cup Matches Dataset](https://www.kaggle.com/datasets/abecklas/fifa-world-cup) using Spark and Scala. This features the use of Spark RDD, Spark SQL and Spark Dataframes executed on Spark-Shell (REPL) using Scala API. We aim to draw useful insights about world cup history by leveraging different forms of Spark APIs.

# Table of Contents
* [Components](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Major-Components)
* [Environment](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Environment)
* [Installation steps](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Installation-steps)
* [Analytical Queries](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Analytical-Queries)
	- [Spark RDD](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Spark-RDD)
	- [Spark SQL](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Spark-SQL)
	- [Spark DataFrames](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Spark-DataFrames)
	- [Miscellaneous](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Miscellaneous)
* [Mentions](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Mentions)
* [License](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#License)

# Major Components

<p align="center">
	<a href="#">
		<img src="https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg" alt="Apache Spark Logo" title="Apache Spark" width=275 hspace=80 />
	</a>
	<a href="#">
		<img src="https://upload.wikimedia.org/wikipedia/commons/3/39/Scala-full-color.svg" alt="Scala" title="Scala" width ="275" />
	</a>
</p>

# Environment
* Linux
* Hadoop 3.2.4
* Spark 3.3.1
* Scala 2.12

# Installation steps

1. Simply clone the repository
	```
	git clone https://github.com/Thomas-George-T/Movies-Analytics-in-Spark-and-Scala.git
	```
2. In the repo, Navigate to Spark RDD, Spark SQL or Spark Dataframe locations as needed.

3. Run the execute script to view results
	```
	sh execute.sh
	```
4. The `execute.sh` will pass the scala code through spark-shell and then display the findings in the terminal from the results folder.

# Analytical Queries

## Spark SQL
- [Create table for WorldCupMatches.csv](/Spark_SQL/sparkdatalake/): Saving Table with Spark SQL as Parquet file
- [Which team has participated in every edition of the World Cup?](/Spark_SQL/Appearance_all_editions/)
- [Which teams have won the most games in the history of the World Cup?](/Spark_SQL/Top_games_winner/)
- [which stadiums have hosted games in two different editions?](/Spark_SQL/Stadium_hosting_2_editions/)
- [Order the world cup editions according to the total number of goals scored.](Spark_SQL/Total_goals_per_edition)
- [which team has won the world cup in each edition?](Spark_SQL/World_cup_winners/) 
- [What are the percentages of winning and losing games after leading in the first half?](Spark_SQL/Leading_first_half_stats/)

## Spark RDD
- [What are the top 10 most viewed movies?](/Spark_RDD/Top_10_Most_Viewed_Movies/)
- [What are the distinct list of genres available?](/Spark_RDD/Distinct_Genres/)
- [How many movies for each genre?](Spark_RDD/Movies_in_each_genre/)
- [How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?](Spark_RDD/Movies_starting_with_Letters_or_Numbers/)
- [List the latest released movies](Spark_RDD/Latest_movies/)

## Spark DataFrames
- [Prepare Movies data: Extracting the Year and Genre from the Text](Spark_DataFrames/prepare_movies_dat)
- [Prepare Users data: Loading a double delimited csv file](Spark_DataFrames/prepare_users_dat)
- [Prepare Ratings data: Programmatically specifying a schema for the dataframe](Spark_DataFrames/prepare_ratings_dat)

## Miscellaneous
- [Import Data from URL: Scala](/Miscellaneous/Import-File-From-URL)
- [Save table without defining DDL in Hive](/Miscellaneous/Save-Table-Without-Explicit-DDL)
- [Broadcast Variable example](/Miscellaneous/Broadcast-variable)
- [Accumulator example](/Miscellaneous/Accumulator-Example)
- [Databricks Community Edition](https://community.cloud.databricks.com/login.html)

_**Note:** The results were collected and repartitioned into the same text file: This is not a recommended practice since performance is highly impacted but it is done here for the sake of readability._

# Mentions
This project was featured on [Data Machina Issue #130](https://www.getrevue.co/profile/datamachina/issues/data-machina-issue-130-112552) listed at number 3 under ScalaTOR. Thank you for the listing

# License
This repository is licensed under Apache License 2.0 - see [License](LICENSE.md) for more details
