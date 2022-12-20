![GitHub language count](https://img.shields.io/github/languages/count/Ramisous/World_Cup_Analytics_in_Spark_and_Scala?color=%23FFA500&logo=github)
![GitHub top language](https://img.shields.io/github/languages/top/Ramisous/World_Cup_Analytics_in_Spark_and_Scala?logo=Github)
![GitHub last commit](https://img.shields.io/github/last-commit/Ramisous/World_Cup_Analytics_in_Spark_and_Scala?logo=Github)

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
* [Mentions](https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala#Mentions)

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
	git clone https://github.com/Ramisous/World_Cup_Analytics_in_Spark_and_Scala.git
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
- [Which edition of the world cup has the highest number of goals scored?](Spark_SQL/Total_goals_per_edition)
- [which team has won the world cup in each edition?](Spark_SQL/World_cup_winners/) 
- [What are the percentages of winning and losing games after leading in the first half?](Spark_SQL/Leading_first_half_stats/)

## Spark RDD
- [Which teams have scored the most goals over all editions?](/Spark_RDD/Most_scoring_teams/)
- [Which countries have the most referees over all editions?](/Spark_RDD/Referee_country/)
- [Which editions have the largest number of total attendance?](Spark_RDD/Greatest_attendances/)
- [Which teams have played the most games over all editions?](Spark_RDD/Most_playing_teams/)

## Spark DataFrames
- [Prepare Match results](Spark_DataFrames/extract_match_details): Extracting the two team names and final score from the csv file
- [Prepare refree table](Spark_DataFrames/prepare_refree_table): Loading the delimited csv file into a Dataframe and specifying the schema programmatically
- [Save table in Hive](Spark_DataFrames/save_to_hive)

# Mentions
This project was featured with [Kaggle](https://www.kaggle.com/) open source datasets. Thank you for the listing.



