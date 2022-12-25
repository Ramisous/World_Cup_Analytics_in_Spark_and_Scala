
spark.read.textFile("../../data/WorldCupMatches.csv").createOrReplaceTempView("worldCupMatches");

// Create a database to store the tables

spark.sql("drop database if exists sparkdatalake cascade")
spark.sql("create database sparkdatalake");

spark.sql(""" Select 

split(value,':::')[0] as Year,
split(value,':::')[1] as Datetime,
split(value,':::')[2] as Stage,
split(value,':::')[3] as Stadium,
split(value,':::')[4] as City,
split(value,':::')[5] as Home_Team_Name,
split(value,':::')[6] as Home_Team_Goals,
split(value,':::')[7] as Away_Team_Goals,
split(value,':::')[8] as Away_Team_Name,
split(value,':::')[9] as Win_conditions,
split(value,':::')[10] as Attendance,
split(value,':::')[11] as Half_time_Home_Goals,
split(value,':::')[12] as Half_time_Away_Goals,
split(value,':::')[13] as Referee,
split(value,':::')[14] as Assistant_1,
split(value,':::')[15] as Assistant_2,
split(value,':::')[16] as RoundID,
split(value,':::')[17] as MatchID,
split(value,':::')[18] as Home_Team_Initials,
split(value,':::')[19] as Away_Team_Initials

from worldCupMatches """).write.mode("overwrite").saveAsTable("sparkdatalake.worldCupMatches");

System.exit(0) 
