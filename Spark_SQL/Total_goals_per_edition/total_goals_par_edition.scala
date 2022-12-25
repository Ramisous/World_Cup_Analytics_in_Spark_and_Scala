spark.read.parquet("../sparkdatalake/spark-warehouse/sparkdatalake.db/worldCupMatches/*.parquet").createOrReplaceTempView("worldCupMatches");

spark.sql(""" 
select Year as edition, cast(sum(Home_Team_Goals) + sum(Away_Team_Goals) as integer) as total_goals
from worldCupMatches
group by Year
order by total_goals desc """).repartition(1).write.format("csv").option("header","true").save("result")

System.exit(0)
