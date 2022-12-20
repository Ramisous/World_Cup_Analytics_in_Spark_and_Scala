spark.read.parquet("../sparkdatalake/spark-warehouse/sparkdatalake.db/worldCupMatches/*.parquet").createOrReplaceTempView("worldCupMatches");

spark.sql(""" select 
case when Home_Team_Goals > Away_Team_Goals then Home_Team_Name
     when Away_Team_Goals > Home_Team_Goals then Away_Team_Name
     end winning_team
     from worldCupMatches """).createOrReplaceTempView("winningTeamTable");

spark.sql(""" select winning_team, count(winning_team) as win_count
from winningTeamTable
group by winning_team 
order by win_count desc 
limit 3""").repartition(1).write.format("csv").option("header","true").save("result")

System.exit(0)