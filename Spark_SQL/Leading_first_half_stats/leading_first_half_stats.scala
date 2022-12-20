spark.read.parquet("../sparkdatalake/spark-warehouse/sparkdatalake.db/worldCupMatches/*.parquet").createOrReplaceTempView("worldCupMatches");

spark.sql(""" select 
case when Half_time_Home_Goals > Half_time_Away_Goals and Home_Team_Goals > Away_Team_Goals then 'win after leading'
     when Half_time_Home_Goals < Half_time_Away_Goals and Home_Team_Goals < Away_Team_Goals then 'win after leading'
     when Half_time_Home_Goals <> Half_time_Away_Goals and Home_Team_Goals = Away_Team_Goals then 'draw after leading'
     when Half_time_Home_Goals > Half_time_Away_Goals and Home_Team_Goals < Away_Team_Goals then 'lose after leading'
     when Half_time_Home_Goals < Half_time_Away_Goals and Home_Team_Goals > Away_Team_Goals then 'lose after leading'
     else 'first half draw' end full_match_status
from worldCupMatches """).createOrReplaceTempView("status_table")

spark.sql(""" select full_match_status, 
       round(count(full_match_status) / (select count(*) from status_table where full_match_status <> 'first half draw') * 100 ,2) as percentage
from status_table
where full_match_status <> 'first half draw'
group by full_match_status
order by percentage desc """).repartition(1).write.format("csv").option("header","true").save("result")

System.exit(0)


 



 

