spark.read.parquet("../sparkdatalake/spark-warehouse/sparkdatalake.db/worldCupMatches/*.parquet").createOrReplaceTempView("worldCupMatches");

spark.sql("""select distinct Home_Team_Name as team, Year
from worldCupMatches
union 
select distinct Away_Team_Name as team, Year
from worldCupMatches""").createOrReplaceTempView("Participations");

spark.sql(""" select team, count(Year) as participations_number
from Participations
group by team
having count(Year) = (select count(distinct Year) from worldCupMatches)
""").repartition(1).write.format("csv").option("header","true").save("result")

System.exit(0)





 



 

