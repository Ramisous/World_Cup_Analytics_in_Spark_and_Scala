spark.read.parquet("../sparkdatalake/spark-warehouse/sparkdatalake.db/worldCupMatches/*.parquet").createOrReplaceTempView("worldCupMatches");

spark.sql(""" select * from
(select *, ROW_NUMBER() OVER (PARTITION BY Year order by (SELECT NULL)) as row_num,
count(*) OVER (PARTITION BY Year) as matches_count
from worldCupMatches)
where row_num = matches_count
 """).createOrReplaceTempView("Finals")

spark.sql(""" select year as edition,
case when Home_Team_Goals > Away_Team_Goals then Home_Team_Name
     when Away_Team_Goals > Home_Team_Goals then Away_Team_Name
     when Away_Team_Goals = Home_Team_Goals then SUBSTRING(Win_conditions, 1, LOCATE(' ', Win_conditions,1) - 1)
     end winner
from Finals """).repartition(1).write.format("csv").option("header","true").save("result")

System.exit(0)

