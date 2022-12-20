spark.read.parquet("../sparkdatalake/spark-warehouse/sparkdatalake.db/worldCupMatches/*.parquet").createOrReplaceTempView("worldCupMatches");

spark.sql("""Select distinct wm1.Stadium, wm1.City, wm1.Year as first_edition, wm2.Year as second_edition
from worldCupMatches wm1
Join worldCupMatches wm2
using (Stadium, City)
where wm1.Year < wm2.Year
order by first_edition""").repartition(1).write.format("csv").option("header","true").save("result")

System.exit(0)