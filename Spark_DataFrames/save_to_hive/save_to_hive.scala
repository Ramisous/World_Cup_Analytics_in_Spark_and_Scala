import java.io.File

import org.apache.spark.sql.SparkSession

val warehouseLocation = new File("spark-warehouse").getAbsolutePath

val spark = SparkSession
  .builder()
  .appName("Spark_App")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()

sql("DROP TABLE IF EXISTS worldCup;")

sql(""" create table worldCup
(Year string, Datetime string, Stage string, 
Stadium string, City string, Home_Team_Name string, 
Home_Team_Goals int,Away_Team_Goals int, Away_Team_Name string, 
Win_conditions string,Attendance int, Half_time_Home_Goals int, 
Half_time_Away_Goals int,Referee string, Assistant_1 string, Assistant_2 string, 
RoundID int, MatchID int, Home_Team_Initials string, Away_Team_Initials string) 
ROW format delimited fields terminated by ',' lines terminated by '\n' stored as textfile; """)

sql("LOAD DATA LOCAL INPATH '../../data/WorldCupMatches.txt' INTO TABLE worldCup;")

val match_details = sql(""" SELECT Home_Team_Name, Home_Team_Goals, Away_Team_Goals, Away_Team_Name
                   FROM worldCup """).toDF("Home_Team_Name","Home_Team_Goals","Away_Team_Goals","Away_Team_Name")

match_details.show()

System.exit(0)




