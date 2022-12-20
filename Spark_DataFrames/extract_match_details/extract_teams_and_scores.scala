import org.apache.spark.sql.functions.monotonically_increasing_id

val data=sc.textFile("../../data/WorldCupMatches.csv")

val Home_Team_Name=data.map(lines=>lines.split(":::")(5)).toDF("Home_Team_Name")

val Home_Team_Goals=data.map(lines=>lines.split(":::")(6)).toDF("Home_Team_Goals")

val Away_Team_Goals=data.map(lines=>lines.split(":::")(7)).toDF("Away_Team_Goals")

val Away_Team_Name=data.map(lines=>lines.split(":::")(8)).toDF("Away_Team_Name")

val m_res1 = Home_Team_Name.withColumn("id", monotonicallyIncreasingId).join(Home_Team_Goals.withColumn("id", monotonicallyIncreasingId), Seq("id"))

val m_res2 = m_res1.join(Away_Team_Goals.withColumn("id",monotonically_increasing_id()), Seq("id"))

val m_result = m_res2.join(Away_Team_Name.withColumn("id", monotonically_increasing_id()), Seq("id")).sort(col("id")).drop("id")

m_result.show()

System.exit(0)



