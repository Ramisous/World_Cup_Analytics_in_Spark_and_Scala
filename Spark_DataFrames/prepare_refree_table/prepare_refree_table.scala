import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType, StructField, StringType};

val data=sc.textFile("../../data/WorldCupMatches.csv")

val schemaString = "Year Datetime Referee Assistant_1 Assistant_2"

val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

val rowRDD = data.map(_.split(":::")).map(x => Row(x(0), x(1), x(13), x(14), x(15)))

val refree_table = spark.createDataFrame(rowRDD, schema)

refree_table.show()

System.exit(0)


