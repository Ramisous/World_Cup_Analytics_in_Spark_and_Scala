val data=sc.textFile("../../data/WorldCupMatches.csv")

val splitted_rdd = data.map(line=>line.split(":::"))

val edition_and_attendance = splitted_rdd.map(line=>(line(0),line(10).toInt))

val editions_count=edition_and_attendance.reduceByKey((x,y)=>x+y)

val editions_sorted=editions_count.sortBy(x=>x._2,false,1).take(5)

val editions_sorted_rdd = sc.parallelize(editions_sorted)

editions_sorted_rdd.repartition(1).saveAsTextFile("results")

System.exit(0)
