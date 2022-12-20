val data=sc.textFile("../../data/WorldCupMatches.csv")

val splitted_rdd = data.map(line=>line.split(":::"))

val refrees = splitted_rdd.map(line=>line(13))

val refree_country = refrees.map(lines=>lines.substring(lines.lastIndexOf("(")+1,lines.lastIndexOf(")")))

val country_count=refree_country.map(refree=>(refree,1)).reduceByKey((x,y)=>x+y)

val country_sorted=country_count.sortBy(x=>x._2,false,1).take(10)

val country_sorted_rdd = sc.parallelize(country_sorted)

country_sorted_rdd.repartition(1).saveAsTextFile("results")

System.exit(0)
