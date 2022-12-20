val data=sc.textFile("../../data/WorldCupMatches.csv")

val home_team= data.map(line=>line.split(":::")(5))

val away_team= data.map(line=>line.split(":::")(8))

val all_teams = home_team ++ away_team

val teams_count=all_teams.map(team=>(team,1)).reduceByKey((x,y)=>x+y)

val teams_sorted=teams_count.sortBy(x=>x._2,false,1).take(5)

val teams_sorted_rdd = sc.parallelize(teams_sorted)

teams_sorted_rdd.repartition(1).saveAsTextFile("results")

System.exit(0)
