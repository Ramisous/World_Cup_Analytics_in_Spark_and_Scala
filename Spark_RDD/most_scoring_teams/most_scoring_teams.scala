val data=sc.textFile("../../data/WorldCupMatches.csv")

val home_team_goals= data.map(line=>(line.split(":::")(5),line.split(":::")(6).toInt))

val away_team_goals= data.map(line=>(line.split(":::")(8),line.split(":::")(7).toInt))

val all_teams_goals = home_team_goals ++ away_team_goals

val goals_count=all_teams_goals.reduceByKey((x,y)=>x+y)

val goals_count_sorted=goals_count.sortBy(x=>x._2,false,1).take(5)

val goals_sorted_rdd = sc.parallelize(goals_count_sorted)

goals_sorted_rdd.repartition(1).saveAsTextFile("results")

System.exit(0)
