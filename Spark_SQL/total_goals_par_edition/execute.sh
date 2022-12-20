if [ -d "result" ]
then 		
	echo ""
	echo "Removing previously existing results"
	echo ""
	rm -r "result"
else
	echo "Executing script"
	echo ""
fi
spark-shell -i total_goals_par_edition.scala
