if [ -d "results" ]
then
	echo ""
	echo "Removing existing directory"
	rm -r "results"
else
	echo ""
	echo "Executing script"
	echo ""
fi
spark-shell -i most_scoring_teams.scala
echo ""
echo "Top 5 teams with the highest number of goals over all editions are:"
echo ""
cat "results/part-00000"
echo ""
