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
spark-shell -i most_playing_teams.scala
echo ""
echo "Top 5 teams that have played the most games over all editions are:"
echo ""
cat "results/part-00000"
echo ""
