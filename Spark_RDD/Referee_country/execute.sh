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
spark-shell -i refree_country.scala
echo ""
echo "top 10 countries with the highest number of referees are:"
echo ""
cat "results/part-00000"
echo ""
