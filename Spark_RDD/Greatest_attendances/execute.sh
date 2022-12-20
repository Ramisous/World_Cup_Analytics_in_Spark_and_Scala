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
spark-shell -i greatest_attendances.scala
echo ""
echo "Top five editions with the greatest number of attendance are:"
echo ""
cat "results/part-00000"
echo ""
