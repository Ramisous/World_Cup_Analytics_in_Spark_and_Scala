if [ -d "spark-warehouse" ]
then 		
	echo ""
	echo "Removing previously existing spark-warehouse"
	echo ""
	rm -r "spark-warehouse"
else
	echo "Executing script"
	echo ""
fi

spark-shell -i make_datalake.scala
