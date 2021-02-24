# Setting up Spark

1> Set SPARK_HOME environment variable to the location of Spark installation as follows

	export SPARK_HOME="/Users/srpatil/Desktop/Suraj/Frameworks/spark-2.4.5-bin-hadoop2.7"

2.> Set HOME variable 

	export PATH=$SPARK_HOME/bin:$PATH

# Starting a Spark Application

1> Type PySpark in the terminal

2> View the Web UI at port 4040. It has Spark, Storage, Environment, SQL, Master and Executors.

	http://localhost:4040/jobs/

3> Access Spark Context and SparkSession Objects through the application

	sc
	<SparkContext master=local[*] appName=PySparkShell>

	spark
	<pyspark.sql.session.SparkSession object at 0x10dd24e80>

# Outside the Spark Application

1> The --master option specifies the master URL for a distributed cluster, or local to run locally with one thread, or local[N] to run locally with N threads. You should start by using local for testing.

	spark-shell --master local[2]

2> To run an example code, navigate to the Spark examples folder and then python

	spark-submit pi.py

Or you can directly type code and execute in the Spark shell


# Running Spark on Jupyter

1> Keep the findspark.py in the same location as the Python notebook. Import findspark 
and initialise it in code.

2> Run the following command:

	srpatil$  Jupyter notebook

Then navigate to your notebook in the UI opened in the browser and open it.


# References

[Execute Scala scripts without creating Jar](https://bigdataprogrammers.com/how-to-execute-scala-script-in-spark-submit-without-creating-jar/)

[Run Spark on Docker](https://github.com/suraj95/Spark-on-Docker)

[Machine Learning Examples with PySpark](https://github.com/suraj95/ML_Spark)

