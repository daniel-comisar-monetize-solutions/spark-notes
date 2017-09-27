# Spark Notes

We were given a sample query (sample-query.sql) and asked to optimize it using Spark SQL. I created a sample database on RDS called inventory-reduction (currently snapshotted and stopped) with some sample data (random strings and numbers). I wrote a DataBricks notebook (spark-notebook.scala) to run the query in Spark SQL. You can compare the execution plans in postgres-explain.txt and spark-explain.txt. I wasn't able to come up with any ideas to optimize the Spark SQL, maybe if I had more time for research.
