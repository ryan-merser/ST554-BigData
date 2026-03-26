------
**Project 2**

This project assesses basic use of Spark and some of the programming techniques we’ve covered.

**Big Picture Goals**

In the first part of the project, we will design and implement a data quality class in pyspark.
Rather than writing a Spark script with standard spark functionality, you will create a python class that
wraps a Spark (SQL style) DataFrame and provides functionality for cleaning and checking the data.

In the second part of the project, we’ll analyze some data using both spark SQL style data frames and the
pandas-on-spark data frames to get some practice with that. 

**Important: Substantial commit history**
Since I did not give insightful descriptions for each commit (I will do this in the future), I will outline the commit history below.
This is mainly the context for file uploads, and each commit with its associated SHA:


3/23:

* Uploaded troubleshooting notebook (probably not substantial), SHA: 0db0d76a36c50a25f01267991cf4d2dad5e2a958
* Fixed errors in the notebook with summarization methods on air quality data using class, added code for analysis of NFL data with pandas on Spark and Spark SQL, SHA: 6c346cdcc7ae378b542632186939f63d57b5bbd2
* Fixed AnalysisException errors in class when reading columns; backticks added everywhere a column name is passed to F.col(), SHA: c8b63f7873af390a898138de6f2fad88cb81152f
* Uploaded first version of project notebook, testing my_class methods on air quality data, SHA: a5719e7264fcae3dbe983de74644d456e4fb7cb2

3/22:

* Renamed the filename of Python class to my_class to fix errors, SHA: 344859c7ae588f7efc342de36024be86749a234b
* Uploaded notebook for testing import of Python class, SHA: 7819bb3e155e7a76cae7e9be6f1c18a8234a5588
* Uploaded first version of Python SparkDataCheck class, SHA: 05e385c253d78cdf1ab83c36115f5160db6e59ae
