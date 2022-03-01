# Real-world-application-project-for-Big-Data-with-Apache-Spark-and-AWS-EMR

Project Idea
Here is my project idea to get started with Big Data concepts and ETL.
1.	We would like to generate Sales and Customer data on hourly/daily basis to emulate a production scenario.
2.	We would want to create a library of utility function that can be utilized by all future ETLs.
3.	We would like to create a small Data Lake on S3 and apply the Big Data optimization techniques on our tables and datasets.
4.	We would like to run multiple Big Data ETL on Amazon Glue
1.	ETL 1 : Daily/Hourly injection job that cleans the raw data on S3, extracts required fields and creates an incremental table.
5.	We would like to use Spark-SQL for our ETL work loads.

## Project Design
Here is the design I came up with, and you could choose to replace any of the components with something you are more comfortable with.
![etl_pic2](https://user-images.githubusercontent.com/34807427/156143294-f5b6ce55-f139-4d0c-86dd-3214487f49eb.jpg)
