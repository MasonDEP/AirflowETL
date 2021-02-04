# AirflowETL

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. As the data engineer, I created high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


![alt text](https://github.com/MasonDEP/AirflowETL/blob/main/dags.png?raw=true)
