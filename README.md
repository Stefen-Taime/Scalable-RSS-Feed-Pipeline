# Building a Scalable RSS Feed Pipeline with Apache Airflow, Kafka, and MongoDB, Flask Api

[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)

## About

In today's data-driven world, processing large volumes of data in real-time has become essential for many organizations. The Extract, Transform, Load (ETL) process is a common way to manage the flow of data between systems. In this article, we'll walk through how to build a scalable ETL pipeline using Apache Airflow, Kafka, and Python, Mongo and Flask
## Architecture 

![alt text](/images/archi.png)

## Scenario
In this pipeline, the RSS feeds are scraped using a Python library called feedparser. This library is used to parse the XML data in the RSS feeds and extract the relevant information. The parsed data is then transformed into a standardized JSON format using Python's built-in json library. This format includes fields such as title, summary, link, published_date, and language, which make the data easier to analyze and consume.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)

## Setup

Clone the project to your desired location:

    $ git clone https://github.com/Stefen-Taime/Scalable-RSS-Feed-Pipeline.git

Execute the following command that will create the .env file containig the Airflow UID needed by docker-compose:

    $ echo -e "AIRFLOW_UID=$(id -u)" > .env

Build Docker:

    $ docker-compose build 

Initialize Airflow database:

    $ docker-compose up airflow-init

Start Containers:

    $ docker-compose up -d

When everything is done, you can check all the containers running:

    $ docker ps

## Airflow Interface

Now you can access Airflow web interface by going to http://localhost:8080 with the default user which is in the docker-compose.yml. **Username/Password: airflow**
Now, we can trigger our DAG and see all the tasks running.

![alt text](/images/airflow-feeds.png)

## Setup kafka, Mongo 

navigate to cd mongo-kafka:

    $ docker-compose up -d

Execute the following command that will create SinkConnector for Mongo    

    $ curl -X POST \-H "Content-Type: application/json" \
  	--data @mongo-sink.json \
  	http://localhost:8083/connectors
  
  
  
	$ docker exec -ti mongo mongo -u debezium -p dbz --authenticationDatabase admin localhost:27017/demo

    $ show collections;
  
![alt text](/images/mongo-feeds.png)
  

Run api

    $ python api.py

![alt text](/images/fr.png)	


## References 

- [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)


