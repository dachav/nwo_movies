IMDB Movie Data Warehouse
============
This is a python powered application that scrapes the top 50 English movies per genre on IMBD and return a star schema in Postgres for analytical use
---

## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Setup](#setup)
* [Data](#data)

## General info
This project is for consideration for employment with nwo.ai
	
## Technologies
Project is created with:
* Python version: 3.8.9
* docker-compose version: 3.7
* postgres version: 14.1
	
## Setup
To run this project, in the base directory first create the postgres instance with docker-compose:

```
$ docker-compose up -d
```

After creating a python virtual environment, install the required packages with the pip command

```
$ pip install -r requirements.txt
```


To run this project:

```
$ python main.py
```

## Data

After running the code, you can view data with Adminer at http://localhost:8080 or use another software. Login with the following credentials:
```
System : PostgreSQL
Server : postgres
Username : root
Password : changeme
Database : moviesdb
```
