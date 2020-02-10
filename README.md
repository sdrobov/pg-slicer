# PostgreSQL DB data slicer
[![Build Status](https://travis-ci.org/sdrobov/pg-slicer.svg?branch=master)](https://travis-ci.org/sdrobov/pg-slicer)
## Requirements
- Python 3.6+
- psycopg2==2.8.4
- libpq-dev
## Installation
### Prerequisites
- Make sure you have pip3 and virtualenv installed
### Install process
```shell script
python3 -m virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
```
## Usage
```
pg-slicer.py [-h HOST]
             [-p PORT]
             [-U USER]
             [-W PASSWORD]
             [-l LIMIT]
             [--dump-full TABLE]
             [--no-privileges]
             [--no-publications]
             [--no-subscriptions]
             [--help]
             DBNAME
```
