The BC-ETL project is a demo project, designed to export data from Dynamic Business Central 365 using Python scripts.

## How to install environment?

```bash

python -m venv venv

source venv/bin/activate

# In the Mac M1
pip install --no-binary :all: pyodbc
odbcinst -j

pip install -r requirements.txt

```

## How to run a project?
```bash

source venv/bin/activate

# Run luigi dashboard

luigid &

Go to URL: http://localhost:8082/

# Run tasks

cd src

cp .env_sample .env

# Change the MSSQL configuration

mkdir -p log output

PYTHONPATH='.' luigi --module item_master ItemMaster --x 123 --y 456 --local-scheduler

PYTHONPATH='.' luigi --module product_attribute ProductAttribute

```
