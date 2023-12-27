
## How to install env?

```bash

python -m venv venv

source venv/bin/activate
pip install --no-binary :all: pyodbc
odbcinst -j

pip install -r requirements.txt

```

## How to run a project?
```bash

source venv/bin/activate

cd src

PYTHONPATH='.' luigi --module item_master ItemMaster --x 123 --y 456 --local-scheduler

PYTHONPATH='.' luigi --module product_attribute ProductAttribute

```
