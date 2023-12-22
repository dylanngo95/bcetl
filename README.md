
## How to install env

```bash

source venv/bin/activate
pip install --no-binary :all: pyodbc
odbcinst -j

```

## How to run a project?
```bash

source venv/bin/activate

cd src

PYTHONPATH='.' luigi --module item_master  MyTask --x 123 --y 456 --local-scheduler


```
