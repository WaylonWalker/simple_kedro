"""
Simple Kedro Pipeline

# Learn from this

This is a learning exercise to learn the internals of kedro.  It is highly reccomended
to use a project template such as `kedro new` in order to start a real kedro project.
Using this technique you will miss out of some of the great tools that kedro provides
(cli, viz, docker), or you will end up re-building infrastructure that `kedro new` would
have given you.

# Usage


## create an instance of the simple_kedro pipeline

``` python
from simple_kedro import create_simple_kedro
sk = create_simple_kedro
```

## load things from the pipeline

```python
sk.io.load('raw_cities')
```

## run the pipeline
```python
# Full Pipeline
sk.run()
# Partial Pipeline
sk.run(sk.pipeline.only_nodes_with_tags('int')
```
"""

from kedro.io import CSVHTTPDataSet, DataCatalog, ParquetLocalDataSet
from kedro.pipeline import Pipeline, node
from pathlib import Path
from kedro.runner import SequentialRunner
from types import SimpleNamespace


def build_catalog(root_dir):
    """Creates the kedro catalog object stored as io"""
    return DataCatalog(
        {
            "raw_cities": CSVHTTPDataSet(
                fileurl="https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv",
                auth=None,
                load_args=None,
            ),
            "int_cities": ParquetLocalDataSet(
                filepath=root_dir / "data" / "int" / "cities",
            ),
            "pri_cities": ParquetLocalDataSet(
                filepath=root_dir / "data" / "pri" / "cities",
            ),
        },
    )


def create_pipeline():
    """creates the kedro pipeline object"""
    return Pipeline(
        [
            node(
                func=create_int_cities,
                inputs="raw_cities",
                outputs="int_cities",
                name="create_int_cities",
                tags=["int", "cities"],
            ),
            node(
                func=create_pri_cities,
                inputs="int_cities",
                outputs="pri_cities",
                name="create_pri_cities",
                tags=["pri", "cities"],
            ),
        ]
    )


def create_simple_kedro():
    """
    creates the simple kedro object that holds the pipeline and io objects as well as 
    the run function
    """
    sk = SimpleNamespace()
    sk.root_dir = Path(__file__).parent
    sk.io = build_catalog(sk.root_dir)
    sk.pipeline = create_pipeline()
    sk.runner = SequentialRunner()
    sk.run = (
        lambda pipeline=None: sk.runner.run(sk.pipeline, sk.io)
        if pipeline == None
        else sk.runner.run(pipeline, sk.io)
    )
    return sk


def create_int_cities(raw_cities):
    """ gets cities from raw and fixes column names """
    int_cities = raw_cities.copy()
    int_cities.columns = [
        col.lower().strip().replace("'", "").replace('"', "")
        for col in raw_cities.columns
    ]
    return int_cities


def create_pri_cities(int_cities):
    """gets cities from int and cleans row content"""
    pri_cities = int_cities.copy()
    str_columns = [
        col for col in pri_cities.columns if pri_cities[col].dtype.name == "object"
    ]
    for col in str_columns:
        pri_cities[col] = pri_cities[col].str.lower().str.strip().str.replace('"', "")
    return pri_cities
