# Simple Kedro Pipeline

## Learn from this

This is a learning exercise to learn the internals of kedro.  It is highly reccomended
to use a project template such as `kedro new` in order to start a real kedro project.
Using this technique you will miss out of some of the great tools that kedro provides
(cli, viz, docker), or you will end up re-building infrastructure that `kedro new` would
have given you.


Aall of the code can be found in [simple_kedro.py](/simple_kedro.py)

## Usage


### create an instance of the simple_kedro pipeline

``` python
from simple_kedro import create_simple_kedro
sk = create_simple_kedro
```

### load things from the pipeline

```python
sk.io.load('raw_cities')
```

### run the pipeline

```python
# Full Pipeline
sk.run()
# Partial Pipeline
sk.run(sk.pipeline.only_nodes_with_tags('int')
