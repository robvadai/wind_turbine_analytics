Wind Turbine Analytics
======================

## Testing

```sh
. ./bin/activate
pip install '.[dev]'
pytest --cache-clear --capture=no -m "integration" ./src
```