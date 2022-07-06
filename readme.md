# Notes for running

- On `fishir`, make sure to set `LD_LIBRARY_PATH` to point to the Oracle drivers, like so:
  - `LD_LIBRARY_PATH="/opt/oracle/instantclient_21_6" python dump_table.py`
- Also make sure to set the `RDW_PASS` environment variable, and try and do it in a way that doesn't show up in shell history.
  - I had good luck using `read -p "some prompt:" RDW_PASS`

