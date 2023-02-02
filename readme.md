# OMOP-notes-to-parquet

The `dump_table.py` script will connect to an OMOP CDM database, and dump the entire `NOTE` table into a series of [Parquet](https://parquet.apache.org/) files, designed to be easy to work with in an offline manner.

Right now, it is very much hard-coded to run against Oracle; if you are not using Oracle, it should be reasonably straightforward to adjust it to work with something else. 
The main things to watch out for are:

1. The connection setup right now is all-Oracle; you can just rip that out and use any other standard Python DBI connection.
2. Oracle doesn't support `LIMIT X` syntax on queries; instead it uses a weird "FETCH FIRST N ROWS ONLY" syntax. You'll need to modify that in a few places.
3. There's a bit of fanciness happening to get the Python Oracle driver to handle CLOB column types properly; you probably won't need that if you are using a different database.
 
If you want to change this over to work with a different database, what I'd suggest is that you just make your own version of `load_notes()` to work appropriately with your system. 
The most important thing to note about that function its use of cursors and `fetchmany()`- you _don't_ want to try and load all of the `note` table into memory! 

I've experimented a bit with different values for things like the Parquet page and row group sizes, and what I've got here seems to strike a good balance in terms of IO efficiency and compression, but YMMV.

# Notes for running

- On `fishir`, make sure to set `LD_LIBRARY_PATH` to point to the Oracle drivers, like so:
  - `LD_LIBRARY_PATH="/opt/oracle/instantclient_21_6" python dump_table.py`
- Make sure to set appropriate environment variables; via either:
  - by putting them in a `.env` file (preferred)
  - by hand (thought be mindful that you don't let passwords show up in shell history, etc., in case that's something you need to care about)
    - I had good luck using `read -p "some prompt:" RDW_PASS`
- The environment variables the script needs in its current form are:
  - `ORACLE_HOST`
  - `ORACLE_PORT`
  - `ORACLE_SID`
  - `ORACLE_USER`
  - `RDW_PASS` (the password for `ORACLE_USER`)
- Notice the `GRAB_EVERYTHING` variable
  - By default, this script will attempt to pull down the first 2M notes, which is a large enough number to be interesting and find out if you've got bugs, but not so large that it will result in an unwieldy runtime
  - When you're ready to pull down the whole table, just set `GRAB_EVERYTHING` to `True`. 
- Right now, the output path is hard-coded because I am lazy
  - Look for `outfile_path` and set it accordingly
  - At some point I'll get around to using click/argparse or something to set this up in a better way. 

