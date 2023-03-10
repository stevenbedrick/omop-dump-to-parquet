from dotenv import load_dotenv
import cx_Oracle
from tqdm import tqdm

import pyarrow.parquet as pq
import pyarrow as pa
import os, sys
import pandas as pd
import logging
from tqdm.contrib.logging import logging_redirect_tqdm

import click

logging.basicConfig(level=logging.DEBUG)

load_dotenv()

if sys.platform == "darwin":
    cx_Oracle.init_oracle_client(lib_dir="/opt/oracle/instantclient_19_8")

oracle_host = os.getenv("ORACLE_HOST")
oracle_port = os.getenv("ORACLE_PORT")
oracle_sid = os.getenv("ORACLE_SID")
GRAB_EVERYTHING = False # set me to True in order to download all notes...
if os.getenv("GRAB_EVERYTHING"):
    GRAB_EVERYTHING = True

DEFAULT_N_TO_FETCH = 2_000_000 # How many records to pull by default?

dsn = f"""
(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = {oracle_host} )(PORT = {oracle_port}))
    )
    (CONNECT_DATA =
      (SID = {oracle_sid})
    )
  )
"""

conn = cx_Oracle.connect(
    user=os.getenv("ORACLE_USER"),
    password=os.getenv("ORACLE_PASS"),
    dsn=dsn
)


# https://cx-oracle.readthedocs.io/en/latest/user_guide/lob_data.html
def output_type_handler(cursor, name, default_type, size, precision, scale):
    if default_type == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)


def get_counts():

    with conn.cursor() as cursor:
        query = "select count(1) from person"
        cursor.execute(query)
        rows = cursor.fetchone()
        if rows and len(rows) == 1:
            n_persons = rows[0]
            logging.info(f"Dumping notes for {n_persons} patients...")
        else:
            logging.error("Couldn't complete person count; bailing out!")
            sys.exit(1)

        query = "select count(1) from note"
        cursor.execute(query)
        rows = cursor.fetchone()
        if rows and len(rows) == 1:
            n_notes = rows[0]
            logging.info(f"Total notes in table: {n_notes}")
        else:
            logging.error("Couldn't compute note count, bailing out!")
            sys.exit(1)
    return n_persons, n_notes


def load_notes(n_notes: int, chunk_size: int, progress_callback=None):
    """
    Generator function that runs a query to load up to n_notes, and then yields them in batches of chunk_size.

    This is done in an efficient using an SQL cursor, so we do _not_ have to load n_notes into memory at once.

    Optionally takes a callback function that will be called once per batch, for e.g. updating a progress bar or logging.

    :param n_notes:
    :param chunk_size:
    :param progress_callback:
    :return:
    """
    sql = f"select * from NOTE fetch first :how_many rows only"

    with conn.cursor() as cursor:
        conn.outputtypehandler = output_type_handler  # to deal with the CLOB column

        cursor.execute(sql, {'how_many': n_notes})  # note parameterized query

        # https://cx-oracle.readthedocs.io/en/latest/user_guide/sql_execution.html#changing-query-results-with-rowfactories
        col_names = [col[0] for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(col_names, args))

        while True:
            rows = cursor.fetchmany(chunk_size)
            if rows is None or len(rows) == 0:
                return
            if progress_callback:
                progress_callback(len(rows))
            yield rows




def schema_from_table() -> pa.Schema:
    """
    Induce a pa.Schema object from the structure of the Note table.

    1. Query a couple of rows of the Note table, use them to make a new Pandas dataframe (Panas automatically induces its schema)
    2. Use pa.Table.from_pandas() to make an Arrow Table, which will inherit the Pandas schema
    3. Return the new Table's schema object, so we can use it for our output file.

    Note the business towards the end of the function with DataFrame.PROVIDER_ID;
    this is a finicky detail involving nulls in the database propagating back badly,
    and might be an Oracle-specific issue - YMMV.
    """
    sql = f"select * from NOTE fetch first 10 rows only"

    with conn.cursor() as cursor:
        conn.outputtypehandler = output_type_handler  # to deal with the CLOB column

        cursor.execute(sql)  # note parameterized query

        # https://cx-oracle.readthedocs.io/en/latest/user_guide/sql_execution.html#changing-query-results-with-rowfactories
        col_names = [col[0] for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(col_names, args))

        rows = cursor.fetchall()

        if rows is None or len(rows) == 0:
            raise Exception("Trouble pulling one row to initialize schema")
        as_df = pd.DataFrame(rows)
        # If we don't do this next part, any Nulls in the database will propagate through to turn this into a floating-point column
        as_df.PROVIDER_ID = as_df.PROVIDER_ID.astype(
            "Int64")  # https://pandas.pydata.org/docs/user_guide/integer_na.html
        as_pa = pa.Table.from_pandas(as_df, preserve_index=False)
        return as_pa.schema

def flush_buffer_to_writer(writer_obj: pq.ParquetWriter, buffer):
    as_df = pd.DataFrame(buffer)
    as_df.PROVIDER_ID = as_df.PROVIDER_ID.astype("Int64")  # https://pandas.pydata.org/docs/user_guide/integer_na.html
    as_rb = pa.RecordBatch.from_pandas(as_df, preserve_index=False)
    writer_obj.write_batch(as_rb)

# TODO: Make this not hard-coded!!
filename_template = "omop_notes.{}.parquet"


def flush_buffer_to_table(output_path: str, buffer, schema_to_use, rg_size):
    logging.info(f"Writing to {output_path}")

    logging.debug("About to convert to dataframe")
    as_df = pd.DataFrame(buffer)
    as_df.PROVIDER_ID = as_df.PROVIDER_ID.astype("Int64")  # https://pandas.pydata.org/docs/user_guide/integer_na.html

    logging.debug("About to make Table")
    as_tb = pa.Table.from_pandas(as_df, schema=schema_to_use, preserve_index=False)

    logging.debug("About to write table")
    pq.write_table(as_tb, output_path, row_group_size=rg_size)



@click.command()
@click.option('--output_path',
                required=True,
                type=click.Path(exists=True, dir_okay=True),
                help="Directory in which to write the sharded Parquet files.")
def main(output_path: str):

    rows_per_pq_file = 2 ** 19  # about 500k-ish
    # rows_per_pq_file = 2 ** 15  # 32K, for testing
    chunk_size = 2048  # how many rows to grab at once from Oracle

    # if we're going with batches of 500k, should end up with 4 or so row groups per file
    pa_row_group_size = 2 ** 17  # how big should each PQ row group be?

    # pa_page_size = 2**10 * 2**10 * 4
    pa_page_size = 2 ** 10 * 2 ** 10

    n_persons, n_notes = get_counts()

    to_fetch = DEFAULT_N_TO_FETCH

    if GRAB_EVERYTHING:
        to_fetch = n_notes



    shard_count = 0

    our_schema = schema_from_table()

    logging.info(f"About to fetch {to_fetch} notes")

    data_buffer = []

    with tqdm(total=to_fetch) as pbar, logging_redirect_tqdm():
        def pbar_cb(n):
            pbar.update(n)

        for idx, rows in enumerate(load_notes(to_fetch, chunk_size, progress_callback=pbar_cb)):

            # if len(data_buffer) < pa_row_group_size:
            if len(data_buffer) < rows_per_pq_file:
                data_buffer.extend(rows)
                continue
            else:
                # time to flush existing data:
                path_to_write = os.path.join(output_path, filename_template.format(shard_count))
                flush_buffer_to_table(path_to_write, data_buffer, our_schema, pa_row_group_size)
                shard_count += 1

                # and get set up for next time:
                data_buffer = rows

    logging.info("Done retrieving rows!")

    if len(data_buffer) > 0:
        logging.info(f"{len(data_buffer)} records in buffer, flushing")
        path_to_write = os.path.join(output_path, filename_template.format(shard_count))
        flush_buffer_to_table(path_to_write, data_buffer, our_schema, pa_row_group_size)

    logging.info("Now to read back in...")

    # try reading back in as a sanity check
    path_to_read = output_path  # it'll be a directory of PQ files, and we can open that directly

    z = pq.ParquetDataset(path_to_read, use_legacy_dataset=False)
    print(z.schema)

    total_count = 0
    for fragment_idx, fragment in enumerate(z.fragments):
        logging.info(f"fragment {fragment_idx+1} has {fragment.metadata.num_rows} rows")
        total_count += fragment.metadata.num_rows

    logging.info(f"total size: {total_count}")


if __name__ == '__main__':
    main()