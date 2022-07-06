import cx_Oracle
from tqdm import tqdm

import pyarrow.parquet as pq
import pyarrow as pa
import os, sys
import pandas as pd
import logging
from tqdm.contrib.logging import logging_redirect_tqdm

logging.basicConfig(level=logging.DEBUG)

if sys.platform == "darwin":
    cx_Oracle.init_oracle_client(lib_dir="/opt/oracle/instantclient_19_8")

dsn = """
(DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = octriusrdblxd1.ohsu.edu )(PORT = 1515))
    )
    (CONNECT_DATA =
      (SID = OCTRIUSR)
    )
  )
"""

conn = cx_Oracle.connect(
    user="RDW_RLS_COHORT_TEXT",
    password=os.getenv("RDW_PASS"),
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
    # query a couple of rows from Oracle, use them to make a new dataframe,
    # then make an Arrow table, and then return the schema
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


# outfile_path = "/data/bedricks/omop_pq_output/notes/"
outfile_path = "/Users/bedricks/Documents/Mayo R01 PHI/pq_files/"
filename_template = "rdw_rls_notes.{}.parquet"


def flush_buffer_to_table(output_path: str, buffer, schema_to_use, rg_size):
    logging.info(f"Writing to {output_path}")

    logging.debug("About to convert to dataframe")
    as_df = pd.DataFrame(buffer)
    as_df.PROVIDER_ID = as_df.PROVIDER_ID.astype("Int64")  # https://pandas.pydata.org/docs/user_guide/integer_na.html

    logging.debug("About to make Table")
    as_tb = pa.Table.from_pandas(as_df, schema=schema_to_use, preserve_index=False)

    logging.debug("About to write table")
    pq.write_table(as_tb, output_path, row_group_size=rg_size)


def main():
    GRAB_EVERYTHING = False # set me to True in order to download all notes...

    to_fetch = 2_000_000

    rows_per_pq_file = 2 ** 19  # about 500k-ish
    # rows_per_pq_file = 2 ** 15  # 32K, for testing
    chunk_size = 2048  # how many rows to grab at once from Oracle

    # if we're going with batches of 500k, should end up with 4 or so row groups per file
    pa_row_group_size = 2 ** 17  # how big should each PQ row group be?

    # pa_page_size = 2**10 * 2**10 * 4
    pa_page_size = 2 ** 10 * 2 ** 10

    n_persons, n_notes = get_counts()

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
                path_to_write = os.path.join(outfile_path, filename_template.format(shard_count))
                flush_buffer_to_table(path_to_write, data_buffer, our_schema, pa_row_group_size)
                shard_count += 1

                # and get set up for next time:
                data_buffer = rows

    logging.info("Done retrieving rows!")

    if len(data_buffer) > 0:
        logging.info(f"{len(data_buffer)} records in buffer, flushing")
        path_to_write = os.path.join(outfile_path, filename_template.format(shard_count))
        flush_buffer_to_table(path_to_write, data_buffer, our_schema, pa_row_group_size)

    logging.info("Now to read back in...")

    # try reading back in
    path_to_read = outfile_path  # it'll be a directory of PQ files, and we can open that directly

    z = pq.ParquetDataset(path_to_read, use_legacy_dataset=False)
    print(z.schema)

    total_count = 0
    for fragment_idx, fragment in enumerate(z.fragments):
        logging.info(f"fragment {fragment_idx+1} has {fragment.metadata.num_rows} rows")
        total_count += fragment.metadata.num_rows

    logging.info(f"total size: {total_count}")


if __name__ == '__main__':
    main()