import cx_Oracle
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
import os, sys
import pandas as pd

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

#sql = "select count(1) from PERSON"
to_fetch = 10000
sql = f"select * from NOTE fetch first :how_many rows only"

chunk_size = 512

# https://cx-oracle.readthedocs.io/en/latest/user_guide/lob_data.html
def output_type_handler(cursor, name, default_type, size, precision, scale):
    if default_type == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)


with conn.cursor() as cursor:
    query = "select count(1) from person"
    cursor.execute(query)
    rows = cursor.fetchone()
    if rows and len(rows) == 1:
        n_persons = rows[0]
        print(f"Dumping notes for {n_persons} patients...")
    else:
        print("Couldn't complete person count; bailing out!")
        sys.exit(1)


curr_table = None

writer = None

with conn.cursor() as cursor:
    conn.outputtypehandler = output_type_handler # to deal with the CLOB column

    cursor.execute(sql, {'how_many': to_fetch}) # note parameterized query

    # https://cx-oracle.readthedocs.io/en/latest/user_guide/sql_execution.html#changing-query-results-with-rowfactories
    col_names = [col[0] for col in cursor.description]
    cursor.rowfactory = lambda *args: dict(zip(col_names, args))

    with tqdm(total=to_fetch) as pbar:

        iter_count = 0

        while True:
            rows = cursor.fetchmany(chunk_size)
            if rows is None or len(rows) == 0:
                break
            pbar.update(len(rows))
            iter_count += 1

            # convert to dataframe
            as_df = pd.DataFrame(rows)
            as_table = pa.Table.from_pandas(as_df, preserve_index=False)

            if curr_table is None:
                curr_table = as_table
            else:
                curr_table = pa.concat_tables([curr_table, as_table])


            if iter_count > 0 and iter_count % 5 == 0:
                pbar.write(f"At iter {iter_count}, about to write {len(curr_table)} rows.")
                if writer is None:
                    writer = pq.ParquetWriter("/data/bedricks/omop_pq_output/rdw_rls_notes.parquet", curr_table.schema)
                writer.write_table(curr_table)
                curr_table = None


            # print(rows[0])
            # tqdm.write(str(rows[0][1]))

if writer is not None:
    if curr_table is not None:
        print(f"flushing {len(curr_table)} records")
        writer.write_table(curr_table)
    print("closing")
    writer.close()
else:
    print("writer is None?")

# try reading back in
z = pq.read_table("/data/bedricks/omop_pq_output/rdw_rls_notes.parquet")
print(z.schema)
print(len(z))