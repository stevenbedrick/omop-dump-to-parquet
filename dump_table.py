import cx_Oracle
from tqdm import tqdm
import pyarrow.parquet as pq
import os

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
to_fetch = 1000000
sql = f"select * from NOTE fetch first {to_fetch} rows only"

chunk_size = 512

# https://cx-oracle.readthedocs.io/en/latest/user_guide/lob_data.html
def output_type_handler(cursor, name, default_type, size, precision, scale):
    if default_type == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)


with conn.cursor() as cursor:
    conn.outputtypehandler = output_type_handler


    cursor.execute(sql)

    with tqdm(total=to_fetch) as pbar:

        while True:
            rows = cursor.fetchmany(chunk_size)
            if rows is None or len(rows) == 0:
                break
            pbar.update(len(rows))

            # print(rows[0])
            # tqdm.write(str(rows[0][1]))

