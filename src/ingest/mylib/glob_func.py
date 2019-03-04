import os
import pandas as pd
import StringIO
import psycopg2
import sqlalchemy as sa # Package for accessing SQL databases via Python
import time
import datetime
import pytz

def time_stamp():
    """
    Time stamp in Eastern time zone
    """
    eastern = pytz.timezone('US/Eastern')
    ts = datetime.datetime.now(eastern).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return ts

def clean_columns(columns):
    """
    Replace space by underscore for column names
    """
    cols = []
    for col in columns:
        col = col.replace(' ', '_')
        cols.append(col)
    return cols

def df_to_redshift(df, table_name, mode, new_table, cur, engine, s3):
    """
    Save DataFrame to .csv, read csv as sql table in memory and copy the table
     directly in batch to PostgreSQL or Redshift.
    """
    # Prepare schema for table
    df[:0].to_sql('temp', engine, if_exists = "replace", index=False)
    df.columns = clean_columns(df.columns)
    # replace mode
    if mode == 'replace':
        cur.execute("DROP TABLE " + table_name)
        cur.connection.commit()
    # Prepare data to temp
    with s3.open('rxminer/temp.csv', 'wb') as f:
        df.to_csv(f, index=False, header=False)
    sql_query = """
        COPY temp
        FROM 's3://rxminer/temp.csv'
        IAM_ROLE 'arn:aws:iam::809946175142:role/RedshiftCopyUnload'
        CSV
        IGNOREHEADER 1;
        COMMIT;
    """
    cur.execute(sql_query)
    # Copy or append table temp to target table
    if (mode == 'replace' or new_table):
        sql_query2 = """
            ALTER TABLE temp
            RENAME TO %s;
            COMMIT;VACUUM;COMMIT;
        """
    else:
        sql_query2 = """
            INSERT INTO %s SELECT * FROM temp;
            DROP TABLE temp;
            COMMIT;VACUUM;COMMIT;
        """
    cur.execute(sql_query2 % table_name)

def df_to_psql(df, table_name, mode, new_table, cur, engine):
    """
    Save DataFrame to .csv, read csv as sql table in memory and copy the table
     directly in batch to PostgreSQL.
    """
    # Prepare schema for table
    df[:0].to_sql('temp', engine, if_exists = "replace", index=False)
    df.columns = cleanColumns(df.columns)
    # replace mode
    if mode == 'replace':
        cur.execute("DROP TABLE " + table_name)
        cur.connection.commit()
    # Prepare data to temp
    data = StringIO.StringIO()
    df.to_csv(data, header=False, index=False)
    data.seek(0)
    sql_query = """
        COPY temp FROM STDIN WITH
            CSV
            HEADER;
    """
    cur.copy_expert(sql=sql_query, file=data)
    cur.connection.commit()
    # Copy or append table temp to target table
    if (mode == 'replace' or new_table):
        sql_query2 = """
            ALTER TABLE temp
            RENAME TO %s;
        """
    else:
        sql_query2 = """
            INSERT INTO %s SELECT * FROM temp;
            DROP TABLE temp;
        """
    cur.execute(sql_query2 % table_name)
    cur.connection.commit()