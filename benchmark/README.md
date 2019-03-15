# Benchmark
This benchmark file is to test the efficiency of each function/method.

`python3 benchmark.py chunk_size target_database odbc_connector connect_type to_db_type`

- `chunk_size`: read data by chunk of `chunk_size`
- `target_database`: `psql` PostgreSQL, `redshift` Redshift
- `odbc_connector`: `psycopg2` or `sqlalchemy`
- `connect_type`: `engine.raw_connection()` or `engine.connect().connection`
- `to_db_type`: use built-in `df.to_sql` or my own `df_to_sql` function to save data into SQL database.

Sample input would be like:

```bash
nohup python3 -u -m cProfile -o benchmark.out benchmark.py 1000000 psql sqlalchemy no yes &
```

# Some take-home
1. chunk_size 200k would be better than 1m,faster;
2. `pd.to_sql` save to sql line by line, my function to read sql in memory is 100x faster than this;
3. `psycopg2` > `raw_connection` in `sqlalchemy` > `con.connection` in `sqlalchemy`, but no huge difference;
4. `COPY FROM STDIN` is not supported in Redshift. I could use S3 to save the temporary file instead;
5. PostgreSQL fast in IO (use STDIN as temp), Redshift fast in operation.
