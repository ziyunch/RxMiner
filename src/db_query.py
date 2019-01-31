import os
import psycopg2
import sqlalchemy as sa

def sum_by_state():
    query = """
        SELECT SUM(total_claim_count), practice_state
        FROM pupd_cleaned
        GROUP BY pupd_cleaned;
    """
    engine.execute(query)
    print("The number of parts: ", cur.rowcount)
    rows = cur.fetchmany(size=10)
    print(rows)

def merge_npi():
    query = """
        SELECT
            pupd.npi,
            pupd.total_claim_count,
            npidata.practice_state
        INTO
            pupd_cleaned
        FROM
            pupd
        LEFT JOIN npidata ON (npidata.npi = pupd.npi AND npidata.last_name = pupd.nppes_provider_last_org_name);
    """
    engine.execute(query)
    print("The number of parts: ", cur.rowcount)
    rows = cur.fetchmany(size=10)
    print(rows)

if __name__ == "__main__":
    user = os.getenv('POSTGRESQL_USER', 'default')
    pswd = os.getenv('POSTGRESQL_PASSWORD', 'default')
    host = os.getenv('POSTGRESQL_HOST_IP', 'default')
    port = os.getenv('POSTGRESQL_PORT', 'default')
    dbname = 'postgres'
    engine = sa.create_engine('postgresql://'+user+':'+pswd+'@'+host+':'+port+'/'+dbname,echo=False)
    con = engine.connect()
    conn = engine.raw_connection()
    cur = conn.cursor()
    print("PostgreSQL connected")
    merge_npi()
    sum_by_state()
    cur.close()
    conn.close()
    con.close()