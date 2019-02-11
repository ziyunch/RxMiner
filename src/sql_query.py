import os
import psycopg2

def union_table():
    sql_query = """
        CREATE TABLE rxsum AS
            (SELECT * FROM sdud_class
            UNION
            SELECT * FROM pupd_class);
    """
    cur.execute(sql_query)
    conn.commit()

def add_healthcare():
    sql_query = """
        ALTER TABLE sdud_class
        ADD healthcare INT NULL;
        UPDATE sdud_class
        SET healthcare = 1;
        ALTER TABLE pupd_class
        ADD healthcare INT NULL;
        UPDATE pupd_class
        SET healthcare = 2;
    """
    cur.execute(sql_query)
    conn.commit()

def sdud_class():
    sql_query = """
        SELECT
            sdud_sum.state,
            sdud_sum.generic,
            sdud_sum.year,
            sdud_sum.total,
            genclass.rxclass
        INTO sdud_class
        FROM sdud_sum
        LEFT JOIN genclass
            ON LOWER(genclass.generic_name) = sdud_sum.generic
    """
    cur.execute(sql_query)
    conn.commit()

def pupd_class():
    sql_query = """
        SELECT
            pupd_sum.state,
            pupd_sum.generic,
            pupd_sum.year,
            pupd_sum.total,
            pupd_genclass.rxclass
        INTO pupd_class
        FROM pupd_sum
        LEFT JOIN pupd_genclass
            ON LOWER(pupd_genclass.generic_name) = pupd_sum.generic
    """
    cur.execute(sql_query)
    conn.commit()

def pupd_sum():
    sql_query = """
        SELECT
            practice_state AS state,
            LOWER(generic_name) AS generic,
            year,
            SUM(total_drug_cost) AS total
        INTO pupd_sum
        FROM pupd_cleaned
        GROUP BY state,generic,year
    """
    cur.execute(sql_query)
    conn.commit()

def sdud_sum():
    sql_query = """
        SELECT
            state,
            LOWER(generic_name) AS generic,
            year,
            SUM(medicaid_reimbursed) AS total
        INTO sdud_sum
        FROM sdud_cleaned
        GROUP BY state,generic,year
    """
    cur.execute(sql_query)
    conn.commit()

def merge_sdud():
    sql_query = """
        SELECT
            sdud.state,
            ndc.generic_name,
            sdud.year,
            sdud.num_prescriptions,
            sdud.medicaid_reimbursed,
            CASE
                WHEN ndc.generic_name IS NULL THEN 0
                ELSE 1
            END AS validated
        INTO
            sdud_cleaned
        FROM
            sdud
        LEFT JOIN
            ndc
            ON (sdud.ndc9 = ndc.product_ndc
            AND UPPER(LEFT(sdud.drug_name,5)) = UPPER(LEFT(ndc.brand_name,5)))
    """
    cur.execute(sql_query)
    conn.commit()

def merge_pupd():
    sql_query = """
        SELECT
            npidata.practice_state,
            pupd.generic_name,
            pupd.year,
            pupd.total_claim_count,
            pupd.total_day_supply,
            pupd.total_drug_cost,
            CASE
                WHEN npidata.practice_state IS NULL THEN 0
                ELSE 1
            END AS validated
        INTO
            pupd_cleaned
        FROM
            pupd
        LEFT JOIN npidata
        ON (npidata.npi = pupd.npi AND npidata.last_name = pupd.nppes_provider_last_org_name);
    """
    cur.execute(sql_query)
    conn.commit()

if __name__ == "__main__":
    user = os.getenv('POSTGRESQL_USER', 'default')
    pswd = os.getenv('POSTGRESQL_PASSWORD', 'default')
    host = os.getenv('POSTGRESQL_HOST_IP', 'default')
    port = os.getenv('POSTGRESQL_PORT', 'default')
    dbname = 'postgres'
    conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=pswd)
    cur = conn.cursor()
    print("PostgreSQL connected")
    merge_pupd()
    pupd_sum()
    pupd_class()
    merge_sdud()
    sdud_sum()
    sdud_class()
    add_healthcare()
    union_table()
    cur.close()
    conn.close()