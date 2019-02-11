import os
import psycopg2

def sum_by_state():
    sql_query = """
        SELECT SUM(total_claim_count), practice_state
        FROM pupd_cleaned
        GROUP BY practice_state;
    """
    cur.execute(sql_query)
    conn.commit()

def ndc_cleaned():
    sql_query = """
CREATE TABLE ndc as (
    SELECT *,ROW_NUMBER() OVER (PARTITION BY product_ndc,brand_name ORDER BY product_ndc) AS rownumber
    FROM ndc9
);
    """


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
    merge_npi()
    sum_by_state()
    cur.close()
    conn.close()