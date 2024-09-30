def registerTmpView(conn, db, sql, alias, card):
    conn.sql(f"""CREATE OR REPLACE TEMP VIEW {alias} AS 
                (SELECT * FROM postgres_query('{db}', "{sql}", {card}))""")

def run_duckdb(plan, conn):
    for v in plan.temp_views:
        registerTmpView(conn, v.db, v.sql.replace('"', '""'), v.alias, v.card)
    return conn.sql(plan.local.sql)

def clean_duckdb(plan, conn):
    for v in plan.temp_views:
        conn.sql(f"DROP VIEW IF EXISTS {v.alias}")
