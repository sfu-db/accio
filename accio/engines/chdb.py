def registerTmpView(session, sql, alias, url):
    session.query(f"""
                CREATE OR REPLACE VIEW {alias} AS 
                SELECT * FROM connectorx('{url}', \"{sql}\")
                 """)

def run_chdb(plan, session, accio):
    for v in plan.temp_views:
        registerTmpView(session, v.sql.replace('"', '\\\"'), v.alias, accio.get_url(v.db))
    return session.query(plan.local.sql, "Arrow")

def clean_chdb(plan, session):
    for v in plan.temp_views:
        session.query(f"DROP VIEW IF EXISTS {v.alias}")
