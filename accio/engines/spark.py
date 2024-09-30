def registerTmpView(session, sql, alias, db_config):
    info = sql.split("|")
    if len(info) >= 5:
        partitionColumn = info[0]
        numPartitions = info[1]
        lowerBound = info[2]
        upperBound = info[3]
        query = info[4]
        session.sql(f"""
                    CREATE OR REPLACE TEMPORARY VIEW {alias}
                    USING org.apache.spark.sql.jdbc
                    OPTIONS (
                      driver "{db_config["driver"]}",
                      url "{db_config["url"]}",
                      user '{db_config["username"]}',
                      password '{db_config["password"]}',
                      dbtable "{query}",
                      partitionColumn "{partitionColumn}",
                      lowerBound "{lowerBound}",
                      upperBound "{upperBound}",
                      numPartitions "{numPartitions}"
                    )
                    """)
    else:
        session.sql(f"""
                CREATE OR REPLACE TEMPORARY VIEW {alias}
                USING org.apache.spark.sql.jdbc
                OPTIONS (
                  driver "{db_config["driver"]}",
                  url "{db_config["url"]}",
                  query "{sql}",
                  user '{db_config["username"]}',
                  password '{db_config["password"]}'
                )
                """)

def run_spark(plan, session, accio):
    for v in plan.temp_views:
        registerTmpView(session, v.sql.replace('"', '\\"'), v.alias, accio.get_config(v.db))
    return session.sql(plan.local.sql)

def clean_spark(plan, session):
    for v in plan.temp_views:
        session.sql(f"DROP VIEW IF EXISTS {v.alias}")
