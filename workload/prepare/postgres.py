"""Naval Fate.

Usage:
  postgres.py [--url <url>] [--schema <schema>] [--stats <stats>] [--seed <seed>]

Options:
  -h --help             Show this screen.
  --url <url>           Database URL.
  --schema <schema>     Target schema [default: public].
  --stats <stats>       Per-column statistics-gathering target for subsequent ANALYZE operations [default: 100].
  --seed <seed>         Random seed [default: 42].
"""


from docopt import docopt
import psycopg2

if __name__ == "__main__":
    args = docopt(__doc__, version="Naval Fate 2.0")

    url = args["--url"]
    schema = args["--schema"]
    stats = int(args["--stats"])
    seed = 1 / int(args["--seed"])
    print(f"collect statistics for {url}\nschema: {schema}, stats: {stats}, seed: {seed}")

    conn = psycopg2.connect(url)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"select setseed({seed});")

    cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}';")
    tables = [row[0] for row in cursor.fetchall()]

    for table in tables:
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' and table_name='{table}';") 
        columns = [row[0] for row in cursor.fetchall()]
        for column in columns:
            cursor.execute(f"""alter table "{schema}"."{table}" alter column "{column}" set statistics {stats};""")
            cursor.execute(f"""alter table "{schema}"."{table}" set (autovacuum_enabled = off);""") # disable auto vacuum
        print(f"analyze table {schema}.{table} with {len(columns)} columns")
        cursor.execute(f"""analyze "{schema}"."{table}";""")
        conn.commit()
    
    conn.close()
