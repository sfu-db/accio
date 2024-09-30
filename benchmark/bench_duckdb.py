"""
Naval Fate.

Usage:
  bench_duckdb.py native [-d <dbfile>] [-n <cores>] [-m <memory>] [-r <runs>] -c <config_path> -w <workload_path> [-q <query>] [--explain] <db>...
  bench_duckdb.py accio [-d <dbfile>] [-n <cores>] [-m <memory>] [-r <runs>] [-s <strategy>] -c <config_path> -w <workload_path> [-q <query>] [--explain] [--disable-jo] <db>...
  bench_duckdb.py manual [-d <dbfile>] [-n <cores>] [-m <memory>] [-r <runs>] -c <config_path> [--explain] -q <query> <db>...

Options:
  -d, --dbfile <dbfile>             DuckDB file path [default: test.db].
  -n, --cores <cores>               # threads for running duckdb [default: 32].
  -m, --memory <memory>             Memory limitation for running duckdb [default: 128GB].
  -r, --runs <runs>                 # trials to run each query in the workload [default: 6].
  -s, --strategy <strategy>         Accio's rewrite strategy [default: benefit].
  -c, --config <config_path>        The path of configuration files.
  -w, --workload <workload_path>    The path of workload sql files.
  -q, --query <query>               If set, only run this single query.
  --explain                         Explain duckdb/accio plan.
  --disable-jo                      Disable join reordering in duckdb.
  -h --help                         Show this screen.
  --version                         Show version.
"""

import os
import sys
import json
from pathlib import Path
from contexttimer import Timer
from docopt import docopt
from urllib.parse import urlparse
from accio import AccioSession, run_duckdb, clean_duckdb
import duckdb


def bench_duckdb_native(conn, args):
    # parse configuration
    config = {}
    for db in args["<db>"]:
        with open(f"{args['--config']}/{db}.json", 'r') as f:
            config[db] = json.load(f)

    # attach to external databases
    for db, info in config.items():
        if db == "local" or info["type"] == "MANUAL":
            continue # only register remote non-manual ones
        url = urlparse(info["url"][5:]) # remove ^jdbc:
        db_url = f"{url.scheme}://{info['username']}:{info['password']}@{url.netloc}{url.path}"
        conn.sql(f"""ATTACH '{db_url}' AS {db} (TYPE {info["type"]}, READ_ONLY)""")

    # enable filter pushdown (disabled by default)
    conn.sql("SET pg_experimental_filter_pushdown = true")

    # traverse and workload
    for filename in sorted(Path(args["--workload"]).glob("q*.sql")) if not args["--query"] else [Path(args["--workload"])/f"{args['--query']}.sql"]:
        print(f"\n============ {filename} ============")
        with open(filename, 'r') as f:
            sql = f.read()
        print(sql)

        # run the query in a given number of times
        for i in range(int(args["--runs"])):
            print(f"------------ run {i} ------------")
            with Timer() as timer:
                df = conn.sql(sql)
                table = df.arrow()
            print(f"get {table.num_rows} rows, {table.num_columns} cols")
            print(f"{filename} took {timer.elapsed:.2f} in total")
            if args["--explain"]:
                print("############# duckdb #############")
                print(df.explain())
                print(table.to_pandas().head(5))
            print()
            sys.stdout.flush()
                    
def bench_duckdb_accio(conn, args):
    # start accio session and attach to external databases
    accio = AccioSession(args["<db>"], config_path=args["--config"])
    for db, info in accio.dbs.items():
        if db == "local" or info["config"]["type"] == "MANUAL":
            continue # only register remote non-manual ones
        conn.sql(f"""ATTACH '{info["url"]}' AS {db} (TYPE {info["config"]["type"]}, READ_ONLY)""")

    # traverse and workload
    for filename in sorted(Path(args["--workload"]).glob("q*.sql")) if not args["--query"] else [Path(args["--workload"])/f"{args['--query']}.sql"]:
        print(f"\n============ {filename} ============")
        with open(filename, 'r') as f:
            sql = f.read()
        print(sql)

        # run the query in a given number of times
        for i in range(int(args["--runs"])):
            print(f"------------ run {i} ------------")
            with Timer(factor=1000) as timer: # measure in milliseconds
                plan = accio.rewrite(sql, stragegy=args["--strategy"])
                print(f"{filename} took {timer.elapsed:.2f} in rewrite (ms)")
                df = run_duckdb(plan, conn)
                table = df.arrow()
            print(f"get {table.num_rows} rows, {table.num_columns} cols")
            print(f"{filename} took {timer.elapsed/1000:.2f} in total")
            if args["--explain"]:
                print("############# accio #############")
                print(plan)
                print("############# duckdb #############")
                print(df.explain())
                print(table.to_pandas().head(5))
            print()
            sys.stdout.flush()
            clean_duckdb(plan, conn)

def bench_duckdb_manual(conn, args):
    from benchmark.plans import generator
    # start accio session and attach to external databases
    accio = AccioSession(args["<db>"], config_path=args["--config"])
    for db, info in accio.dbs.items():
        if db == "local" or info["config"]["type"] == "MANUAL":
            continue # only register remote non-manual ones
        conn.sql(f"""ATTACH '{info["url"]}' AS {db} (TYPE {info["config"]["type"]}, READ_ONLY)""")

    # traverse and workload
    for idx, (pushdowns, plan) in enumerate(generator.get_plans(args["--query"])):
        print(f"\n============ {idx} {pushdowns} ============")
        print(plan)
        sys.stdout.flush()

        # run the query in a given number of times
        for i in range(int(args["--runs"])):
            print(f"------------ run {i} ------------")
            if plan is None:
                print(f"{idx} {pushdowns} took -1 in total")
                continue
            with Timer(factor=1000) as timer: # measure in milliseconds
                df = run_duckdb(plan, conn)
                table = df.arrow()
            print(f"get {table.num_rows} rows, {table.num_columns} cols")
            print(f"{idx} {pushdowns} took {timer.elapsed/1000:.2f} in total")
            if args["--explain"]:
                print("############# accio #############")
                print(plan)
                print("############# duckdb #############")
                print(df.explain())
                print(table.to_pandas().head(5))
            print()
            sys.stdout.flush()
            clean_duckdb(plan, conn)

if __name__ == "__main__":
    # parse args
    args = docopt(__doc__, version="Naval Fate 2.0")
    print(f"start bench_duckdb.py args: {args}")

    # start duckdb connection
    conn = duckdb.connect(args["--dbfile"], config = {
        "threads": args["--cores"], 
        "memory_limit": args["--memory"],
        "allow_unsigned_extensions": "true"})
    conn.install_extension(os.environ["DUCK_PG_EXTENSION"])
    conn.load_extension(os.environ["DUCK_PG_EXTENSION"])
    print(f"duckdb connection started...")

    if args["--disable-jo"]:
        conn.query("SET disabled_optimizers TO 'join_order'")

    # run duckdb native baseline
    if args["native"]:
        bench_duckdb_native(conn, args)

    # run duckdb + accio
    if args["accio"]:
        bench_duckdb_accio(conn, args)

    # run manual plan test
    if args["manual"]:
        bench_duckdb_manual(conn, args)

    conn.close()
