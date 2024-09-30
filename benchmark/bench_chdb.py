"""
Naval Fate.

Usage:
  bench_chdb.py native [-n <cores>] [-m <memory>] [-r <runs>] -c <config_path> -w <workload_path> [-q <query>] [-t <timeout>] [--explain] <db>...
  bench_chdb.py accio [-n <cores>] [-m <memory>] [-r <runs>] [-s <strategy>] -c <config_path> -w <workload_path> [-q <query>] [-t <timeout>] [--explain] <db>...
  bench_chdb.py manual [-n <cores>] [-m <memory>] [-r <runs>] -c <config_path> [-t <timeout>] [--explain] -q <query> <db>...

Options:
  -n, --cores <cores>               # threads for running chdb [default: 32].
  -m, --memory <memory>             Memory limitation for running chdb [default: 128000000000].
  -r, --runs <runs>                 # trials to run each query in the workload [default: 6].
  -s, --strategy <strategy>         Accio's rewrite strategy [default: benefit].
  -c, --config <config_path>        The path of configuration files.
  -w, --workload <workload_path>    The path of workload sql files.
  -q, --query <query>               If set, only run this single query.
  -t, --timeout <timeout>           Set the timeout for each query in seconds [default: 120].
  --explain                         Explain chdb/accio result.
  -h --help                         Show this screen.
  --version                         Show version.
"""

import re
import sys
import json
from pathlib import Path
from contexttimer import Timer
from docopt import docopt
from urllib.parse import urlparse
from accio import AccioSession, run_chdb, clean_chdb
import chdb
from chdb import session

def postgres_func(address, database, user, password):
    return "postgresql('" + address + "', '" + database + "', '{table}', '" + user + "', '" + password + "')"

def bench_chdb_native(sess, args):
    # parse configuration
    config = {}
    for db in args["<db>"]:
        with open(f"{args['--config']}/{db}.json", 'r') as f:
            pattern = fr'\b{db}\.([a-zA-Z0-9_]+)\b'
            db_config = json.load(f)
            if db_config["type"] == "POSTGRES":
                url = urlparse(db_config["url"][5:]) # remove ^jdbc:
                db_config["func"] = postgres_func(url.netloc, url.path[1:], db_config["username"], db_config["password"])
            else:
                print(f"{db} type {db_config['type']} is not configured/supported yet!")
            config[db] = (pattern, db_config)

    # traverse and workload
    for filename in sorted(Path(args["--workload"]).glob("q*.sql")) if not args["--query"] else [Path(args["--workload"])/f"{args['--query']}.sql"]:
        print(f"\n============ {filename} ============")
        with open(filename, 'r') as f:
            sql = f.read()
        print(sql)

        # prepare to chdb query by registering remote tables as views
        print("prepare...")
        for db in config:
            (pattern, db_config) = config[db]
            tables = re.findall(pattern, sql)
            # register a temporary view for the table used in the query
            for tab in set(tables):
                sess.query(f"""
                    CREATE OR REPLACE VIEW {db}_{tab} AS
                    SELECT * FROM {db_config["func"].format(table=tab)}
                    """)
                sql = sql.replace(f"{db}.{tab}", f"{db}_{tab}")

        # run the query in a given number of times
        num_timeout = 0
        for i in range(int(args["--runs"])):
            print(f"------------ run {i} ------------")
            try:
                with Timer() as timer:
                    df = sess.query(sql, "Arrow")
                    table = chdb.to_arrowTable(df)
                print(f"get {table.num_rows} rows, {table.num_columns} cols")
                print(f"{filename} took {timer.elapsed:.2f} in total")
                if args["--explain"]:
                    print("############# chdb #############")
                    print(table.to_pandas().head(5))
            except Exception as e:
                print(f"{filename} got 999999 in total")
                print(e)
                num_timeout += 1
                # break if timeout twice
                if num_timeout > 1:
                    # print out rest of the logs for easy processing
                    for _ in range(i+1, int(args["--runs"])):
                        print(f"{filename} got 999999 in total")
                    break
            print()
            sys.stdout.flush()
                    
def bench_chdb_accio(sess, args):
    accio = AccioSession(args["<db>"], config_path=args["--config"])

    # traverse and workload
    for filename in sorted(Path(args["--workload"]).glob("q*.sql")) if not args["--query"] else [Path(args["--workload"])/f"{args['--query']}.sql"]:
        print(f"\n============ {filename} ============")
        with open(filename, 'r') as f:
            sql = f.read()
        print(sql)

        # run the query in a given number of times
        for i in range(int(args["--runs"])):
            print(f"------------ run {i} ------------")
            try:
                with Timer(factor=1000) as timer: # measure in milliseconds
                    plan = accio.rewrite(sql, stragegy=args["--strategy"])
                    print(f"{filename} took {timer.elapsed:.2f} in rewrite (ms)")
                    df = run_chdb(plan, sess, accio)
                    table = chdb.to_arrowTable(df)
                print(f"get {table.num_rows} rows, {table.num_columns} cols")
                print(f"{filename} took {timer.elapsed/1000:.2f} in total")
                if args["--explain"]:
                    print("############# accio #############")
                    print(plan)
                    print("############# chdb #############")
                    print(table.to_pandas().head(5))
            except Exception as e:
                print(f"{filename} took {args['--timeout']} in total [TIMEOUT]")
                print(e)
            print()
            sys.stdout.flush()
            clean_chdb(plan, sess)

def bench_chdb_manual(sess, args):
    from benchmark.plans import generator
    accio = AccioSession(args["<db>"], config_path=args["--config"])

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
            try:
                with Timer(factor=1000) as timer: # measure in milliseconds
                    df = run_chdb(plan, sess, accio)
                    table = chdb.to_arrowTable(df)
                print(f"get {table.num_rows} rows, {table.num_columns} cols")
                print(f"{idx} {pushdowns} took {timer.elapsed/1000:.2f} in total")
                if args["--explain"]:
                    print("############# accio #############")
                    print(plan)
                    print("############# chdb #############")
                    print(table.to_pandas().head(5))
            except Exception as e:
                print(f"{idx} {pushdowns} got 999999 in total")
                print(e)
            print()
            sys.stdout.flush()
            clean_chdb(plan, sess)

if __name__ == "__main__":
    # parse args
    args = docopt(__doc__, version="Naval Fate 2.0")
    print(f"start bench_chdb.py args: {args}")

    # start chdb session
    sess = session.Session()
    sess.query("CREATE DATABASE IF NOT EXISTS test ENGINE = Atomic")
    sess.query("Use test")
    sess.query("SET join_use_nulls = 1")
    sess.query(f"SET max_threads = {args['--cores']}")
    sess.query(f"SET max_memory_usage = {args['--memory']}")
    sess.query(f"SET max_execution_time = {args['--timeout']}")

    print("chdb session started:")
    sess.query("SELECT * FROM system.settings WHERE changed").show()
    sys.stdout.flush()

    # run chdb native baseline
    if args["native"]:
        bench_chdb_native(sess, args)

    # run chdb + accio
    if args["accio"]:
        bench_chdb_accio(sess, args)

    # run manual plan test
    if args["manual"]:
        bench_chdb_manual(sess, args)

