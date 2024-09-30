"""
Naval Fate.

Usage:
  bench_trino.py native [-n <cores>] [-m <memory>] [-r <runs>] [-H <host>] [-p <port>] [-u <user>] [-c <catalog>] -w <workload_path> [-q <query>] <db>...

Options:
  -n, --cores <cores>               # threads for running trino [default: 32].
  -m, --memory <memory>             Memory limitation for running trino [default: 128g].
  -r, --runs <runs>                 # trials to run each query in the workload [default: 6].
  -H, --host <host>                 Host [default: 127.0.0.1].
  -p, --port <port>                 Port [default: 8080].
  -u, --user <user>                 Username [default: trino-user].
  -c, --catalog <catalog>           Catalog [default: system].
  -w, --workload <workload_path>    The path of workload sql files.
  -q, --query <query>               If set, only run this single query.
  -h --help                         Show this screen.
  --version                         Show version.
"""

import os
import sys
from pathlib import Path
from contexttimer import Timer
from docopt import docopt
from trino.dbapi import connect

def bench_trino(conn, args):
    # traverse and workload
    for filename in sorted(Path(args["--workload"]).glob("q*.sql")) if not args["--query"] else [Path(args["--workload"])/f"{args['--query']}.sql"]:
        print(f"\n============ {filename} ============")
        with open(filename, 'r') as f:
            sql = f.read()
        for db in args["<db>"]:
            sql = sql.replace(f"{db}.", f"{db}.public.") # since trino requires to set the schema
        print(sql)
        sys.stdout.flush()

        # run the query in a given number of times
        for i in range(int(args["--runs"])):
            print(f"------------ run {i} ------------")
            cur = conn.cursor()
            with Timer(factor=1000) as timer: # measure in milliseconds
                cur.execute(sql)
                rows = cur.fetchall()
            print(f"get {len(rows)} rows, {len(rows[0]) if len(rows) > 0 else 0} cols")
            print(f"{filename} took {timer.elapsed/1000:.2f} in total")
            print()
            sys.stdout.flush()

if __name__ == "__main__":
    # parse args
    args = docopt(__doc__, version="Naval Fate 2.0")
    print(f"start bench_trino.py args: {args}")

    conn = connect(
        host=args["--host"],
        port=int(args["--port"]),
        user=args["--user"],
        catalog=args["--catalog"])

    bench_trino(conn, args)

    conn.close()