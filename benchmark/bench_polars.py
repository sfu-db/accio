"""
Naval Fate.

Usage:
  bench_polars.py accio [-n <cores>] [-r <runs>] [-s <strategy>] -c <config_path> -w <workload_path> [-q <query>] [--explain] <db>...

Options:
  -n, --cores <cores>               # threads for running polars [default: 32].
  -r, --runs <runs>                 # trials to run each query in the workload [default: 6].
  -s, --strategy <strategy>         Accio's rewrite strategy [default: benefit].
  -c, --config <config_path>        The path of configuration files.
  -w, --workload <workload_path>    The path of workload sql files.
  -q, --query <query>               If set, only run this single query.
  --explain                         Explain polars/accio plan.
  -h --help                         Show this screen.
  --version                         Show version.
"""

import os
import sys
from pathlib import Path
from contexttimer import Timer
from docopt import docopt
from accio import AccioSession, run_polars, clean_polars
import polars as pl

def bench_polars_accio(context, args):
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
            with Timer(factor=1000) as timer: # measure in milliseconds
                plan = accio.rewrite(sql, stragegy=args["--strategy"])
                print(f"{filename} took {timer.elapsed:.2f} in rewrite (ms)")
                lazy_df = run_polars(plan, context, accio)
                df = lazy_df.collect()
            print(f"get {df.shape[0]} rows, {df.shape[1]} cols")
            print(f"{filename} took {timer.elapsed/1000:.2f} in total")
            if args["--explain"]:
                print("############# accio #############")
                print(plan)
                print("############# polars #############")
                print(lazy_df.explain())
                print(df.head(5))
            print()
            sys.stdout.flush()
            clean_polars(plan, context)

if __name__ == "__main__":
    # parse args
    args = docopt(__doc__, version="Naval Fate 2.0")
    print(f"start bench_polars.py args: {args}")

    # polars does not support customized memory yet: https://github.com/pola-rs/polars/issues/9892
    os.environ["POLARS_MAX_THREADS"] = args["--cores"]
    context = pl.SQLContext()

    # run polars + accio
    if args["accio"]:
        bench_polars_accio(context, args)
