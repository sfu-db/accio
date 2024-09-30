"""
Naval Fate.

Usage:
  bench_datafusion.py accio [-n <cores>] [-m <memory>] [-r <runs>] [-s <strategy>] -c <config_path> -w <workload_path> [-q <query>] [--explain] <db>...

Options:
  -n, --cores <cores>               # threads for running datafusion [default: 32].
  -m, --memory <memory>             Memory limitation for running datafusion [default: 128000000000].
  -r, --runs <runs>                 # trials to run each query in the workload [default: 6].
  -s, --strategy <strategy>         Accio's rewrite strategy [default: benefit].
  -c, --config <config_path>        The path of configuration files.
  -w, --workload <workload_path>    The path of workload sql files.
  -q, --query <query>               If set, only run this single query.
  --explain                         Explain datafusion/accio plan.
  -h --help                         Show this screen.
  --version                         Show version.
"""

import sys
from pathlib import Path
from contexttimer import Timer
from docopt import docopt
from accio import AccioSession, run_datafusion, clean_datafusion
from datafusion import RuntimeConfig, SessionConfig, SessionContext

def bench_datafusion_accio(context, args):
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
                df = run_datafusion(plan, context, accio)
            print(f"get {df.count()} rows, {len(df.schema())} cols")
            print(f"{filename} took {timer.elapsed/1000:.2f} in total")
            if args["--explain"]:
                print("############# accio #############")
                print(plan)
                print("############# datafusion #############")
                df.explain()
                df.show(5)
            print()
            sys.stdout.flush()
            clean_datafusion(plan, context)

if __name__ == "__main__":
    # parse args
    args = docopt(__doc__, version="Naval Fate 2.0")
    print(f"start bench_datafusion.py args: {args}")

    # start spark session
    session_config = (
        SessionConfig()
        .with_target_partitions(int(args["--cores"]))
    )
    runtime_config = (
        RuntimeConfig()
        .with_greedy_memory_pool(int(args["--memory"]))
    )
    context = SessionContext(config=session_config, runtime=runtime_config)

    # run datafusion + accio
    if args["accio"]:
        bench_datafusion_accio(context, args)
