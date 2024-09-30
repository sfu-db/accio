"""
Naval Fate.

Usage:
  bench_spark.py native [-n <cores>] [-m <memory>] [-r <runs>] -c <config_path> -w <workload_path> [-q <query>] [--explain] <db>...
  bench_spark.py accio [-n <cores>] [-m <memory>] [-r <runs>] [-s <strategy>] -c <config_path> -w <workload_path> [-q <query>] [--explain] <db>...
  bench_spark.py manual [-n <cores>] [-m <memory>] [-r <runs>] -c <config_path> [--explain] -q <query> <db>...
  bench_spark.py calibrate [-n <cores>] [-m <memory>] [-r <runs>] -c <config_path> -q <query>

Options:
  -n, --cores <cores>               # threads for running spark [default: 32].
  -m, --memory <memory>             Memory limitation for running spark [default: 128g].
  -r, --runs <runs>                 # trials to run each query in the workload [default: 6].
  -s, --strategy <strategy>         Accio's rewrite strategy [default: benefit].
  -c, --config <config_path>        The path of configuration files.
  -w, --workload <workload_path>    The path of workload sql files.
  -q, --query <query>               If set, only run this single query.
  --explain                         Explain spark/accio plan.
  -h --help                         Show this screen.
  --version                         Show version.
"""

import re
import os
import sys
import json
from pathlib import Path
from contexttimer import Timer
from docopt import docopt
from accio import AccioSession, run_spark, clean_spark
from pyspark.sql import SparkSession


def bench_spark_native(spark, args):
    # parse configuration
    config = {}
    for db in args["<db>"]:
        with open(f"{args['--config']}/{db}.json", 'r') as f:
            pattern = fr'\b{db}\.([a-zA-Z0-9_]+)\b'
            config[db] = (pattern, json.load(f))

    # traverse and workload
    for filename in sorted(Path(args["--workload"]).glob("q*.sql")) if not args["--query"] else [Path(args["--workload"])/f"{args['--query']}.sql"]:
        print(f"\n============ {filename} ============")
        with open(filename, 'r') as f:
            sql = f.read()
        print(sql)

        # prepare to sparksql query by registering remote tables as views
        print("prepare...")
        for db in config:
            (pattern, db_config) = config[db]
            tables = re.findall(pattern, sql)
            # register a temporary view for the table used in the query
            for tab in set(tables):
                spark.sql(f"""
                    CREATE OR REPLACE TEMPORARY VIEW {db}_{tab}
                    USING org.apache.spark.sql.jdbc
                    OPTIONS (
                      driver "{db_config["driver"]}",
                      url "{db_config["url"]}",
                      dbtable "{tab}",
                      user '{db_config["username"]}',
                      password '{db_config["password"]}'
                    )
                    """)
                sql = sql.replace(f"{db}.{tab}", f"{db}_{tab}")

        # run the query in a given number of times
        for i in range(int(args["--runs"])):
            print(f"------------ run {i} ------------")
            with Timer() as timer:
                df = spark.sql(sql)
                df.collect()
            print(f"get {df.count()} rows, {len(df.columns)} cols")
            print(f"{filename} took {timer.elapsed:.2f} in total")
            if args["--explain"]:
                print("############# spark #############")
                df.explain()
                # df.show(n=5)
            print()
            sys.stdout.flush()
                    
def bench_spark_accio(spark, args):
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
                df = run_spark(plan, spark, accio)
                df.collect()
            print(f"get {df.count()} rows, {len(df.columns)} cols")
            print(f"{filename} took {timer.elapsed/1000:.2f} in total")
            if args["--explain"]:
                print("############# accio #############")
                print(plan)
                # print("############# spark #############")
                # df.explain()
                # df.show(n=5)
            print()
            sys.stdout.flush()
            clean_spark(plan, spark)

def bench_spark_manual(spark, args):
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
            with Timer(factor=1000) as timer: # measure in milliseconds
                df = run_spark(plan, spark, accio)
                df.collect()
            print(f"get {df.count()} rows, {len(df.columns)} cols")
            print(f"{idx} {pushdowns} took {timer.elapsed/1000:.2f} in total")
            if args["--explain"]:
                print("############# accio #############")
                print(plan)
                print("############# spark #############")
                df.explain()
                # df.show(n=5)
            print()
            sys.stdout.flush()
            clean_spark(plan, spark)

def calibrate_spark(spark, args):
    import psycopg2
    from benchmark.plans import generator
    accio = AccioSession([os.environ["TARGET_DB"]], config_path=args["--config"])
    pg_url = accio.get_url(os.environ["TARGET_DB"])
    conn = psycopg2.connect(pg_url)
    conn.autocommit = True
    cur = conn.cursor()
    
    latency = []
    for (tid, plan) in generator.all_pairs(args["--query"]):
        print(f"========= {tid} =========")
        print(plan)
        id = tid[2]
        tables = tid[:2]
        # run the query in a given number of times
        sum_t = 0
        for i in range(int(args["--runs"])):
            print(f"------------ run {i} ------------")
            with Timer(factor=1000) as timer: # measure in milliseconds
                df = run_spark(plan, spark, accio)
                df.collect()
            print(f"get {df.count()} rows, {len(df.columns)} cols")
            print(f"{tables} p{id} took {timer.elapsed} ms in total")
            if i > 0:
                sum_t += timer.elapsed
            print()
            sys.stdout.flush()
            clean_spark(plan, spark)
        latency.append(sum_t / (int(args["--runs"])-1))

        # last query
        if id == 3: 
            # issue an explain query to get remote execution only
            sum_t = 0
            for i in range(int(args["--runs"])):
                cur.execute(f"EXPLAIN (ANALYZE, FORMAT JSON, COSTS FALSE, TIMING FALSE) SELECT COUNT(*) FROM ({plan.temp_views[0].sql}) tmp")
                res = cur.fetchall()
                time = res[0][0][0]['Planning Time'] + res[0][0][0]['Execution Time']
                print(f"{tables} p4 took {time} ms in total")
                sys.stdout.flush()
                if i > 0:
                    sum_t += time
            latency.append(sum_t / (int(args["--runs"])-1))

            # compute coefficients
            gamma = latency[4] / (latency[2] - latency[1] - latency[0])
            tau = (latency[3] - latency[4])/ (latency[2] - latency[1] - latency[0])
            print(f"latency of all queries for {tables}: {latency}, coefficients: gamma={gamma}, tau={tau}")
            sys.stdout.flush()

            # reset latency
            latency = []

    cur.close()
    conn.close()     

if __name__ == "__main__":
    # parse args
    args = docopt(__doc__, version="Naval Fate 2.0")
    print(f"start bench_spark.py args: {args}")

    # start spark session
    spark = (
        SparkSession.builder.master(f"local[{args['--cores']}]")
        .appName("spark-accio")
        .config("spark.jars", os.environ["SPARK_JARS"])
        .config("spark.executor.memory", args["--memory"])
        .config("spark.driver.memory", args["--memory"])
        .config("spark.sql.cbo.enabled", "true")
        .config("spark.log.level", "WARN")
        .config("spark.driver.maxResultSize", "0")
        .getOrCreate()
    )
    print(f"spark session started: {spark.sparkContext.getConf().getAll()}")

    # run sparksql native baseline
    if args["native"]:
        bench_spark_native(spark, args)

    # run sparksql + accio
    if args["accio"]:
        bench_spark_accio(spark, args)

    # run manual plan test
    if args["manual"]:
        bench_spark_manual(spark, args)

    # run calibration
    if args["calibrate"]:
        calibrate_spark(spark, args)

    spark.stop()
