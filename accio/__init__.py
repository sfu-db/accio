import os
import time
import json
from urllib.parse import urlparse
from subprocess import Popen, PIPE
from py4j.java_gateway import JavaGateway, Py4JNetworkError

from .engines.spark import run_spark, clean_spark
from .engines.duckdb import run_duckdb, clean_duckdb
from .engines.datafusion import run_datafusion, clean_datafusion
from .engines.polars import run_polars, clean_polars
from .engines.chdb import run_chdb, clean_chdb


class AccioQuery:
    def __init__(self, db, alias, sql, card=0):
        self.db = db
        self.alias = alias
        self.sql = sql
        self.card = card

    def __repr__(self):
        return f"{self.alias}[{self.db}][{self.card}]: {self.sql}"

class AccioPlan:
    def __init__(self, rplan, local=None, temp_views=None):
        if local is not None and temp_views is not None:
            # manually construct a plan
            self.local = local
            self.temp_views = temp_views
        else:
            # parse from rewriter
            self.temp_views = []
            for i in range(rplan.getCount()):
                db = rplan.getDBName(i)
                alias = rplan.getAliasDBName(i)
                sql = rplan.getSql(i).replace("$f", "ACCIOVAR_").replace("EXPR$", "ACCIOEXPR_")
                card = rplan.getCardinality(i)

                if db == "LOCAL":
                    self.local = AccioQuery(db, alias, sql)
                else:
                    self.temp_views.append(AccioQuery(db, alias, sql, card))

    def __repr__(self):
        str_views = "\n".join(str(x) for x in self.temp_views)
        return f"\n###temp views###\n{str_views}\n###local###\n{self.local}\n######\n"


class AccioSession:
    def __init__(self, dbs, config_path=os.environ["ACCIO_CONFIG_PATH"]):
        # parse config information
        self.dbs = {}
        for db in dbs:
            with open(f"{config_path}/{db}.json", 'r') as f:
                d = json.load(f)
                if "url" in d:
                    url = urlparse(d["url"][5:]) # remove ^jdbc:
                    self.dbs[db] = {"url": f"{url.scheme}://{d['username']}:{d['password']}@{url.netloc}{url.path}", "config": d}
                else: # manual schema
                    self.dbs[db] = {"url": None, "config": d}

        # connect to java gateway
        try:
            self._gateway = JavaGateway(eager_load=True)
            self._process = None
        except Py4JNetworkError:
            # start one if not already initialized
            self._process = Popen(["java", "-cp", "accio/accio-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar:accio/deps/*", "ai.dataprep.accio.FederatedQueryRewriter"],
                                stdout=PIPE, stderr=PIPE)
            time.sleep(2)
            self._gateway = JavaGateway()

        # create accio session
        self._entry = self._gateway.entry_point
        db_list = self._gateway.jvm.java.util.ArrayList()
        for db in dbs:
            db_list.append(db)
        self._session = self._entry.createAccioSession(db_list, None, config_path)

    def __del__(self):
        self._gateway.close()
        if self._process:
            self._process.kill()
            time.sleep(2)
    
    def rewrite(self, sql, stragegy=os.environ["ACCIO_REWRITE_STRATEGY"]):
        rplan = self._entry.rewrite(sql, self._session, stragegy)
        return AccioPlan(rplan)

    def get_url(self, db):
        return self.dbs[db]["url"]

    def get_config(self, db):
        return self.dbs[db]["config"]
    
