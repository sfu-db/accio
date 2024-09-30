package ai.dataprep.accio.plan;

import ai.dataprep.accio.FederatedDataSource;
import ai.dataprep.accio.cost.BaseCostEstimator;
import ai.dataprep.accio.cost.CostEstimator;
import ai.dataprep.accio.partition.*;
import ai.dataprep.accio.sql.*;
import ai.dataprep.accio.stats.*;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.commons.dbcp2.BasicDataSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static java.util.Map.entry;

public class FedConvention extends Convention.Impl {
    private static final Logger logger = LoggerFactory.getLogger(FedConvention.class);

    public static DataSource createDataSource(String url, @Nullable String driverClassName,
                                              @Nullable String username, @Nullable String password) {
        DataSource res = JdbcSchema.dataSource(url, driverClassName, username, password);
        if (url.startsWith("jdbc:duckdb:")) {
            ((BasicDataSource)res).addConnectionProperty("duckdb.read_only", "true");
        }
        return res;
    }

    public static SqlDialect createDialect(DataSource dataSource) {
        SqlDialect dialect = JdbcSchema.createDialect(SqlDialectFactoryImpl.INSTANCE, dataSource);
        return dialect;
    }

    public static PlanPartitioner createPlanPartitioner(JSONObject jsonConfig) {
        PlanPartitioner planPartitioner = null;

        if (jsonConfig.has("partitionType")) {
            String type = jsonConfig.getString("partitionType").toUpperCase();
            if (type.equals("POSTGRES")) {
                planPartitioner = new PostgresPlanPartitioner();
            } else if (type.equals("MYSQL")) {
                planPartitioner = new MysqlPlanPartitioner();
            }
        }

        if (planPartitioner == null) {
            planPartitioner = new NoPlanPartitioner();
        }

        planPartitioner.updateWithConfig(jsonConfig);
        return planPartitioner;
    }

    public static CostEstimator createFedCostEstimator(JSONObject jsonConfig, boolean isLocal) {
        CostEstimator costEstimator = new BaseCostEstimator(isLocal);
        costEstimator.updateWithConfig(jsonConfig);
        return costEstimator;
    }

    public static CardinalityEstimator createFedCardinalityEstimator(JSONObject jsonConfig) {
        CardinalityEstimator cardinalityEstimator = null;

        if (jsonConfig.has("cardEstType")) {
            String type = jsonConfig.getString("cardEstType").toUpperCase();
            if (type.equals("POSTGRES")) {
                cardinalityEstimator = new PostgresCardinalityEstimator();
            } else if (type.equals("MYSQL")) {
                cardinalityEstimator = new MysqlCardinalityEstimator();
            } else if (type.equals("DUCKDB")) {
                cardinalityEstimator = new DuckdbCardinalityEstimator();
            } else if (type.equals("POSTGRES_REAL")) {
                // get real cardinality through count(*) query, only use for analysis purpose
                cardinalityEstimator = new PostgresCardinalityCollector();
            }
        }
        if (cardinalityEstimator == null) {
            cardinalityEstimator = new BaseCardinalityEstimator();
        }

        cardinalityEstimator.updateWithConfig(jsonConfig);
        return cardinalityEstimator;
    }

    public static SqlDialect createSqlDialect(JSONObject jsonConfig, DataSource dataSource) {
        if (jsonConfig.has("dialect")) {
            String type = jsonConfig.getString("dialect").toUpperCase();
            if (type.equals("POSTGRES")) {
                return new MyPostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT);
            } else if (type.equals("MYSQL")) {
                return new MyMysqlSqlDialect(MysqlSqlDialect.DEFAULT_CONTEXT);
            } else if (type.equals("CLICKHOUSE")) {
                return new MyClickHouseSqlDialect(ClickHouseSqlDialect.DEFAULT_CONTEXT);
            } else if (type.equals("DATAFUSION")) {
                return new DataFusionSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT);
            } else if (type.equals("SPARK")) {
                return new SparkSqlDialect(SparkSqlDialect.DEFAULT_CONTEXT);
            } else if (type.equals("POLARS")) {
                return new PolarsSqlDialect(PolarsSqlDialect.DEFAULT_CONTEXT);
            }
        }

        return dataSource == null ? new MyPostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT) : createDialect(dataSource);
    }

    public static FedConvention createConventionFromConfig(String name, String configPath) {
        boolean isLocal = name.equals("local");
        String jsonString;
        try {
            jsonString = new String(Files.readAllBytes(Paths.get(configPath != null ? configPath: System.getenv("FED_CONFIG_PATH"), name+".json")));
            logger.info(name + ": found config!");
        } catch (Exception e) {
            logger.info(name + ": not found config! " + e);
            jsonString = "{dialect: \"postgres\"}";
        }

        JSONObject jsonConfig = new JSONObject(jsonString);
        DataSource dataSource = null;
        HashMap<String, List<String>> manualSchemaInfo = new HashMap<>();
        if (jsonConfig.has("url")) {
            dataSource = createDataSource(
                    jsonConfig.getString("url"),
                    jsonConfig.getString("driver"),
                    jsonConfig.optString("username", ""),
                    jsonConfig.optString("password", ""));
        } else if (jsonConfig.has("schema")) {
            JSONObject schema = jsonConfig.getJSONObject("schema");
            Iterator<String> keys = schema.keys();
            while (keys.hasNext()) {
                String t = keys.next();
                JSONArray cols = schema.getJSONArray(t);
                List<String> c = new ArrayList<>();
                for (int i = 0; i < cols.length(); ++i) {
                    c.add(cols.getString(i));
                }
                manualSchemaInfo.put(t, c);
            }
        }
        SqlDialect dialect = createSqlDialect(jsonConfig, dataSource);
        CostEstimator costEstimator = createFedCostEstimator(jsonConfig, isLocal);
        CardinalityEstimator cardinalityEstimator = createFedCardinalityEstimator(jsonConfig);
        PlanPartitioner planPartitioner = createPlanPartitioner(jsonConfig);

        FedConvention convention = new FedConvention(name, dataSource, dialect, costEstimator, cardinalityEstimator, planPartitioner, isLocal, manualSchemaInfo);
        convention.updateWithConfig(jsonConfig);
        return convention;
    }

    public static FedConvention createConventionFromConnection(String name, FederatedDataSource fedDataSource, String configPath) {
        DataSource dataSource = null;
        if (!fedDataSource.isManualSchema) {
            dataSource = createDataSource(fedDataSource.url, fedDataSource.driver, fedDataSource.username, fedDataSource.password);
        }

        JSONObject jsonConfig = new JSONObject();
        try {
            jsonConfig = new JSONObject(new String(Files.readAllBytes(Paths.get(configPath != null ? configPath: System.getenv("FED_CONFIG_PATH"), name+".json"))));
            logger.info(name + ": found config!");
            logger.debug(jsonConfig.toString());
        } catch (Exception e) {
            logger.info(name + ": not found config! " + e);
            // if not found config file, add some default values
            if (fedDataSource.url != null) {
                if (fedDataSource.url.startsWith("jdbc:postgresql:")) {
                    jsonConfig.put("dialect", "postgres");
                    jsonConfig.put("cardEstType", "postgres");
                    jsonConfig.put("partitionType", "postgres");
                } else if (fedDataSource.url.startsWith("jdbc:mysql:")) {
                    jsonConfig.put("cardEstType", "mysql");
                    jsonConfig.put("partitionType", "mysql");
                } else if (fedDataSource.url.startsWith("jdbc:duckdb:")) {
                    jsonConfig.put("cardEstType", "duckdb");
                }
            }

            if (!fedDataSource.isLocal) {
                JSONObject costParams = new JSONObject();
                costParams.put("trans", 10.0d);
                costParams.put("join", 2.0d);
                costParams.put("agg", 5.0d);
                costParams.put("sort", 10.0d);
                jsonConfig.put("costParams", costParams);
            }
        }


        SqlDialect dialect = createSqlDialect(jsonConfig, dataSource);
        CostEstimator costEstimator = createFedCostEstimator(jsonConfig, fedDataSource.isLocal);
        CardinalityEstimator cardinalityEstimator = createFedCardinalityEstimator(jsonConfig);
        PlanPartitioner planPartitioner = createPlanPartitioner(jsonConfig);

        FedConvention convention = new FedConvention(name, dataSource, dialect, costEstimator, cardinalityEstimator, planPartitioner, fedDataSource.isLocal, fedDataSource.schemaInfo);
        convention.updateWithConfig(jsonConfig);
        return convention;
    }

    public static FedConvention LOCAL_INSTANCE = null;
    // NOTE: only used for cardinality estimation on plans cannot handle by a specific estimator
    public static FedConvention GLOBAL_INSTANCE = new FedConvention("global", null, null, null, new BaseCardinalityEstimator(), null,false);;
    public static FedConvention CE_INSTANCE = null;


    public boolean isLocal;
    public DataSource dataSource;
    public HashMap<String, List<String>> manualSchemaInfo;
    public SqlDialect dialect;
    public CostEstimator costEstimator;
    public CardinalityEstimator cardinalityEstimator;
    public PlanPartitioner planPartitioner;

    public HashMap<String, Boolean> supportOps = new HashMap<>(Map.ofEntries(
            entry("project", true),
            entry("filter", true),
            entry("join", true),
            entry("agg", true),
            entry("sort", true)));

    public FedConvention(String name, DataSource dataSource, SqlDialect dialect, CostEstimator costEstimator, CardinalityEstimator cardinalityEstimator, PlanPartitioner planPartitioner, boolean isLocal, HashMap<String, List<String>> manualSchemaInfo) {
        super(name.toLowerCase(), FedRel.class);
        this.isLocal = isLocal;
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.costEstimator = costEstimator;
        this.cardinalityEstimator = cardinalityEstimator;
        this.planPartitioner = planPartitioner;
        this.manualSchemaInfo = manualSchemaInfo;
    }

    public FedConvention(String name, DataSource dataSource, SqlDialect dialect, CostEstimator costEstimator, CardinalityEstimator cardinalityEstimator, PlanPartitioner planPartitioner, boolean isLocal) {
        this(name, dataSource, dialect, costEstimator, cardinalityEstimator, planPartitioner, isLocal, new HashMap<>());
    }

    public void updateWithConfig(JSONObject jsonConfig) {
        if (jsonConfig.has("disableOps")) {
            JSONArray disableOps = jsonConfig.getJSONArray("disableOps");
            for (int i = 0; i < disableOps.length(); ++i) {
                String op = disableOps.getString(i);
                if (this.supportOps.containsKey(op)) {
                    this.supportOps.put(op, false);
                }
            }
        }
    }

    @Override public void register(RelOptPlanner planner) {
        for (RelOptRule rule : FedRelRules.rules(this)) {
            planner.addRule(rule);
        }
        planner.addRule(CoreRules.FILTER_SET_OP_TRANSPOSE);
        planner.addRule(CoreRules.PROJECT_REMOVE);
    }

    public boolean supportOperation(RelNode rel) {
        if (rel instanceof Project) return this.supportOps.get("project");
        if (rel instanceof Filter) return this.supportOps.get("filter");
        if (rel instanceof Join) return this.supportOps.get("join");
        if (rel instanceof Aggregate) return this.supportOps.get("agg");
        if (rel instanceof Sort) return this.supportOps.get("sort");
        return (rel instanceof TableScan);
    }
}
