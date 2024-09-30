package ai.dataprep.accio;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedSchema;
import ai.dataprep.accio.rewriter.DPsizeRewriter;
import ai.dataprep.accio.rewriter.GOORewriter;
import ai.dataprep.accio.rewriter.PDBenefitRewriter;
import ai.dataprep.accio.rewriter.PushdownRewriter;
import ai.dataprep.accio.rules.*;
import ai.dataprep.accio.sql.FedRelSqlImplementor;
import ai.dataprep.accio.sql.UdfRTrim;
import ai.dataprep.accio.utils.LocalOnlyChecker;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;


public class FederatedQueryRewriter {

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query,
                                                                   schema, path) -> null;
    private static final Logger logger = LoggerFactory.getLogger(FederatedQueryRewriter.class);

    public static FedSchema createFedSchema(FedConvention convention) {
        // schema through jdbc
        if (convention.dataSource != null) {
            return new FedSchema(convention.dataSource, convention.dialect, convention, null, null);
        }
        // empty schema
        return new FedSchema(convention, convention.manualSchemaInfo, null, null);
    }

    public static void registerDataSourcesFromConfig(SchemaPlus rootSchema, List<String> dbs, String configPath) {
        long startTime = System.currentTimeMillis();
        // register remote
        for (String name: dbs) {
            if (name.equalsIgnoreCase("local")) {
                continue;
            }
            FedConvention convention = FedConvention.createConventionFromConfig(name, configPath);
            Schema schema = createFedSchema(convention);
            rootSchema.add(name, schema);
        }
        logger.info("Data source register finished (config for {}), took {}ms\n", dbs, System.currentTimeMillis() - startTime);
    }

    public static void registerDataSourcesFromConnection(SchemaPlus rootSchema, HashMap<String, FederatedDataSource> dbs, String configPath) {
        long startTime = System.currentTimeMillis();
        for (Map.Entry<String, FederatedDataSource> entry: dbs.entrySet()) {
            String name = entry.getKey();
            FederatedDataSource fedDataSource = entry.getValue();
            FedConvention convention = FedConvention.createConventionFromConnection(name, fedDataSource, configPath);
            if (fedDataSource.isLocal) {
                assert FedConvention.LOCAL_INSTANCE == null; // can only have one local instance
                FedConvention.LOCAL_INSTANCE = convention;
                Schema schema = createFedSchema(FedConvention.LOCAL_INSTANCE);
                // Add tables to root schema directly
                for (String tableName: schema.getTableNames()) {
                    rootSchema.add(tableName, schema.getTable(tableName));
                }
            } else {
                Schema schema = createFedSchema(convention);
                rootSchema.add(name, schema);
            }
        }
        logger.info("Data source register finished (manual for {}), took {}ms\n", dbs.keySet(), System.currentTimeMillis() - startTime);
    }

    public static void registerLocalDataSource(SchemaPlus rootSchema, String configPath) {
        if (FedConvention.LOCAL_INSTANCE == null) {
            logger.info("Data source register: init fake local");
            FedConvention.LOCAL_INSTANCE = FedConvention.createConventionFromConfig("local", configPath);
            Schema schema = createFedSchema(FedConvention.LOCAL_INSTANCE);
            // Add tables to root schema directly
            for (String tableName: schema.getTableNames()) {
                rootSchema.add(tableName, schema.getTable(tableName));
            }
        }
    }

    public static SchemaPlus getRootSchema(List<String> dbs_config, HashMap<String, FederatedDataSource> dbs_manual, String configPath) throws SQLException {
        // Configure connection and schema via JDBC
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // Register data sources to root schema
        FedConvention.LOCAL_INSTANCE = null;
        if (dbs_config != null) {
            registerDataSourcesFromConfig(rootSchema, dbs_config, configPath);
        }
        if (dbs_manual != null) {
            registerDataSourcesFromConnection(rootSchema, dbs_manual, configPath);
        }
        registerLocalDataSource(rootSchema, configPath);

        return rootSchema;
    }

    public AccioSession createAccioSession(List<String> dbs_config, HashMap<String, FederatedDataSource> dbs_manual) throws SQLException {
        return new AccioSession(getRootSchema(dbs_config, dbs_manual, null));
    }

    public AccioSession createAccioSession(List<String> dbs_config, HashMap<String, FederatedDataSource> dbs_manual, String configPath) throws SQLException {
        return new AccioSession(getRootSchema(dbs_config, dbs_manual, configPath));
    }

    public FederatedPlan rewrite(String sql, List<String> dbs_config, HashMap<String, FederatedDataSource> dbs_manual, String strategy) throws Exception {
        return rewrite(sql, dbs_config, dbs_manual, null, strategy);
    }

    public FederatedPlan rewrite(String sql, List<String> dbs_config, HashMap<String, FederatedDataSource> dbs_manual, String configPath, String strategy) throws Exception {
        final SchemaPlus rootSchema = getRootSchema(dbs_config, dbs_manual, configPath);
        return startRewrite(sql, rootSchema, strategy);
    }

    public FederatedPlan rewrite(String sql, AccioSession session, String strategy) throws Exception {
        return startRewrite(sql, session.rootSchema, strategy);
    }

    public FederatedPlan startRewrite(String sql, SchemaPlus rootSchema, String strategy) throws Exception {
        long startTime = System.currentTimeMillis();

        // Parse the query into an AST
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        logger.debug("[Parsed query]\n{}\n", sqlNode.toString());
        logger.info("Query parsed, took {}ms\n", System.currentTimeMillis()-startTime);
        startTime = System.currentTimeMillis();


        // Configure validator
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema.from(rootSchema),
                new ArrayList<>(), // use root schema by default
                typeFactory, config);
        SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
        // Add UDFs
        sqlStdOperatorTable.register(UdfRTrim.INSTANCE);
        SqlValidator validator = SqlValidatorUtil.newValidator(sqlStdOperatorTable,
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT.withConformance(SqlConformanceEnum.LENIENT));

        // Validate the initial AST
        SqlNode validNode = validator.validate(sqlNode);
        logger.debug("[Validated query]\n{}\n", validNode.toString());
        logger.info("Query validated, took {}ms\n", System.currentTimeMillis()-startTime);
        startTime = System.currentTimeMillis();

        // Configure planner cluster
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        cluster.setMetadataProvider(FedRelMetadataProvider.INSTANCE);

        // Configure SqlToRelConverter
        SqlToRelConverter relConverter = new SqlToRelConverter(
                NOOP_EXPANDER, // do not use views
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        // Convert the valid AST into a logical plan
        RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;
        logger.debug(
                RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        logger.info("Logical plan generated, took {}ms\n", System.currentTimeMillis()-startTime);
        startTime = System.currentTimeMillis();

        // If the query does not involve external data sources, return directly
        LocalOnlyChecker checkLocalOnlyVisitor = new LocalOnlyChecker();
        checkLocalOnlyVisitor.go(logPlan);
        if (!checkLocalOnlyVisitor.isHasRemote()) {
            // Do not rewrite the query if it is not a federated one
            FederatedPlan plan = new FederatedPlan();
            plan.add("LOCAL", sql);
            return plan;
        }

//        // Hack: do not rewrite queries with multiple correlated subqueries
//        DegradeTriggerChecker degradeTrigger = new DegradeTriggerChecker();
//        degradeTrigger.go(logPlan);
//        if (degradeTrigger.shouldDegrade()) {
//            SqlToRelConverter naiveConverter = new SqlToRelConverter(
//                    NOOP_EXPANDER, // do not use views
//                    validator,
//                    catalogReader,
//                    cluster,
//                    StandardConvertletTable.INSTANCE,
//                SqlToRelConverter.config().withExpand(false));
//
//            // Convert the valid AST into a logical plan
//            RelNode naiveLogPlan = naiveConverter.convertQuery(validNode, false, true).rel;
//            logger.debug(
//                    RelOptUtil.dumpPlan("[Naive plan]", naiveLogPlan, SqlExplainFormat.TEXT,
//                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
//
//            // Generate naive plan
//            NaivePlanGenerator generator = new NaivePlanGenerator();
//            generator.go(naiveLogPlan);
//            logger.debug(
//                    RelOptUtil.dumpPlan("[Remaining naive plan]", naiveLogPlan, SqlExplainFormat.TEXT,
//                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
//
//            FederatedPlan plan = generator.getPlan();
//            plan.add("LOCAL", generator.rewrite(sql));
//
//            return plan;
//        }

//        logger.info("Plan checked, took {}ms\n", System.currentTimeMillis()-startTime);
//        startTime = System.currentTimeMillis();

        // remove subqueries to correlate
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
            builder.addRuleCollection(ImmutableList.of(
                    CoreRules.FILTER_SUB_QUERY_TO_CORRELATE
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Subquery removal]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        // De-correlate subqueries (Remove rel `LogicalCorrelate`)
        logPlan = relConverter.decorrelate(validNode, logPlan);
        logger.debug(
                RelOptUtil.dumpPlan("[De-correlated plan]", logPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        logger.info("Plan decorrelated, took {}ms\n", System.currentTimeMillis()-startTime);
        startTime = System.currentTimeMillis();

        // Add patch to decorrelation / subquery
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
            builder.addRuleCollection(ImmutableList.of(
                    PatchCreateExistRule.Config.DEFAULT.toRule()
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[After patch]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        // Start rewrite
        RelNode phyPlan = null;
        if (strategy.equalsIgnoreCase("goo")) {
            GOORewriter rewriter = new GOORewriter();
            phyPlan = rewriter.rewrite(logPlan, planner, cluster);
        } else if (strategy.equalsIgnoreCase("dpsize")) {
            DPsizeRewriter rewriter = new DPsizeRewriter();
            phyPlan = rewriter.rewrite(logPlan, planner, cluster, false);
        } else if (strategy.equalsIgnoreCase("dpsize_plain")) {
            DPsizeRewriter rewriter = new DPsizeRewriter();
            phyPlan = rewriter.rewrite(logPlan, planner, cluster, true);
        } else if (strategy.equalsIgnoreCase("pushdown")) {
            PushdownRewriter rewriter = new PushdownRewriter();
            phyPlan = rewriter.rewrite(logPlan, planner, cluster);
        } else if (strategy.equalsIgnoreCase("benefit")) {
            PDBenefitRewriter rewriter = new PDBenefitRewriter();
            phyPlan = rewriter.rewrite(logPlan, planner, cluster, false);
        } else if (strategy.equalsIgnoreCase("benefit_plain")) {
            PDBenefitRewriter rewriter = new PDBenefitRewriter();
            phyPlan = rewriter.rewrite(logPlan, planner, cluster, true);
        } else {
            throw new Exception("Strategy " + strategy + " not supported!");
        }
        startTime = System.currentTimeMillis();
        logger.debug(
                RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // Convert physical plan to sql
        DBSourceVisitor visitor = new DBSourceVisitor();
        phyPlan = visitor.go(phyPlan);
        logger.debug(
                RelOptUtil.dumpPlan("[Remaining physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        String resSql = FedRelSqlImplementor.ConvertToRemoteSql(FedConvention.LOCAL_INSTANCE, phyPlan, null);
        FederatedPlan plan = visitor.getPlan();
        plan.add("LOCAL", resSql);

        logger.info("Federated plan generated, took {}ms\n", System.currentTimeMillis()-startTime);

        return plan;
    }

    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(new FederatedQueryRewriter());
        gatewayServer.start();
        logger.info("Gateway Server Started");
    }
}
