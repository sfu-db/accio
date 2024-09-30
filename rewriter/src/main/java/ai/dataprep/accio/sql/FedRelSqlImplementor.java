package ai.dataprep.accio.sql;

import ai.dataprep.accio.FedRelMetadataProvider;
import ai.dataprep.accio.partition.PlanPartitioner;
import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTable;
import ai.dataprep.accio.rules.JoinExtractFilterRule;
import ai.dataprep.accio.rules.JoinProjectTransposeRule;
import ai.dataprep.accio.utils.CleanPlanExtractor;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class FedRelSqlImplementor extends RelToSqlConverter {
    private static final Logger logger = LoggerFactory.getLogger(FedRelSqlImplementor.class);
    /**
     * Creates a RelToSqlConverter.
     *
     * @param dialect
     */
    public FedRelSqlImplementor(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    protected boolean isAnon() {
        return false;
    }

    public Result visit(RelSubset e) {
        RelNode rel = e.getOriginal();
        return dispatch(rel);
    }

    public Result visit(HepRelVertex e) {
        RelNode rel = e.getCurrentRel();
        return dispatch(rel);
    }

    /** Visits a TableScan; called by {@link #dispatch} via reflection. */
    public Result visit(TableScan e) {
        final SqlIdentifier identifier = getSqlTargetTable(e);
        final SqlNode node;
        final ImmutableList<RelHint> hints = e.getHints();
        if (!hints.isEmpty()) {
            SqlParserPos pos = identifier.getParserPosition();
            node = new SqlTableRef(pos, identifier,
                    SqlNodeList.of(pos, hints.stream().map(h -> toSqlHint(h, pos))
                            .collect(Collectors.toList())));
        } else {
            node = identifier;
        }
        return result(node, ImmutableList.of(Clause.FROM), e, null);
    }

    private static SqlIdentifier getSqlTargetTable(RelNode e) {
        // Use the foreign catalog, schema and table names, if they exist,
        // rather than the qualified name of the shadow table in Calcite.
        final RelOptTable table = requireNonNull(e.getTable());
        return table.maybeUnwrap(FedTable.class)
                .map(FedTable::tableName)
                .orElseGet(() ->
                        new SqlIdentifier(table.getQualifiedName(), SqlParserPos.ZERO));
    }
    private static SqlHint toSqlHint(RelHint hint, SqlParserPos pos) {
        if (hint.kvOptions != null) {
            return new SqlHint(pos, new SqlIdentifier(hint.hintName, pos),
                    SqlNodeList.of(pos, hint.kvOptions.entrySet().stream()
                            .flatMap(
                                    e -> Stream.of(new SqlIdentifier(e.getKey(), pos),
                                            SqlLiteral.createCharString(e.getValue(), pos)))
                            .collect(Collectors.toList())),
                    SqlHint.HintOptionFormat.KV_LIST);
        } else if (hint.listOptions != null) {
            return new SqlHint(pos, new SqlIdentifier(hint.hintName, pos),
                    SqlNodeList.of(pos, hint.listOptions.stream()
                            .map(e -> SqlLiteral.createCharString(e, pos))
                            .collect(Collectors.toList())),
                    SqlHint.HintOptionFormat.LITERAL_LIST);
        }
        return new SqlHint(pos, new SqlIdentifier(hint.hintName, pos),
                SqlNodeList.EMPTY, SqlHint.HintOptionFormat.EMPTY);
    }

    /**
     * Transformation before convert to sql
    */
    public RelNode prepare(RelNode r, SqlDialect dialect) {
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
            builder.addRuleCollection(ImmutableList.of(
                    CoreRules.FILTER_PROJECT_TRANSPOSE,
                    JoinProjectTransposeRule.Config.OUTER.toRule(),
                    JoinProjectTransposeRule.Config.LEFT_OUTER.toRule(),
                    JoinProjectTransposeRule.Config.RIGHT_OUTER.toRule(),
                    CoreRules.PROJECT_MERGE,
                    CoreRules.PROJECT_REMOVE,
                    CoreRules.FILTER_MERGE
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build());

            hepPlanner.setRoot(r);
            r = hepPlanner.findBestExp();
        }

        if (dialect instanceof ClickHouseSqlDialect){
            // clickhouse does not support join on operators other than '='
            {
                HepProgramBuilder builder = new HepProgramBuilder();
                builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
                builder.addRuleCollection(ImmutableList.of(
                        JoinExtractFilterRule.Config.DEFAULT.toRule()
                ));
                HepPlanner hepPlanner = new HepPlanner(builder.build());

                hepPlanner.setRoot(r);
                r = hepPlanner.findBestExp();
            }
        }

        return r;
    }

    public static String ConvertToRemoteSql(FedConvention convention, RelNode node, PlanPartitioner.PartitionScheme partitionScheme) {
        FedRelSqlImplementor sqlConverter = new FedRelSqlImplementor(convention.dialect);
        // Check partition
        if (partitionScheme != null) {
            String partSql = convention.planPartitioner.doPartitionPlan(convention, node, partitionScheme);
            requireNonNull(partSql, "partSql");
            return partSql;
        }

        // Some preprocessing before generate sql (e.g., pull up projection for read-friendly)
        RelNode finalPlan = sqlConverter.prepare(node, convention.dialect);

        // Convert to SQL
        RelToSqlConverter.Result res = sqlConverter.visitRoot(finalPlan);
        SqlNode resSqlNode = res.asQueryOrValues();
        return resSqlNode.toSqlString(convention.dialect).getSql();
    }

    public Result visitRoot2(RelNode r) {
        try {
            return visitInput(new SingleRel(r.getCluster(), r.getTraitSet(), r){}, 0);
        } catch (Error | RuntimeException e) {
            throw Util.throwAsRuntime("Error while converting RelNode to SqlNode:\n"
                    + RelOptUtil.toString(r), e);
        }
    }

    public static String ConvertToRemoteSql2(FedConvention convention, RelNode node) {
        FedRelSqlImplementor sqlConverter = new FedRelSqlImplementor(convention.dialect);

        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addRuleCollection(ImmutableList.of(
                // MySQL needs this since its estimated cardinality won't use histograms if query is nested
                // for example:
                // SELECT * FROM (SELECT c1, c2 FROM r) WHERE c1 > 10 -> CE: 30
                // SELECT c1, c2 FROM r WHERE c1 > 10 -> CE: 10
                CoreRules.FILTER_PROJECT_TRANSPOSE
        ));
        HepPlanner hepPlanner = new HepPlanner(builder.build());

        // avoid 1) calling HepPlanner here to avoid cycle (getRowCount -> HepPlanner -> getRowCount -> ...), 2) invalidate metadata query
        // 1. create a new cluster
        // 2. copy the entire plan with the new cluster
        // 3. apply HepPlanner (it will invalidate the metadata query of the new cluster, but not the old one)
        // 4. rest metadata provider (THREAD_PROVIDERS)
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, new RexBuilder(new SqlTypeFactoryImpl(new RelDataTypeSystemImpl() { })));
        CleanPlanExtractor planExtractor = new CleanPlanExtractor(cluster);
        RelNode copyNode = planExtractor.extract(node);

        hepPlanner.setRoot(copyNode);
        copyNode = hepPlanner.findBestExp();

        // has to reset the metadata provider since the THREAD_PROVIDERS is rested in `RelOptCluster.create`
        node.getCluster().setMetadataProvider(FedRelMetadataProvider.INSTANCE);

        RelToSqlConverter.Result res = sqlConverter.visitRoot2(copyNode);
        SqlNode resSqlNode = res.asQueryOrValues();
        return resSqlNode.toSqlString(convention.dialect).getSql();
    }
}
