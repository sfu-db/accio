package ai.dataprep.accio.rewriter;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.rules.*;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GOORewriter {
    private static final Logger logger = LoggerFactory.getLogger(GOORewriter.class);

    public RelNode rewrite(RelNode logPlan, VolcanoPlanner planner, RelOptCluster cluster) {
        long startTime = System.currentTimeMillis();

        // Project pushdown
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
            builder.addRuleCollection(ImmutableList.of(
                    CoreRules.PROJECT_JOIN_TRANSPOSE,
                    CoreRules.PROJECT_FILTER_TRANSPOSE,
                    CoreRules.PROJECT_MERGE
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Project pushdown]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        // Predicate pushdown
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
            builder.addRuleCollection(ImmutableList.of(
                    CoreRules.FILTER_PROJECT_TRANSPOSE,
                    CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                    CoreRules.FILTER_INTO_JOIN,
                    CoreRules.JOIN_CONDITION_PUSH,
                    ConditionRules.FilterConditionRule.FilterConfig.DEFAULT.toRule(),
                    ConditionRules.JoinConditionRule.JoinConfig.DEFAULT.toRule()
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Predicate pushdown]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        // Convert to multi-join
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
            builder.addRuleCollection(ImmutableList.of(
                    CoreRules.MULTI_JOIN_BOTH_PROJECT,
                    CoreRules.MULTI_JOIN_LEFT_PROJECT,
                    CoreRules.MULTI_JOIN_RIGHT_PROJECT,
                    CoreRules.JOIN_TO_MULTI_JOIN,
                    CoreRules.PROJECT_MULTI_JOIN_MERGE,
                    CoreRules.FILTER_MULTI_JOIN_MERGE
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Multi join]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        // Join re-order
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
            builder.addRuleCollection(ImmutableList.of(
                    MultiJoinConditionRule.Config.DEFAULT.toRule(),
                    FedMultiJoinGOORule.Config.DEFAULT.toRule()
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Optimize multi join]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        // Other operations (e.g., project, aggregation) pushdown
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
            builder.addRuleCollection(ImmutableList.of(
                    CoreRules.PROJECT_JOIN_TRANSPOSE,
                    CoreRules.PROJECT_FILTER_TRANSPOSE
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Project pushdown 1]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
            builder.addRuleCollection(ImmutableList.of(
                    CoreRules.PROJECT_MERGE
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Project pushdown 2]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        logger.info("Rule-based optimization finished, took {}ms\n", System.currentTimeMillis()-startTime);
        startTime = System.currentTimeMillis();

        // Initialize optimizer/planner with the necessary rules
        // Logical -> Physical rules
        // Define the type of the output plan (in this case we want a physical plan in EnumerableConvention)
        FedConvention.LOCAL_INSTANCE.register(planner);

        // The node in logPlan should all be logical other than TableScan,
        // otherwise `RelMdRowCount` might not able to find the equivalent logical counterpart
        logPlan = planner.changeTraits(logPlan, cluster.traitSet().replace(FedConvention.LOCAL_INSTANCE));
        planner.setRoot(logPlan);

        // Start the optimization process to obtain the most efficient physical plan
        // NOTE: we are using different heuristic to estimate cross-site join cardinality here and inside the multi-join rule
        //       however, in GOO since we already fixed join order, cross-site joins in the plan only have one physical mapping
        //       thus, its cardinality does not matter
        logger.debug("[Rules]\n{}\n", planner.getRules());
        RelNode phyPlan = planner.findBestExp();

//        logger.debug("Graph:\n{}", planner.toDot());
        logger.info("Cost-based optimization finished, took {}ms\n", System.currentTimeMillis()-startTime);
        return phyPlan;
    }
}
