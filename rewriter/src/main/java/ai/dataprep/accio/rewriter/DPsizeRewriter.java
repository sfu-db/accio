package ai.dataprep.accio.rewriter;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.rules.*;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DPsizeRewriter {
    private static final Logger logger = LoggerFactory.getLogger(DPsizeRewriter.class);

    public RelNode rewrite(RelNode logPlan, VolcanoPlanner planner, RelOptCluster cluster, boolean isPlain) {
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

        // aggregation pushdown
        // [Note]: cannot do this since we may lose the project that can be pushdown to `RemoteToLocalConverter` after join reorder
//        {
//            HepProgramBuilder builder = new HepProgramBuilder();
//            builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
//            builder.addRuleCollection(ImmutableList.of(
//                    CoreRules.AGGREGATE_PROJECT_MERGE,
//                    CoreRules.AGGREGATE_JOIN_TRANSPOSE
//            ));
//            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
//            hepPlanner.setRoot(logPlan);
//            logPlan = hepPlanner.findBestExp();
//            logger.debug(
//                    RelOptUtil.dumpPlan("[Aggregation pushdown]", logPlan, SqlExplainFormat.TEXT,
//                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
//        }

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
            if (isPlain) {
                builder.addRuleCollection(ImmutableList.of(
                        MultiJoinConditionRule.Config.DEFAULT.toRule(),
                        FedMultiJoinDPsizeRule.Config.PLAIN.toRule()
                ));
            } else {
                builder.addRuleCollection(ImmutableList.of(
                        MultiJoinConditionRule.Config.DEFAULT.toRule(),
                        FedMultiJoinDPsizeRule.Config.DEFAULT.toRule()
                ));
            }

            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Optimize multi join]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        // Pushdown Project
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
            builder.addRuleCollection(ImmutableList.of(
                    CoreRules.PROJECT_MERGE,
                    ProjectJoinTransposeRule.Config.DEFAULT.withOperandFor(Project.class, Join.class).toRule()
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Final project pushdown (step 1)]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }
        {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
            builder.addRuleCollection(ImmutableList.of(
                    ProjectConverterTransposeRule.Config.DEFAULT.toRule()
            ));
            HepPlanner hepPlanner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
            logger.debug(
                    RelOptUtil.dumpPlan("[Final project pushdown (step 2)]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        logger.info("Optimization finished, took {}ms\n", System.currentTimeMillis()-startTime);

        startTime = System.currentTimeMillis();


        // [NOTE]: Since we have already fixed the physical subplan of `MultiJoin`
        // the lower part of the plan does not need accurate cardinality estimation nor cost estimation any more
        // thus we will route back to heuristic when estimating those nodes.
        // However, the root node of the subplan itself contains "accurate" (derived by the customized estimator) cardinality and cost
        // for further decision-making on the rest of the tree
        FedConvention.LOCAL_INSTANCE.register(planner);
        logPlan = planner.changeTraits(logPlan, cluster.traitSet().replace(FedConvention.LOCAL_INSTANCE));
        planner.setRoot(logPlan);

        // Start the optimization process to obtain the most efficient physical plan
        logger.debug("[Rules]\n{}\n", planner.getRules());
        RelNode phyPlan = planner.findBestExp();

        // check the note above, in the graph, lower nodes' cost and # rows may be heuristic which are not really used during join ordering and pushdown
//        logger.trace("Graph:\n{}", planner.toDot());
        logger.info("DPsizeRewriter finished, took {}ms\n", System.currentTimeMillis()-startTime);
        return phyPlan;
    }
}
