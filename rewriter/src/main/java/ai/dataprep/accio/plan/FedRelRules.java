package ai.dataprep.accio.plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class FedRelRules {
    private FedRelRules() {
    }
    protected static final Logger LOGGER = LoggerFactory.getLogger(FedRelRules.class);

    /** Creates a list of rules with the given FedRel convention instance. */
    public static List<RelOptRule> rules(FedConvention out) {
        final ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
        foreachRule(out, b::add);
        return b.build();
    }

    /** Creates a list of rules with the given FedRel convention instance
     * and builder factory. */
    public static List<RelOptRule> rules(FedConvention out,
                                         RelBuilderFactory relBuilderFactory) {
        final ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
        foreachRule(out, r ->
                b.add(r.config.withRelBuilderFactory(relBuilderFactory).toRule()));
        return b.build();
    }

    private static void foreachRule(FedConvention out,
                                    Consumer<RelRule<?>> consumer) {
        if (!out.isLocal) {
            consumer.accept(RemoteToLocalConverterRule.create(out));
        }
        if (out.supportOps.get("project")) {
            consumer.accept(FedRelRules.FedRelProjectRule.create(out));
        }
        if (out.supportOps.get("filter")) {
            consumer.accept(FedRelRules.FedRelFilterRule.create(out));
        }
        if (out.supportOps.get("join")) {
            consumer.accept(FedRelRules.FedRelJoinRule.create(out));
        }
        if (out.supportOps.get("agg")) {
            consumer.accept(FedRelRules.FedRelAggregateRule.create(out));
        }
        if (out.supportOps.get("sort")) {
            consumer.accept(FedRelRules.FedRelSortRule.create(out));
        }

//        consumer.accept(FedRelRules.FedRelUnionRule.create(out));
//        consumer.accept(FedRelRules.FedRelIntersectRule.create(out));
//        consumer.accept(FedRelRules.FedRelMinusRule.create(out));
//        consumer.accept(FedRelRules.FedRelValuesRule.create(out));
    }

    /** Abstract base class for rule that converts to FedRel. */
    abstract static class FedRelConverterRule extends ConverterRule {
        protected FedRelConverterRule(Config config) {
            super(config);
        }
    }

    /** Rule that converts a join to FedRel. */
    public static class FedRelJoinRule extends FedRelRules.FedRelConverterRule {
        /** Creates a FedRelJoinRule. */
        public static FedRelRules.FedRelJoinRule create(FedConvention out) {
            return Config.INSTANCE
                    .withConversion(Join.class, Convention.NONE, out, "RemoteFedRelJoinRule")
                    .withRuleFactory(FedRelRules.FedRelJoinRule::new)
                    .toRule(FedRelRules.FedRelJoinRule.class);
        }

        /** Called from the Config. */
        protected FedRelJoinRule(Config config) {
            super(config);
        }

        @Override public @Nullable RelNode convert(RelNode rel) {
            final Join join = (Join) rel;
            switch (join.getJoinType()) {
                case SEMI:
                case ANTI:
                    // It's not possible to convert semi-joins or anti-joins. They have fewer columns
                    // than regular joins.
                    return null;
                default:
                    return convert(join, true);
            }
        }

        /**
         * Converts a {@code Join} into a {@code FedRelJoin}.
         *
         * @param join Join operator to convert
         * @param convertInputTraits Whether to convert input to {@code join}'s
         *                            FedRel convention
         * @return A new FedRelJoin
         */
        public @Nullable RelNode convert(Join join, boolean convertInputTraits) {
            final List<RelNode> newInputs = new ArrayList<>();
            for (RelNode input : join.getInputs()) {
                if (convertInputTraits && input.getConvention() != getOutTrait()) {
                    input =
                            convert(input,
                                    input.getTraitSet().replace(out));
                }
                newInputs.add(input);
            }
            if (convertInputTraits && !canJoinOnCondition(join.getCondition())) {
                return null;
            }
            try {
                return new FedRelRules.FedRelJoin(
                        join.getCluster(),
                        join.getTraitSet().replace(out),
                        newInputs.get(0),
                        newInputs.get(1),
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType());
            } catch (InvalidRelException e) {
                LOGGER.debug(e.toString());
                return null;
            }
        }

        /**
         * Returns whether a condition is supported by {@link FedRelRules.FedRelJoin}.
         *
         * <p>Corresponds to the capabilities of
         * {@link SqlImplementor#convertConditionToSqlNode}.
         *
         * @param node Condition
         * @return Whether condition is supported
         */
        private static boolean canJoinOnCondition(RexNode node) {
            final List<RexNode> operands;
            switch (node.getKind()) {
                case AND:
                case OR:
                    operands = ((RexCall) node).getOperands();
                    if ((operands.get(0) instanceof RexInputRef)
                            && (operands.get(1) instanceof RexInputRef)) {
                        return true;
                    }
                    for (RexNode operand : operands) {
                        if (!canJoinOnCondition(operand)) {
                            return false;
                        }
                    }
                    return true;

                case EQUALS:
                case IS_NOT_DISTINCT_FROM:
                case NOT_EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    operands = ((RexCall) node).getOperands();
                    if ((operands.get(0) instanceof RexInputRef)
                            && (operands.get(1) instanceof RexInputRef)) {
                        return true;
                    }
                case LITERAL:
                    // NOTE: we assume all systems support cartesian product currently
                    // if this is not the case in the future, we can add a configuration for this
                    return true;
                    // fall through
                default:
                    return false;
            }
        }

        @Override public boolean matches(RelOptRuleCall call) {
            Join join = call.rel(0);
            JoinRelType joinType = join.getJoinType();
            return ((FedConvention) getOutConvention()).dialect.supportsJoinType(joinType);
        }
    }

    /** Join operator implemented in FedRel convention. */
    public static class FedRelJoin extends Join implements FedRel {
        /** Creates a FedRelJoin. */
        public Double rowCount;

        public FedRelJoin(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode left, RelNode right, RexNode condition,
                        Set<CorrelationId> variablesSet, JoinRelType joinType)
                throws InvalidRelException {
            super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
            this.rowCount = null;
        }

        @Deprecated // to be removed before 2.0
        protected FedRelJoin(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode left,
                RelNode right,
                RexNode condition,
                JoinRelType joinType,
                Set<String> variablesStopped)
                throws InvalidRelException {
            this(cluster, traitSet, left, right, condition,
                    CorrelationId.setOf(variablesStopped), joinType);
        }

        @Override public FedRelRules.FedRelJoin copy(RelTraitSet traitSet, RexNode condition,
                                                   RelNode left, RelNode right, JoinRelType joinType,
                                                   boolean semiJoinDone) {
            try {
                FedRelRules.FedRelJoin rel = new FedRelRules.FedRelJoin(getCluster(), traitSet, left, right,
                        condition, variablesSet, joinType);
                rel.rowCount = this.rowCount;
                return rel;
            } catch (InvalidRelException e) {
                // Semantic error not possible. Must be a bug. Convert to
                // internal error.
                throw new AssertionError(e);
            }
        }

        public double estimateLeftRowCount(RelMetadataQuery mq) {
            return left.estimateRowCount(mq);
        }

        public double estimateRightRowCount(RelMetadataQuery mq) {
            return right.estimateRowCount(mq);
        }

        @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                                              RelMetadataQuery mq) {
            return ((FedConvention) getConvention()).costEstimator.computeCost(this, planner, mq);
        }

        @Override public double estimateRowCount(RelMetadataQuery mq) {
            if (this.rowCount != null) return rowCount;

            final double leftRowCount = left.estimateRowCount(mq);
            final double rightRowCount = right.estimateRowCount(mq);
            return Math.max(leftRowCount, rightRowCount);
        }

        @Override public String getRelTypeName() {
            return super.getRelTypeName() + "[" + getConvention().getName() + "]";
        }

        @Override
        public void setRowCount(Double rowCount) {
            this.rowCount = rowCount;
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Project} to
     * an {@link FedRelRules.FedRelProject}.
     */
    public static class FedRelProjectRule extends FedRelRules.FedRelConverterRule {
        /** Creates a FedRelProjectRule. */
        public static FedRelRules.FedRelProjectRule create(FedConvention out) {
            return Config.INSTANCE
                    .withConversion(Project.class, project ->
                                    (out.dialect.supportsWindowFunctions()
                                            || !project.containsOver())
                                            && !userDefinedFunctionInProject(project),
                            Convention.NONE, out, "RemoteFedRelProjectRule")
                    .withRuleFactory(FedRelRules.FedRelProjectRule::new)
                    .toRule(FedRelRules.FedRelProjectRule.class);
        }

        /** Called from the Config. */
        protected FedRelProjectRule(Config config) {
            super(config);
        }

        private static boolean userDefinedFunctionInProject(Project project) {
            FedRelRules.CheckingUserDefinedFunctionVisitor visitor = new FedRelRules.CheckingUserDefinedFunctionVisitor();
            for (RexNode node : project.getProjects()) {
                node.accept(visitor);
                if (visitor.containsUserDefinedFunction()) {
                    return true;
                }
            }
            return false;
        }

        @Override public @Nullable RelNode convert(RelNode rel) {
            final Project project = (Project) rel;

            return new FedRelRules.FedRelProject(
                    rel.getCluster(),
                    rel.getTraitSet().replace(out),
                    convert(
                            project.getInput(),
                            project.getInput().getTraitSet().replace(out)),
                    project.getProjects(),
                    project.getRowType());
        }
    }

    /** Implementation of {@link org.apache.calcite.rel.core.Project} in
     * {@link FedConvention FedRel calling convention}. */
    public static class FedRelProject
            extends Project
            implements FedRel {
        public Double rowCount;

        public FedRelProject(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                List<? extends RexNode> projects,
                RelDataType rowType) {
            super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
            assert getConvention() instanceof FedConvention;
            this.rowCount = null;
        }

        @Deprecated // to be removed before 2.0
        public FedRelProject(RelOptCluster cluster, RelTraitSet traitSet,
                           RelNode input, List<RexNode> projects, RelDataType rowType, int flags) {
            this(cluster, traitSet, input, projects, rowType);
            Util.discard(flags);
        }

        @Override public FedRelRules.FedRelProject copy(RelTraitSet traitSet, RelNode input,
                                                      List<RexNode> projects, RelDataType rowType) {
            FedRelRules.FedRelProject rel = new FedRelRules.FedRelProject(getCluster(), traitSet, input, projects, rowType);
            rel.rowCount = this.rowCount;
            return rel;
        }

        @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                                              RelMetadataQuery mq) {
            return ((FedConvention)getConvention()).costEstimator.computeCost(this, planner, mq);
        }

        @Override public String getRelTypeName() {
            return super.getRelTypeName() + "[" + getConvention().getName() + "]";
        }

        @Override
        public double estimateRowCount(RelMetadataQuery mq) {
            if (this.rowCount != null) return this.rowCount;
            return super.estimateRowCount(mq);
        }

        @Override
        public void setRowCount(Double rowCount) {
            this.rowCount = rowCount;
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Filter} to
     * an {@link FedRelRules.FedRelFilter}.
     */
    public static class FedRelFilterRule extends FedRelRules.FedRelConverterRule {
        /** Creates a FedRelFilterRule. */
        public static FedRelRules.FedRelFilterRule create(FedConvention out) {
            return Config.INSTANCE
                    .withConversion(Filter.class, r -> !userDefinedFunctionInFilter(r),
                            Convention.NONE, out, "RemoteFedRelFilterRule")
                    .withRuleFactory(FedRelRules.FedRelFilterRule::new)
                    .toRule(FedRelRules.FedRelFilterRule.class);
        }

        /** Called from the Config. */
        protected FedRelFilterRule(Config config) {
            super(config);
        }

        private static boolean userDefinedFunctionInFilter(Filter filter) {
            FedRelRules.CheckingUserDefinedFunctionVisitor visitor = new FedRelRules.CheckingUserDefinedFunctionVisitor();
            filter.getCondition().accept(visitor);
            return visitor.containsUserDefinedFunction();
        }

        @Override public @Nullable RelNode convert(RelNode rel) {
            final Filter filter = (Filter) rel;

            return new FedRelRules.FedRelFilter(
                    rel.getCluster(),
                    rel.getTraitSet().replace(out),
                    convert(filter.getInput(),
                            filter.getInput().getTraitSet().replace(out)),
                    filter.getCondition());
        }
    }

    /** Implementation of {@link org.apache.calcite.rel.core.Filter} in
     * {@link FedConvention FedRel calling convention}. */
    public static class FedRelFilter extends Filter implements FedRel {
        public Double rowCount;
        public FedRelFilter(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                RexNode condition) {
            super(cluster, traitSet, input, condition);
            assert getConvention() instanceof FedConvention;
            this.rowCount = null;
        }

        @Override public FedRelRules.FedRelFilter copy(RelTraitSet traitSet, RelNode input,
                                                     RexNode condition) {
            FedRelRules.FedRelFilter rel = new FedRelRules.FedRelFilter(getCluster(), traitSet, input, condition);
            rel.rowCount = this.rowCount;
            return rel;
        }

        @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
            return ((FedConvention)getConvention()).costEstimator.computeCost(this, planner, mq);
        }

        @Override public String getRelTypeName() {
            return super.getRelTypeName() + "[" + getConvention().getName() + "]";
        }

        @Override
        public double estimateRowCount(RelMetadataQuery mq) {
            if (this.rowCount != null) return this.rowCount;
            return super.estimateRowCount(mq);
        }

        @Override
        public void setRowCount(Double rowCount) {
            this.rowCount = rowCount;
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Aggregate}
     * to a {@link FedRelRules.FedRelAggregate}.
     */
    public static class FedRelAggregateRule extends FedRelRules.FedRelConverterRule {
        /** Creates a FedRelAggregateRule. */
        public static FedRelRules.FedRelAggregateRule create(FedConvention out) {
            return Config.INSTANCE
                    .withConversion(Aggregate.class, Convention.NONE, out,
                            "RemoteFedRelAggregateRule")
                    .withRuleFactory(FedRelRules.FedRelAggregateRule::new)
                    .toRule(FedRelRules.FedRelAggregateRule.class);
        }

        /** Called from the Config. */
        protected FedRelAggregateRule(Config config) {
            super(config);
        }

        @Override public @Nullable RelNode convert(RelNode rel) {
            final Aggregate agg = (Aggregate) rel;
            if (agg.getGroupSets().size() != 1) {
                // GROUPING SETS not supported; see
                // [CALCITE-734] Push GROUPING SETS to underlying SQL via FedRel adapter
                return null;
            }
            final RelTraitSet traitSet =
                    agg.getTraitSet().replace(out);
            try {
                return new FedRelRules.FedRelAggregate(rel.getCluster(), traitSet,
                        convert(agg.getInput(), out), agg.getGroupSet(),
                        agg.getGroupSets(), agg.getAggCallList());
            } catch (InvalidRelException e) {
                LOGGER.debug(e.toString());
                return null;
            }
        }
    }

    /** Returns whether this FedRel data source can implement a given aggregate
     * function. */
    private static boolean canImplement(AggregateCall aggregateCall,
                                        SqlDialect sqlDialect) {
        return sqlDialect.supportsAggregateFunction(
                aggregateCall.getAggregation().getKind())
                && aggregateCall.distinctKeys == null;
    }

    /** Aggregate operator implemented in FedRel convention. */
    public static class FedRelAggregate extends Aggregate implements FedRel {
        public Double rowCount;
        public FedRelAggregate(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                ImmutableBitSet groupSet,
                @Nullable List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggCalls)
                throws InvalidRelException {
            super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
            assert getConvention() instanceof FedConvention;
            assert this.groupSets.size() == 1 : "Grouping sets not supported";
            final SqlDialect dialect = ((FedConvention) getConvention()).dialect;
            for (AggregateCall aggCall : aggCalls) {
                if (!canImplement(aggCall, dialect)) {
                    throw new InvalidRelException("cannot implement aggregate function "
                            + aggCall);
                }
                if (aggCall.hasFilter() && !dialect.supportsAggregateFunctionFilter()) {
                    throw new InvalidRelException("dialect does not support aggregate "
                            + "functions FILTER clauses");
                }
            }
            this.rowCount = null;
        }

        @Deprecated // to be removed before 2.0
        public FedRelAggregate(RelOptCluster cluster, RelTraitSet traitSet,
                             RelNode input, boolean indicator, ImmutableBitSet groupSet,
                             List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
                throws InvalidRelException {
            this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
            checkIndicator(indicator);
        }

        @Override public FedRelRules.FedRelAggregate copy(RelTraitSet traitSet, RelNode input,
                                                        ImmutableBitSet groupSet,
                                                        @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
            try {
                FedRelRules.FedRelAggregate rel = new FedRelRules.FedRelAggregate(getCluster(), traitSet, input,
                        groupSet, groupSets, aggCalls);
                rel.rowCount = this.rowCount;
                return rel;
            } catch (InvalidRelException e) {
                // Semantic error not possible. Must be a bug. Convert to
                // internal error.
                throw new AssertionError(e);
            }
        }

        @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
            return ((FedConvention)getConvention()).costEstimator.computeCost(this, planner, mq);
        }

        @Override public String getRelTypeName() {
            return super.getRelTypeName() + "[" + getConvention().getName() + "]";
        }

        @Override
        public double estimateRowCount(RelMetadataQuery mq) {
            if (this.rowCount != null) return this.rowCount;
            return super.estimateRowCount(mq);
        }

        @Override
        public void setRowCount(Double rowCount) {
            this.rowCount = rowCount;
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to an
     * {@link FedRelRules.FedRelSort}.
     */
    public static class FedRelSortRule extends FedRelRules.FedRelConverterRule {
        /** Creates a FedRelSortRule. */
        public static FedRelRules.FedRelSortRule create(FedConvention out) {
            return Config.INSTANCE
                    .withConversion(Sort.class, Convention.NONE, out, "RemoteFedRelSortRule")
                    .withRuleFactory(FedRelRules.FedRelSortRule::new)
                    .toRule(FedRelRules.FedRelSortRule.class);
        }

        /** Called from the Config. */
        protected FedRelSortRule(Config config) {
            super(config);
        }

        @Override public @Nullable RelNode convert(RelNode rel) {
            return convert((Sort) rel, true);
        }

        /**
         * Converts a {@code Sort} into a {@code FedRelSort}.
         *
         * @param sort Sort operator to convert
         * @param convertInputTraits Whether to convert input to {@code sort}'s
         *                            FedRel convention
         * @return A new FedRelSort
         */
        public RelNode convert(Sort sort, boolean convertInputTraits) {
            final RelTraitSet traitSet = sort.getTraitSet().replace(out);

            final RelNode input;
            if (convertInputTraits) {
                final RelTraitSet inputTraitSet = sort.getInput().getTraitSet().replace(out);
                input = convert(sort.getInput(), inputTraitSet);
            } else {
                input = sort.getInput();
            }

            return new FedRelRules.FedRelSort(sort.getCluster(), traitSet,
                    input, sort.getCollation(), sort.offset, sort.fetch);
        }
    }

    /** Sort operator implemented in FedRel convention. */
    public static class FedRelSort
            extends Sort
            implements FedRel {
        public Double rowCount;
        public FedRelSort(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                RelCollation collation,
                @Nullable RexNode offset,
                @Nullable RexNode fetch) {
            super(cluster, traitSet, input, collation, offset, fetch);
            assert getConvention() instanceof FedConvention;
            assert getConvention() == input.getConvention();
            this.rowCount = null;
        }

        @Override public FedRelRules.FedRelSort copy(RelTraitSet traitSet, RelNode newInput,
                                                   RelCollation newCollation, @Nullable RexNode offset, @Nullable RexNode fetch) {
            FedRelRules.FedRelSort rel = new FedRelRules.FedRelSort(getCluster(), traitSet, newInput, newCollation,
                    offset, fetch);
            rel.rowCount = this.rowCount;
            return rel;
        }

        public RelOptCost getLogicalCost(RelOptPlanner planner,
                                  RelMetadataQuery mq) {
            return super.computeSelfCost(planner, mq);
        }

        @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                                              RelMetadataQuery mq) {
            return ((FedConvention) getConvention()).costEstimator.computeCost(this, planner, mq);
        }

        @Override public String getRelTypeName() {
            return super.getRelTypeName() + "[" + getConvention().getName() + "]";
        }

        @Override
        public double estimateRowCount(RelMetadataQuery mq) {
            if (this.rowCount != null) return this.rowCount;
            return super.estimateRowCount(mq);
        }

        @Override
        public void setRowCount(Double rowCount) {
            this.rowCount = rowCount;
        }
    }

    /** Visitor that checks whether part of a projection is a user-defined
     * function (UDF). */
    private static class CheckingUserDefinedFunctionVisitor
            extends RexVisitorImpl<Void> {

        private boolean containsUsedDefinedFunction = false;

        CheckingUserDefinedFunctionVisitor() {
            super(true);
        }

        public boolean containsUserDefinedFunction() {
            return containsUsedDefinedFunction;
        }

        @Override public Void visitCall(RexCall call) {
            SqlOperator operator = call.getOperator();
            if (operator instanceof SqlFunction
                    && ((SqlFunction) operator).getFunctionType().isUserDefined()) {
                containsUsedDefinedFunction |= true;
            }
            return super.visitCall(call);
        }

    }
}
