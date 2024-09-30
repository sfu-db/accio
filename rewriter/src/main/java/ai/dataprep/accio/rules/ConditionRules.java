package ai.dataprep.accio.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

/**
* These condition rules extract common predicates from ORs,
* which are good for push down predicates and avoid cross join
*
* Reference: https://github.com/apache/hive/blob/374564f49b345f214ab8845e84244ac4ea4f0668/ql/src/java/org/apache/hadoop/hive/ql/optimizer/calcite/rules/HivePointLookupOptimizerRule.java
* */
@Value.Enclosing
public class ConditionRules {

    public static class FilterConditionRule
            extends RelRule<FilterConditionRule.FilterConfig>
            implements TransformationRule {
        protected FilterConditionRule(FilterConfig config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final Filter filter = call.rel(0);
            final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
            final RexNode condition = RexUtil.pullFactors(rexBuilder, filter.getCondition());
            // If we could not transform anything, we bail out
            if (filter.getCondition().toString().equals(condition.toString())) {
                return;
            }
            RelNode newNode = filter.copy(filter.getTraitSet(), filter.getInput(), condition);
            call.transformTo(newNode);
        }

        @Value.Immutable
        public interface FilterConfig extends RelRule.Config {
            FilterConditionRule.FilterConfig DEFAULT = ImmutableConditionRules.FilterConfig.builder().build().withOperandFor(Filter.class);

            @Override default FilterConditionRule toRule() {return new FilterConditionRule(this);}

            default FilterConditionRule.FilterConfig withOperandFor(Class<? extends Filter> filterClass) {
                return withOperandSupplier(b0 ->
                        b0.operand(filterClass).anyInputs())
                        .as(FilterConditionRule.FilterConfig.class);
            }
        }
    }

    public static class JoinConditionRule
            extends RelRule<JoinConditionRule.JoinConfig>
            implements TransformationRule {
        protected JoinConditionRule(JoinConfig config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final Join join = call.rel(0);
            final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
            final RexNode condition = RexUtil.pullFactors(rexBuilder, join.getCondition());
            // If we could not transform anything, we bail out
            if (join.getCondition().toString().equals(condition.toString())) {
                return;
            }
            RelNode newNode = join.copy(join.getTraitSet(),
                    condition,
                    join.getLeft(),
                    join.getRight(),
                    join.getJoinType(),
                    join.isSemiJoinDone());

            call.transformTo(newNode);
        }

        @Value.Immutable
        public interface JoinConfig extends RelRule.Config {
            JoinConditionRule.JoinConfig DEFAULT = ImmutableConditionRules.JoinConfig.builder().build().withOperandFor(Join.class);

            @Override default JoinConditionRule toRule() {return new JoinConditionRule(this);}

            default JoinConditionRule.JoinConfig withOperandFor(Class<? extends Join> joinClass) {
                return withOperandSupplier(b0 ->
                        b0.operand(joinClass).anyInputs())
                        .as(JoinConditionRule.JoinConfig.class);
            }
        }
    }
}
