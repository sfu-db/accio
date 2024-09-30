package ai.dataprep.accio.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Infer more equal conditions for the Join Condition.
 *
 * <p> For example, in {@code SELECT * FROM T1, T2, T3 WHERE T1.id = T3.id AND T2.id = T3.id},
 * we can infer {@code T1.id = T2.id}.
 *
 * <p>For the above SQL, the second Join's condition is {@code T1.id = T3.id AND T2.id = T3.id}.
 * After inference, the final condition would be: {@code T1.id = T3.id AND T1.id = T3.id AND T1.id = T2.id}.
 *
 */

@Value.Enclosing
public class MultiJoinConditionRule extends RelRule<MultiJoinConditionRule.Config> implements TransformationRule {

    /** Creates a FederatedMultiJoinOptimizeRule. */
    protected MultiJoinConditionRule(MultiJoinConditionRule.Config config) { super(config); }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final MultiJoin multiJoinRel = call.rel(0);

        List<RexNode> joinFilters = RelOptUtil.conjunctions(multiJoinRel.getJoinFilter());
        // Currently only work on all inner join
        if (multiJoinRel.isFullOuterJoin()) {
            return;
        }
        for (JoinRelType joinType: multiJoinRel.getJoinTypes()) {
            if (!joinType.equals(JoinRelType.INNER)) {
                return;
            }
        }

        final List<Set<Integer>> equalSets = new ArrayList<>();
        final List<RexNode> result = new ArrayList<>(joinFilters.size());
        for (RexNode rexNode : joinFilters) {
            if (rexNode.isA(SqlKind.EQUALS)) {
                final RexNode op1 = ((RexCall) rexNode).getOperands().get(0);
                final RexNode op2 = ((RexCall) rexNode).getOperands().get(1);
                if (op1 instanceof RexInputRef && op2 instanceof RexInputRef) {
                    final RexInputRef in1 = (RexInputRef) op1;
                    final RexInputRef in2 = (RexInputRef) op2;
                    Set<Integer> set = null;
                    for (int i = 0; i < equalSets.size(); ++i) {
                        Set<Integer> s = equalSets.get(i);
                        if (s.contains(in1.getIndex()) || s.contains(in2.getIndex())) {
                            if (set == null) {
                                set = s;
                            } else {
                                // merge two sets and remove the later one
                                set.addAll(equalSets.remove(i));
                                --i;
                            }
                        }
                    }
                    if (set == null) {
                        // to make the result deterministic
                        set = new LinkedHashSet<>();
                        equalSets.add(set);
                    }
                    set.add(in1.getIndex());
                    set.add(in2.getIndex());
                } else {
                    result.add(rexNode);
                }
            } else {
                result.add(rexNode);
            }
        }

        boolean needOptimize = false;
        for (Set<Integer> set : equalSets) {
            if (set.size() > 2) {
                needOptimize = true;
                break;
            }
        }
        if (!needOptimize) {
            // keep the conditions unchanged.
            return;
        }

        final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        for (Set<Integer> set : equalSets) {
            List<Integer> equalColumns = new ArrayList<>(set);
            for (int i = 0; i < equalColumns.size()-1; ++i) {
                for (int j = i+1; j < equalColumns.size(); ++j) {
                    result.add(
                            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                                    rexBuilder.makeInputRef(multiJoinRel, equalColumns.get(i)),
                                    rexBuilder.makeInputRef(multiJoinRel, equalColumns.get(j))));
                }
            }
        }

        MultiJoin newMultiJoin =
                new MultiJoin(
                        multiJoinRel.getCluster(),
                        multiJoinRel.getInputs(),
                        RexUtil.composeConjunction(rexBuilder, result, true),
                        multiJoinRel.getRowType(),
                        multiJoinRel.isFullOuterJoin(),
                        multiJoinRel.getOuterJoinConditions(),
                        multiJoinRel.getJoinTypes(),
                        multiJoinRel.getProjFields(),
                        multiJoinRel.getJoinFieldRefCountsMap(),
                        multiJoinRel.getPostJoinFilter());

        call.transformTo(newMultiJoin);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        MultiJoinConditionRule.Config DEFAULT = ImmutableMultiJoinConditionRule.Config.builder().build()
                .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());

        @Override default MultiJoinConditionRule toRule() {
            return new MultiJoinConditionRule(this);
        }
    }
}
