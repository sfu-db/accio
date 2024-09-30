package ai.dataprep.accio.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.AbstractJoinExtractFilterRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Copied from JoinExtractFilterRule with
 * simple modification of supporting all Join
 * instead of only LogicalJoin
 */

@Value.Enclosing
public class JoinExtractFilterRule extends AbstractJoinExtractFilterRule {
    /**
     * Creates an AbstractJoinExtractFilterRule.
     *
     * @param config
     */
    protected JoinExtractFilterRule(Config config) {
        super(config);
    }

    // copy from `AbstractJoinExtractFilterRule`
    @Override public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);

        if (join.getJoinType() != JoinRelType.INNER) {
            return;
        }

        if (join.getCondition().isAlwaysTrue()) {
            return;
        }

        if (!join.getSystemFieldList().isEmpty()) {
            // FIXME Enable this rule for joins with system fields
            return;
        }

        final RelBuilder builder = call.builder();

        /********* Modify begin *********/
        // NOTE: Columns with same name `xxx` will be renamed as `xxx0` after join
        // This alias will be missed if we directly extract the condition (if it involves column `xxx0`)
        // Therefore, we add a projection to the side of `xxx0` and force it to alias from `xxx` to `xxx0` before join
        List<RelDataTypeField> fieldsAfterJoin = join.getRowType().getFieldList();
        int i = 0;
        boolean needProject = false;
        List<RexNode> newFields = new ArrayList<>();
        List<String> newNames = new ArrayList<>();
        RelNode left = join.getLeft();
        for (RelDataTypeField field : left.getRowType().getFieldList()) {
            if (!field.getName().equals(fieldsAfterJoin.get(i).getName())) {
                needProject = true;
            }
            newFields.add(new RexInputRef(i, field.getType()));
            newNames.add(fieldsAfterJoin.get(i).getName());
            ++i;
        }
        if (needProject) {
            builder.push(left)
                    .project(newFields, newNames, true);
            left = builder.build();
        }

        needProject = false;
        List<RexNode> newFields1 = new ArrayList<>();
        List<String> newNames1 = new ArrayList<>();
        RelNode right = join.getRight();

        for (RelDataTypeField field : right.getRowType().getFieldList()) {
            if (!field.getName().equals(fieldsAfterJoin.get(i).getName())) {
                needProject = true;
            }
            newFields1.add(new RexInputRef(i - left.getRowType().getFieldCount(), field.getType()));
            newNames1.add(fieldsAfterJoin.get(i).getName());
            ++i;
        }
        if (needProject) {
            builder.push(right)
                    .project(newFields1, newNames1, true);
            right = builder.build();
        }
        /********* Modify end *********/

        // NOTE jvs 14-Mar-2006:  See JoinCommuteRule for why we
        // preserve attribute semiJoinDone here.
        final RelNode cartesianJoin =
                join.copy(
                        join.getTraitSet(),
                        builder.literal(true),
                        left,
                        right,
                        join.getJoinType(),
                        join.isSemiJoinDone());

        builder.push(cartesianJoin)
                .filter(join.getCondition());

        call.transformTo(builder.build());
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends AbstractJoinExtractFilterRule.Config {
        JoinExtractFilterRule.Config DEFAULT = ImmutableJoinExtractFilterRule.Config.builder().build()
                .withOperandSupplier(b -> b.operand(Join.class).anyInputs());

        @Override default JoinExtractFilterRule toRule() {
            return new JoinExtractFilterRule(this);
        }
    }
}
