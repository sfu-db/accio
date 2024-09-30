package ai.dataprep.accio.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.*;

/**
 * Since currently `createExistsPlan` of `SqlToRelConverter` will create `MIN(true)`, which not all databases (e.g. postgres) support MIN(boolean)
 * we add a patch to convert it INTO `MIN(1)`.
 * In the future, we may directly make this change inside `SqlToRelConverter`
 */


@Value.Enclosing
public class PatchCreateExistRule
        extends RelRule<PatchCreateExistRule.Config>
        implements TransformationRule {
    protected PatchCreateExistRule(Config config) { super(config); }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        final Project project = call.rel(1);
        RelNode x = apply(call, aggregate, project);
        if (x != null) {
            call.transformTo(x);
        }
    }

    public static @Nullable RelNode apply(RelOptRuleCall call, Aggregate aggregate,
                                          Project project) {
        Set<Integer> suspiciousFields = new HashSet<>();
        Set<Integer> confirmFields = new HashSet<>();
        for (AggregateCall aggCall: aggregate.getAggCallList()) {
            if (aggCall.getAggregation().getKind() == SqlKind.MIN
                    && aggCall.getArgList().size() == 1) {
                suspiciousFields.add(aggCall.getArgList().get(0));
            }
        }

        if (suspiciousFields.isEmpty()) return null;

        boolean hasChange = false;
        final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        final List<RexNode> exprs = new ArrayList<>();
        final List<RexNode> old = project.getProjects();
        for (int i = 0; i < old.size(); ++i) {
            if (suspiciousFields.contains(i)) {
                if (old.get(i).isAlwaysTrue()) {
                    exprs.add(rexBuilder.makeExactLiteral(BigDecimal.ONE));
                    hasChange = true;
                    confirmFields.add(i);
                    continue;
                }
            }
            exprs.add(old.get(i));
        }

        if (!hasChange) return null;

        // add another project to produce "min(1) = 1" in order to make result type boolean again
        List<RelDataTypeField> fields = aggregate.getRowType().getFieldList();
        final List<RexNode> f_exprs = new ArrayList<>();
        int i = 0;
        for (; i < aggregate.getGroupCount(); ++i) {
            f_exprs.add(rexBuilder.makeInputRef(fields.get(i).getType(), i));
        }
        for (AggregateCall aggCall: aggregate.getAggCallList()) {
            if (aggCall.getAggregation().getKind() == SqlKind.MIN
                    && aggCall.getArgList().size() == 1
            && confirmFields.contains(aggCall.getArgList().get(0))) {
                f_exprs.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeExactLiteral(BigDecimal.ONE), rexBuilder.makeInputRef(fields.get(i).getType(), i)));
            } else {
                f_exprs.add(rexBuilder.makeInputRef(fields.get(i).getType(), i));
            }
            ++i;
        }

        final RelBuilder relBuilder = call.builder();
        relBuilder.push(project.getInput())
                .project(exprs)
                .aggregate(relBuilder.groupKey(aggregate.getGroupSet()), aggregate.getAggCallList())
                .project(f_exprs);

        return relBuilder.build();
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        PatchCreateExistRule.Config DEFAULT = ImmutablePatchCreateExistRule.Config.builder().build()
                .withOperandFor(Aggregate.class, Project.class);

        @Override default PatchCreateExistRule toRule() {
            return new PatchCreateExistRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default PatchCreateExistRule.Config withOperandFor(Class<? extends Aggregate> aggregateClass,
                                                                Class<? extends Project> projectClass) {
            return withOperandSupplier(b0 ->
                    b0.operand(aggregateClass).oneInput(b1 ->
                            b1.operand(projectClass).anyInputs())).as(PatchCreateExistRule.Config.class);
        }
    }
}
