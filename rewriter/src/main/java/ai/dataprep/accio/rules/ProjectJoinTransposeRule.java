package ai.dataprep.accio.rules;

import ai.dataprep.accio.plan.FedRelRules;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project}
 * past a {@link org.apache.calcite.rel.core.Join}
 * by splitting the projection into a projection on top of each child of
 * the join.
 *
 * @see
 */
@Value.Enclosing
public class ProjectJoinTransposeRule
        extends RelRule<ProjectJoinTransposeRule.Config>
        implements TransformationRule {

    /** Creates a ProjectJoinTransposeRule. */
    protected ProjectJoinTransposeRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public ProjectJoinTransposeRule(
            Class<? extends Project> projectClass,
            Class<? extends Join> joinClass,
            PushProjector.ExprCondition preserveExprCondition,
            RelBuilderFactory relBuilderFactory) {
        this(Config.DEFAULT
                .withRelBuilderFactory(relBuilderFactory)
                .as(Config.class)
                .withOperandFor(projectClass, joinClass)
                .withPreserveExprCondition(preserveExprCondition));
    }

    //~ Methods ----------------------------------------------------------------

    @Override public void onMatch(RelOptRuleCall call) {
        final Project origProject = call.rel(0);
        final Join join = call.rel(1);

        // Normalize the join condition so we don't end up misidentified expanded
        // form of IS NOT DISTINCT FROM as PushProject also visit the filter condition
        // and push down expressions.
        RexNode joinFilter = join.getCondition().accept(new RexShuttle() {
            @Override public RexNode visitCall(RexCall rexCall) {
                final RexNode node = super.visitCall(rexCall);
                if (!(node instanceof RexCall)) {
                    return node;
                }
                return RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) node,
                        call.builder().getRexBuilder());
            }
        });

        // locate all fields referenced in the projection and join condition;
        // determine which inputs are referenced in the projection and
        // join condition; if all fields are being referenced and there are no
        // special expressions, no point in proceeding any further
        final PushProjector pushProjector =
                new PushProjector(
                        origProject,
                        joinFilter,
                        join,
                        config.preserveExprCondition(),
                        call.builder());
        if (pushProjector.locateAllRefs()) {
            return;
        }

        // create left and right projections, projecting only those
        // fields referenced on each side
        final RelNode leftProject =
                pushProjector.createProjectRefsAndExprs(
                        join.getLeft(),
                        true,
                        false);
        final RelNode rightProject =
                pushProjector.createProjectRefsAndExprs(
                        join.getRight(),
                        true,
                        true);

        // convert the join condition to reference the projected columns
        RexNode newJoinFilter = null;
        int[] adjustments = pushProjector.getAdjustments();
        if (joinFilter != null) {
            List<RelDataTypeField> projectJoinFieldList = new ArrayList<>();
            projectJoinFieldList.addAll(
                    join.getSystemFieldList());
            projectJoinFieldList.addAll(
                    leftProject.getRowType().getFieldList());
            projectJoinFieldList.addAll(
                    rightProject.getRowType().getFieldList());
            newJoinFilter =
                    pushProjector.convertRefsAndExprs(
                            joinFilter,
                            projectJoinFieldList,
                            adjustments);
        }

        /******************************************************************************/
        // [NEWLY ADDED]: if we push down a join into a physical join
        if (join instanceof FedRelRules.FedRelJoin) {
            RelNode fedLeftProject = new FedRelRules.FedRelProject(
                    leftProject.getCluster(),
                    join.getTraitSet(),
                    ((Project)leftProject).getInput(),
                    ((Project)leftProject).getProjects(),
                    leftProject.getRowType()
            );
            RelNode fedRightProject = new FedRelRules.FedRelProject(
                    rightProject.getCluster(),
                    join.getTraitSet(),
                    ((Project)rightProject).getInput(),
                    ((Project)rightProject).getProjects(),
                    rightProject.getRowType()
            );

            // create a new join with the projected children
            final Join newJoin =
                    join.copy(
                            join.getTraitSet(),
                            requireNonNull(newJoinFilter, "newJoinFilter must not be null"),
                            fedLeftProject,
                            fedRightProject,
                            join.getJoinType(),
                            join.isSemiJoinDone());

            // put the original project on top of the join, converting it to
            // reference the modified projection list
            RelNode topProject =
                    pushProjector.createNewProject(newJoin, adjustments);

            if (topProject instanceof Project && origProject instanceof FedRelRules.FedRelProject) {
                topProject = new FedRelRules.FedRelProject(
                        topProject.getCluster(),
                        origProject.getTraitSet(),
                        ((Project)topProject).getInput(),
                        ((Project)topProject).getProjects(),
                        topProject.getRowType()
                );
            }

            call.transformTo(topProject);
            return;
        }
        /******************************************************************************/

        // create a new join with the projected children
        final Join newJoin =
                join.copy(
                        join.getTraitSet(),
                        requireNonNull(newJoinFilter, "newJoinFilter must not be null"),
                        leftProject,
                        rightProject,
                        join.getJoinType(),
                        join.isSemiJoinDone());

        // put the original project on top of the join, converting it to
        // reference the modified projection list
        final RelNode topProject =
                pushProjector.createNewProject(newJoin, adjustments);

        call.transformTo(topProject);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableProjectJoinTransposeRule.Config.builder()
                .preserveExprCondition(expr -> {
                    // Do not push down over's expression by default
                    if (expr instanceof RexOver) {
                        return false;
                    }
                    if (SqlKind.CAST == expr.getKind()) {
                        final RelDataType relType = expr.getType();
                        final RexCall castCall = (RexCall) expr;
                        final RelDataType operand0Type = castCall.getOperands().get(0).getType();
                        if (relType.getSqlTypeName() == operand0Type.getSqlTypeName()
                                && operand0Type.isNullable() && !relType.isNullable()) {
                            // Do not push down not nullable cast's expression with the same type by default
                            // eg: CAST($1):VARCHAR(10) NOT NULL, and type of $1 is nullable VARCHAR(10)
                            return false;
                        }
                    }
                    return true;
                })
                .build()
                .withOperandFor(LogicalProject.class, LogicalJoin.class);

        @Override default ProjectJoinTransposeRule toRule() {
            return new ProjectJoinTransposeRule(this);
        }

        /** Defines when an expression should not be pushed. */
        PushProjector.ExprCondition preserveExprCondition();

        /** Sets {@link #preserveExprCondition()}. */
        Config withPreserveExprCondition(PushProjector.ExprCondition condition);

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Project> projectClass,
                                      Class<? extends Join> joinClass) {
            return withOperandSupplier(b0 ->
                    b0.operand(projectClass).oneInput(b1 ->
                            b1.operand(joinClass).anyInputs()))
                    .as(Config.class);
        }
    }
}