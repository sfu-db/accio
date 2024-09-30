package ai.dataprep.accio.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RexImplicationChecker;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.plan.RelOptUtil.*;

/**
 * Copy from org.apache.calcite.plan.RelOptUtil
 *
 * Changes:
 *  * `createExistsPlan`: exprs.add(rexBuilder.makeLiteral(true));
 *                      -> exprs.add(rexBuilder.makeExactLiteral(BigDecimal.ONE));
 */

public class FedRelOptUtil {

  /**
   * Creates a plan suitable for use in <code>EXISTS</code> or <code>IN</code>
   * statements.
   *
   * @see org.apache.calcite.sql2rel.SqlToRelConverter
   * SqlToRelConverter#convertExists
   *
   * @param seekRel    A query rel, for example the resulting rel from 'select *
   *                   from emp' or 'values (1,2,3)' or '('Foo', 34)'.
   * @param subQueryType Sub-query type
   * @param logic  Whether to use 2- or 3-valued boolean logic
   * @param notIn Whether the operator is NOT IN
   * @param relBuilder Builder for relational expressions
   *
   * @return A pair of a relational expression which outer joins a boolean
   * condition column, and a numeric offset. The offset is 2 if column 0 is
   * the number of rows and column 1 is the number of rows with not-null keys;
   * 0 otherwise.
   */
  public static Exists createExistsPlan(
      RelNode seekRel,
      SubQueryType subQueryType,
      Logic logic,
      boolean notIn,
      RelBuilder relBuilder) {
    switch (subQueryType) {
    case SCALAR:
      return new Exists(seekRel, false, true);
    default:
      break;
    }

    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
    case UNKNOWN_AS_TRUE:
      if (notIn && !containsNullableFields(seekRel)) {
        logic = Logic.TRUE_FALSE;
      }
      break;
    default:
      break;
    }
    RelNode ret = seekRel;
    final RelOptCluster cluster = seekRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final int keyCount = ret.getRowType().getFieldCount();
    final boolean outerJoin = notIn
        || logic == Logic.TRUE_FALSE_UNKNOWN;
    if (!outerJoin) {
      final LogicalAggregate aggregate =
          LogicalAggregate.create(ret, ImmutableList.of(), ImmutableBitSet.range(keyCount),
              null, ImmutableList.of());
      return new Exists(aggregate, false, false);
    }

    // for IN/NOT IN, it needs to output the fields
    final List<RexNode> exprs = new ArrayList<>();
    if (subQueryType == SubQueryType.IN) {
      for (int i = 0; i < keyCount; i++) {
        exprs.add(rexBuilder.makeInputRef(ret, i));
      }
    }

    final int projectedKeyCount = exprs.size();
    exprs.add(rexBuilder.makeExactLiteral(BigDecimal.ONE));

    ret = relBuilder.push(ret)
        .project(exprs)
        .aggregate(
            relBuilder.groupKey(ImmutableBitSet.range(projectedKeyCount)),
            relBuilder.min(relBuilder.field(projectedKeyCount)))
        .build();

    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
    case UNKNOWN_AS_TRUE:
      return new Exists(ret, true, true);
    default:
      return new Exists(ret, false, true);
    }
  }

    /**
     * Determines whether any of the fields in a given relational expression may
     * contain null values, taking into account constraints on the field types and
     * also deduced predicates.
     *
     * <p>The method is cautious: It may sometimes return {@code true} when the
     * actual answer is {@code false}. In particular, it does this when there
     * is no executor, or the executor is not a sub-class of
     * {@link RexExecutorImpl}.
     */
    private static boolean containsNullableFields(RelNode r) {
        final RexBuilder rexBuilder = r.getCluster().getRexBuilder();
        final RelDataType rowType = r.getRowType();
        final List<RexNode> list = new ArrayList<>();
        final RelMetadataQuery mq = r.getCluster().getMetadataQuery();
        for (RelDataTypeField field : rowType.getFieldList()) {
            if (field.getType().isNullable()) {
                list.add(
                        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
                                rexBuilder.makeInputRef(field.getType(), field.getIndex())));
            }
        }
        if (list.isEmpty()) {
            // All columns are declared NOT NULL.
            return false;
        }
        final RelOptPredicateList predicates = mq.getPulledUpPredicates(r);
        if (RelOptPredicateList.isEmpty(predicates)) {
            // We have no predicates, so cannot deduce that any of the fields
            // declared NULL are really NOT NULL.
            return true;
        }
        final RexExecutor executor = r.getCluster().getPlanner().getExecutor();
        if (!(executor instanceof RexExecutorImpl)) {
            // Cannot proceed without an executor.
            return true;
        }
        final RexImplicationChecker checker =
                new RexImplicationChecker(rexBuilder, executor,
                        rowType);
        final RexNode first =
                RexUtil.composeConjunction(rexBuilder, predicates.pulledUpPredicates);
        final RexNode second = RexUtil.composeConjunction(rexBuilder, list);
        // Suppose we have EMP(empno INT NOT NULL, mgr INT),
        // and predicates [empno > 0, mgr > 0].
        // We make first: "empno > 0 AND mgr > 0"
        // and second: "mgr IS NOT NULL"
        // and ask whether first implies second.
        // It does, so we have no nullable columns.
        return !checker.implies(first, second);
    }

    /** Result of calling
   * {@link org.apache.calcite.plan.RelOptUtil#createExistsPlan}. */
  public static class Exists {
    public final RelNode r;
    public final boolean indicator;
    public final boolean outerJoin;

    private Exists(RelNode r, boolean indicator, boolean outerJoin) {
      this.r = r;
      this.indicator = indicator;
      this.outerJoin = outerJoin;
    }
  }
}
