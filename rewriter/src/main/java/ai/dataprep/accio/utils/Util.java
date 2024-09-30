package ai.dataprep.accio.utils;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedRel;
import ai.dataprep.accio.plan.FedRelRules;
import ai.dataprep.accio.plan.RemoteToLocalConverter;
import ai.dataprep.accio.stats.FactorGroupStatsInfo;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Util {
    public static void updateFieldDomain(LVertex vertex, RexNode joinCondition, double[] newFieldDomain) {
        for (RexNode pred : RelOptUtil.conjunctions(joinCondition)) {
            switch (pred.getKind()) {
                case EQUALS:
                    List<RexNode> operands = ((RexCall)pred).getOperands();
                    if ((operands.get(0) instanceof RexInputRef)
                            && (operands.get(1) instanceof RexInputRef)) {
                        int f0 = ((RexInputRef)operands.get(0)).getIndex();
                        int f1 = ((RexInputRef)operands.get(1)).getIndex();
                        assert vertex.fieldDomain[f0] >= 0 && vertex.fieldDomain[f1] >= 0;
                        if (vertex.fieldDomain[f0] > vertex.fieldDomain[f1]) {
                            newFieldDomain[f0] = newFieldDomain[f1]; // the larger domain is reduced to the smaller one after join
                        } else {
                            newFieldDomain[f1] = newFieldDomain[f0]; // the larger domain is reduced to the smaller one after join
                        }
                    }
                    break;
                case OR:
                    for (RexNode pred2: RelOptUtil.disjunctions(pred)) {
                        updateFieldDomain(vertex, pred2, newFieldDomain);
                    }
            }
        }
    }

    public static Double fillInRowCount(LVertex vertex, HashMap<ImmutableBitSet, FactorGroupStatsInfo> rowCountCache, ImmutableBitSet bitSet, FedConvention convention)
    {
        if (rowCountCache != null && rowCountCache.containsKey(bitSet)) {
            vertex.rowCount = rowCountCache.get(bitSet).rowCount;
            vertex.fieldDomain = rowCountCache.get(bitSet).fieldDomain;
        } else {
            // try to get the row count from remote directly (e.g., through `EXPLAIN`)
            Double rowCount = FedConvention.CE_INSTANCE.cardinalityEstimator.getRowCountDirectly(FedConvention.CE_INSTANCE, vertex.rel);

            if (rowCount == null) {
                // guess row count using heuristics (field domain size are already updated within the procedure)
                if (vertex.isJoin) {
                    rowCount = estimateCrossSiteJoinRowCount(vertex);
                } else {
                    rowCount = 100D; // heuristic: 100 rows for unknown factors
                }
            } else {
                // update domain size of each field with the smaller value of the corresponding join key
                // example: T = R join S on R.a = S.b
                //          |R.a| = 10, |S.b| = 20 => |T.a| = |T.b| = 10 (assume inclusive)
                if (vertex.isJoin) {
                    double[] newFieldDomain = vertex.fieldDomain.clone();
                    updateFieldDomain(vertex, ((Join)vertex.rel).getCondition(), newFieldDomain);
                    vertex.fieldDomain = newFieldDomain;
                }
            }
            if (rowCountCache != null) {
                rowCountCache.put(bitSet, new FactorGroupStatsInfo(rowCount, vertex.fieldDomain));
            }
            vertex.rowCount = rowCount;
        }

        // keep the domain size of each field <= overall cardinality
        for (int i = 0; i < vertex.fieldDomain.length; ++i) {
            vertex.fieldDomain[i] = Math.min(vertex.rowCount, vertex.fieldDomain[i]);
        }
        return vertex.rowCount;
    }

    public static @Nullable Double estimateJoinSelectivity(LVertex vertex, RexNode joinCondition, double[] newFieldDomain) {
        double selectivity = 1.0;
        for (RexNode pred : RelOptUtil.conjunctions(joinCondition)) {
            switch (pred.getKind()) {
                case EQUALS:
                    List<RexNode> operands = ((RexCall)pred).getOperands();
                    if ((operands.get(0) instanceof RexInputRef)
                            && (operands.get(1) instanceof RexInputRef)) {
                        int f0 = ((RexInputRef)operands.get(0)).getIndex();
                        int f1 = ((RexInputRef)operands.get(1)).getIndex();
                        if (vertex.fieldDomain[f0] > vertex.fieldDomain[f1]) {
                            selectivity = Math.min(1 / vertex.fieldDomain[f0], selectivity); // use the minimum selectivity among all conjunctive conditions
                            newFieldDomain[f0] = newFieldDomain[f1]; // the larger domain is reduced to the smaller one after join
                        } else {
                            selectivity = Math.min(1 / vertex.fieldDomain[f1], selectivity); // use the minimum selectivity among all conjunctive conditions
                            newFieldDomain[f1] = newFieldDomain[f0]; // the larger domain is reduced to the smaller one after join
                        }
                    } else {
                        selectivity = Math.min(RelMdUtil.guessSelectivity(pred), selectivity);
                    }
                    break;
                case OR:
                    double sel2 = -1.0;
                    for (RexNode pred2: RelOptUtil.disjunctions(pred)) {
                        sel2 = Math.max(estimateJoinSelectivity(vertex, pred2, newFieldDomain), sel2); // use the maximum selectivity among all disjunctive conditions
                    }
                    assert sel2 >= 0.0;
                    selectivity = Math.min(sel2, selectivity);
                    break;
                default:
                    selectivity = Math.min(RelMdUtil.guessSelectivity(pred), selectivity);
            }
        }
        return selectivity;
    }

    // guess the join row count directly using children cardinality and join condition
    // NOTE: only support conjunctive equal conditions for now
    public static @Nullable Double estimateCrossSiteJoinRowCount(LVertex vertex) {
        assert vertex.isJoin;
        Join join = (Join)vertex.rel;
        Double leftRowCount = vertex.left.rowCount;
        Double rightRowCount = vertex.right.rowCount;

        if (leftRowCount == null || rightRowCount == null) {
            return null;
        }

        double[] newFieldDomain = vertex.fieldDomain.clone();
        double selectivity = estimateJoinSelectivity(vertex, join.getCondition(), newFieldDomain);
        assert selectivity >= 0.0 && selectivity <= 1.0;

        // Row count estimates of 0 will be rounded up to 1.
        double innerRowCount = Math.max(leftRowCount * rightRowCount * selectivity, 1D);

        vertex.fieldDomain = newFieldDomain;

        switch (join.getJoinType()) {
            case INNER:
                return innerRowCount;
            default:
                throw org.apache.calcite.util.Util.unexpected(join.getJoinType());
        }
    }

    public static PVertex constructJoinVertex(
        LoptMultiJoin multiJoin,
        RelBuilder relBuilder,
        RexBuilder rexBuilder,
        PVertex vertex1,
        PVertex vertex2,
        VariableUpdater variableUpdater){
        FedConvention convention = FedConvention.LOCAL_INSTANCE;
        if (vertex1.convention == vertex2.convention) {
            convention = vertex1.convention;
        }
        return constructJoinVertex(
                multiJoin,
                relBuilder,
                rexBuilder,
                vertex1,
                vertex2,
                variableUpdater,
                convention);
    }

    public static PVertex constructJoinVertex(
            LoptMultiJoin multiJoin,
            RelBuilder relBuilder,
            RexBuilder rexBuilder,
            PVertex vertex1,
            PVertex vertex2,
            VariableUpdater variableUpdater,
            FedConvention convention){
        // try to construct a join between the two vertexes
        ImmutableBitSet newBitSet = vertex1.factors.union(vertex2.factors);
        ImmutableBitSet overlappedFilters = vertex1.unusedFilters.intersect(vertex2.unusedFilters);
        ImmutableBitSet.Builder joinConditionBuilder = ImmutableBitSet.builder();
        List<RexNode> oldConditions = new ArrayList<>();
        for (int k: overlappedFilters) {
            RexNode filter = multiJoin.getJoinFilters().get(k);
            // a filter is a join condition if and only if all the factors it touches are contained by the inputs
            if (newBitSet.contains(multiJoin.getFactorsRefByJoinFilter(filter))) {
                joinConditionBuilder.set(k);
                oldConditions.add(filter);
            }
        }
        if (oldConditions.isEmpty()) return null; // no valid join (non-cross join) between the two vertexes yet
        // construct a new join (left: vertex1, right: vertex2)
        // new unusedFilters should remove the join conditions from the unioned unused Filters
        ImmutableBitSet currentJoinFilters = joinConditionBuilder.build();
        ImmutableBitSet newUnusedFilters = vertex1.unusedFilters.union(vertex2.unusedFilters).except(currentJoinFilters);
        ImmutableBitSet newUsedFilters = vertex1.usedFilters.union(vertex2.usedFilters).union(currentJoinFilters);

        // construct a new join (left: vertex1, right: vertex2)
        int[] newFactorOffsets = vertex1.factorOffets.clone();
        for (int i = 0; i < newFactorOffsets.length; ++i) {
            if (vertex2.factorOffets[i] >= 0) {
                newFactorOffsets[i] = vertex1.nFields + vertex2.factorOffets[i];
            }
        }
        // combine field domains (do not update the domain until joined cardinality is estimated)
        double[] newFieldDomain = new double[vertex1.fieldDomain.length + vertex2.fieldDomain.length];
        System.arraycopy(vertex1.fieldDomain, 0, newFieldDomain, 0, vertex1.fieldDomain.length);
        System.arraycopy(vertex2.fieldDomain, 0, newFieldDomain, vertex1.fieldDomain.length, vertex2.fieldDomain.length);

        variableUpdater.setNewFactorOffsets(newFactorOffsets);
        // update the filters within the new join tree
        List<RexNode> conditions = new ArrayList<>();
        for (RexNode filter : oldConditions) {
            conditions.add(variableUpdater.apply(filter));
        }
        final RexNode newJoinCondition = RexUtil.composeConjunction(rexBuilder, conditions);
        final RelNode newJoin = relBuilder.push(vertex1.rel)
                .push(vertex2.rel)
                .join(JoinRelType.INNER, newJoinCondition)
                .build();

        // set the in and out convention the same
        PVertex vertex = new PVertex(convention, convention, newJoin, newBitSet, newUsedFilters, newUnusedFilters, newFactorOffsets, vertex1.nFields + vertex2.nFields, newFieldDomain);
        vertex.setChildren(vertex1, vertex2);
        return vertex;
    }

    public static RelNode convertToPlain(LoptMultiJoin multiJoin,
                                         RelBuilder relBuilder,
                                         RexBuilder rexBuilder,
                                         PVertex vertex,
                                         VariableUpdater variableUpdater) {
        int[] factorOffsets = new int[multiJoin.getNumJoinFactors()];
        Arrays.fill(factorOffsets, -1);
        int fields = 0;
        Mappings.TargetMapping mapping = Mappings.createIdentity(0);
        for (int i: vertex.factors) {
            relBuilder.push(multiJoin.getJoinFactor(i));
            if (fields > 0) {
                relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));
            }
            factorOffsets[i] = fields;

            final Mappings.TargetMapping tmp = Mappings.offsetTarget(Mappings.offsetSource(
                    Mappings.createIdentity(multiJoin.getNumFieldsInJoinFactor(i)),
                    vertex.factorOffets[i],
                    vertex.nFields), fields);
            mapping = Mappings.merge(mapping, tmp);

            fields += multiJoin.getNumFieldsInJoinFactor(i);
        }
        variableUpdater.setNewFactorOffsets(factorOffsets);
        // update the filters within the new join tree
        List<RexNode> conditions = new ArrayList<>();
        for (int i: vertex.usedFilters) {
            RexNode filter = multiJoin.getJoinFilters().get(i);
            conditions.add(variableUpdater.apply(filter));
        }
        final RexNode allJoinCondition = RexUtil.composeConjunction(rexBuilder, conditions);

        RelNode node = relBuilder.filter(allJoinCondition).project(relBuilder.fields(mapping)).build();
        return node;
    }

    public static RelNode dumpPlanPlain(LoptMultiJoin multiJoin,
                                        RelBuilder relBuilder,
                                        RexBuilder rexBuilder,
                                        PVertex vertex,
                                        VariableUpdater variableUpdater) {
        if (vertex.convention != vertex.outConvention) {
            assert vertex.outConvention == FedConvention.LOCAL_INSTANCE;
            // all the vertex will be pushed down to remote and transfer the result back
            PhysicalPlanExtractor extractor = new PhysicalPlanExtractor(vertex.convention);
            RelNode rel = extractor.extract(convertToPlain(multiJoin, relBuilder, rexBuilder, vertex, variableUpdater));
            ((FedRel)rel).setRowCount(vertex.rowCount);
            return new RemoteToLocalConverter(vertex.rel.getCluster(), RelTraitSet.createEmpty().plus(FedConvention.LOCAL_INSTANCE), rel, vertex.partitionScheme, vertex.rowCount.longValue());
        }

        assert vertex.isJoin;
        assert vertex.convention == FedConvention.LOCAL_INSTANCE;
        RelNode left = dumpPlanPlain(multiJoin, relBuilder, rexBuilder, (PVertex) vertex.left, variableUpdater);
        RelNode right = dumpPlanPlain(multiJoin, relBuilder, rexBuilder, (PVertex) vertex.right, variableUpdater);
        LogicalJoin join = (LogicalJoin)vertex.rel;
        try {
            FedRelRules.FedRelJoin fedJoin = new FedRelRules.FedRelJoin(
                    join.getCluster(),
                    join.getTraitSet().replace(FedConvention.LOCAL_INSTANCE),
                    left,
                    right,
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType()
            );
            fedJoin.setRowCount(vertex.rowCount);
            return fedJoin;
        } catch (InvalidRelException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static RelNode dumpPlan(PVertex vertex){
        if (vertex.convention != vertex.outConvention) {
            assert vertex.outConvention == FedConvention.LOCAL_INSTANCE;
            // all the vertex will be pushed down to remote and transfer the result back
            PhysicalPlanExtractor extractor = new PhysicalPlanExtractor(vertex.convention);
            RelNode rel = extractor.extract(vertex.rel);
            ((FedRel)rel).setRowCount(vertex.rowCount);
            return new RemoteToLocalConverter(vertex.rel.getCluster(), RelTraitSet.createEmpty().plus(FedConvention.LOCAL_INSTANCE), rel, vertex.partitionScheme, vertex.rowCount.longValue());
        }

        assert vertex.isJoin;
        assert vertex.convention == FedConvention.LOCAL_INSTANCE;
        RelNode left = dumpPlan((PVertex) vertex.left);
        RelNode right = dumpPlan((PVertex) vertex.right);
        LogicalJoin join = (LogicalJoin)vertex.rel;
        try {
            FedRelRules.FedRelJoin fedJoin = new FedRelRules.FedRelJoin(
                    join.getCluster(),
                    join.getTraitSet().replace(FedConvention.LOCAL_INSTANCE),
                    left,
                    right,
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType()
            );
            fedJoin.setRowCount(vertex.rowCount);
            return fedJoin;
        } catch (InvalidRelException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static RelNode deepCopy(RelNode r) {
        RelNode copy = r.copy(r.getTraitSet(), r.getInputs());
        List<RelNode> inputs = r.getInputs();
        for (int i = 0; i < inputs.size(); ++i) {
            RelNode child = inputs.get(i);
            child = deepCopy(child);
            copy.replaceInput(i, child);
        }
        return copy;
    }
}
