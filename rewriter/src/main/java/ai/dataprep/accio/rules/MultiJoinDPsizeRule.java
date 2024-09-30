package ai.dataprep.accio.rules;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Value.Enclosing
public class MultiJoinDPsizeRule
        extends RelRule<MultiJoinDPsizeRule.Config>
        implements TransformationRule {

    protected MultiJoinDPsizeRule(Config config) {
        super(config);
    }
    private static final Logger logger = LoggerFactory.getLogger(MultiJoinDPsizeRule.class);

    class Vertex {
        RelOptCost cost;
        private RelNode rel;
        ImmutableBitSet unusedFilters;
        int [] factorOffets;
        int nFields;

        Vertex(RelOptCost cost, RelNode rel, ImmutableBitSet unusedFilters, int [] factorOffets, int nFields) {
            this.cost = cost;
            this.rel = rel;
            this.unusedFilters = unusedFilters;
            this.factorOffets = factorOffets;
            this.nFields = nFields;
        }

        @Override public String toString() {
            return "[" + rel
                    + "] (cost=" + cost
                    + "), unusedFilters: " + unusedFilters
                    + ", factorOffsets: " + factorOffets;
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final MultiJoin multiJoinRel = call.rel(0);

        // check applicability: only work on all inner joins for now
        if (multiJoinRel.isFullOuterJoin()) {
            return;
        }
        for (JoinRelType joinType: multiJoinRel.getJoinTypes()) {
            if (!joinType.equals(JoinRelType.INNER)) {
                return;
            }
        }

        // init infrastructures
        final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);
        final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        final RelBuilder relBuilder = call.builder();
        final RelMetadataQuery mq = call.getMetadataQuery();
        final int nJoinFactors = multiJoin.getNumJoinFactors();
        final List<RexNode> joinFilters = multiJoin.getJoinFilters();
        VariableUpdater variableUpdater = new VariableUpdater(rexBuilder, multiJoin);
        List<HashMap<ImmutableBitSet, Vertex>> levels = new ArrayList<>();
        for (int i = 0; i <= nJoinFactors; ++i) {
            levels.add(new HashMap<>());
        }

        // init vertexes (level 1)
        for (int i = 0; i < nJoinFactors; i++) {
            final RelNode rel = ((HepRelVertex)multiJoin.getJoinFactor(i)).getCurrentRel();
            ImmutableBitSet.Builder filterBitMap = ImmutableBitSet.builder();
            // add all filters that touch the vertex as it unusedFilters
            for (int j = 0; j < joinFilters.size(); ++j) {
                 if (multiJoin.getFactorsRefByJoinFilter(joinFilters.get(j)).get(i)) {
                     filterBitMap.set(j);
                 }
            }
            ImmutableBitSet bitSet = ImmutableBitSet.of(i);
            int[] factorOffsets = new int[nJoinFactors];
            Arrays.fill(factorOffsets, -1);
            factorOffsets[i] = 0;
            Vertex vertex = new Vertex(mq.getCumulativeCost(rel), rel, filterBitMap.build(), factorOffsets, multiJoin.getNumFieldsInJoinFactor(i));
            levels.get(1).put(bitSet, vertex);
            logger.debug(bitSet + ": " + vertex);
        }

        // conduct DPsize by enumerating the next level (n) with previous levels (1, 2, ..., n-1)
        for (int curLevel = 2; curLevel <= nJoinFactors; ++curLevel) {
            for (int s1 = 1; s1 < curLevel; ++s1) {
                int s2 = curLevel - s1;
                if (s2 < s1) break; // do not touch the same pair twice
                for (Map.Entry<ImmutableBitSet, Vertex> entry1: levels.get(s1).entrySet()) {
                    for (Map.Entry<ImmutableBitSet, Vertex> entry2: levels.get(s2).entrySet()) {
                        // do not touch the same pair twice if two entries are from the same level
                        if ((s1 == s2) && (entry1.getKey().compareTo(entry2.getKey()) >= 0)) continue;

                        // check two sets overlap
                        if (entry1.getKey().intersects(entry2.getKey())) continue;

                        // check two sets can join (prune cross joins)
                        ImmutableBitSet newBitSet = entry1.getKey().union(entry2.getKey());
                        Vertex vertex1 = entry1.getValue();
                        Vertex vertex2 = entry2.getValue();
                        ImmutableBitSet overlappedFilters = vertex1.unusedFilters.intersect(vertex2.unusedFilters);
                        ImmutableBitSet.Builder joinConditionBuilder = ImmutableBitSet.builder();
                        List<RexNode> oldConditions = new ArrayList<>();
                        for (int i: overlappedFilters) {
                            RexNode filter = joinFilters.get(i);
                            // a filter is a join condition if and only if all the factors it touches are contained by the inputs
                            if (newBitSet.contains(multiJoin.getFactorsRefByJoinFilter(filter))) {
                                joinConditionBuilder.set(i);
                                oldConditions.add(filter);
                            }
                        }
                        if (oldConditions.isEmpty()) continue;

                        // construct a new join (left: vertex1, right: vertex2)
                        int[] newFactorOffsets = vertex1.factorOffets.clone();
                        for (int i = 0; i < nJoinFactors; ++i) {
                            if (vertex2.factorOffets[i] >= 0) {
                                newFactorOffsets[i] = vertex1.nFields + vertex2.factorOffets[i];
                            }
                        }
                        variableUpdater.setNewFactorOffsets(newFactorOffsets);
                        // update the filters within the new join tree
                        List<RexNode> conditions = new ArrayList<>();
                        for (RexNode filter: oldConditions) {
                            conditions.add(variableUpdater.apply(filter));
                        }
                        final RexNode newJoinCondition = RexUtil.composeConjunction(rexBuilder, conditions);
                        final RelNode newJoin = relBuilder.push(vertex1.rel)
                                .push(vertex2.rel)
                                .join(JoinRelType.INNER, newJoinCondition)
                                .build();
                        RelOptCost newCost = mq.getCumulativeCost(newJoin);

                        // adding the new join by comparing with existing ones
                        if (levels.get(curLevel).containsKey(newBitSet)) {
                            Vertex oldVertex = levels.get(curLevel).get(newBitSet);
                            if (newCost.isLe(oldVertex.cost)) {
                                // add the new vertex, we can reuse `unusedFilters` and `nFields` from the old vertex since the set of factors is identical
                                Vertex newVertex = new Vertex(newCost, newJoin, oldVertex.unusedFilters, newFactorOffsets, oldVertex.nFields);
                                levels.get(curLevel).put(newBitSet, newVertex);
                            }
                            logger.debug(newBitSet + ": " + levels.get(curLevel).get(newBitSet));
                        } else {
                            // new unusedFilters should remove the join conditions from the unioned unused Filters
                            ImmutableBitSet newUnusedFilters = vertex1.unusedFilters.union(vertex2.unusedFilters).except(joinConditionBuilder.build());
                            Vertex newVertex = new Vertex(newCost, newJoin, newUnusedFilters, newFactorOffsets, vertex1.nFields+vertex2.nFields);
                            levels.get(curLevel).put(newBitSet, newVertex);
                            logger.debug(newBitSet + "(init): " + newVertex);
                        }
                    }
                }
            }
        }

        // extract result
        assert levels.get(nJoinFactors).size() == 1;
        Vertex finalVertex = levels.get(nJoinFactors).values().iterator().next();

        // create top project
        Mappings.TargetMapping mapping = Mappings.createIdentity(0);
        for (int i = 0; i < nJoinFactors; ++i) {
            final Mappings.TargetMapping tmp = Mappings.offsetTarget(Mappings.offsetSource(
                    Mappings.createIdentity(multiJoin.getNumFieldsInJoinFactor(i)),
                    multiJoin.getJoinStart(i),
                    multiJoin.getNumTotalFields()), finalVertex.factorOffets[i]);
            mapping = Mappings.merge(mapping, tmp);
        }

        relBuilder.push(finalVertex.rel).project(relBuilder.fields(mapping));
        call.transformTo(relBuilder.build());
    }

    private static class VariableUpdater extends RexShuttle {
        private final RexBuilder rexBuilder;
        private final int[] fieldFactorMap;
        private final int[] fieldOffsetMap;
        private int[] newFactorOffsets;

        VariableUpdater(RexBuilder rexBuilder, LoptMultiJoin multiJoin) {
            this.rexBuilder = rexBuilder;
            this.fieldFactorMap = new int[multiJoin.getNumTotalFields()];
            this.fieldOffsetMap = new int[multiJoin.getNumTotalFields()];
            int idx = 0;
            for (int i = 0; i < multiJoin.getNumJoinFactors(); ++i) {
                for (int j = 0; j < multiJoin.getNumFieldsInJoinFactor(i); ++j) {
                    fieldFactorMap[idx] = i;
                    fieldOffsetMap[idx] = j;
                    ++idx;
                }
            }
            assert idx == multiJoin.getNumTotalFields();
            this.newFactorOffsets = null;
        }

        public void setNewFactorOffsets(int[] newFactorOffsets) {
            this.newFactorOffsets = newFactorOffsets;
        }

        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            assert this.newFactorOffsets != null;

            int index = inputRef.getIndex();
            int inputFactor = this.fieldFactorMap[index];
            int newIndex = this.newFactorOffsets[inputFactor] + this.fieldOffsetMap[index];
            return rexBuilder.makeInputRef(inputRef.getType(), newIndex);
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableMultiJoinDPsizeRule.Config.builder().build()
                .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());

        @Override default MultiJoinDPsizeRule toRule() { return new MultiJoinDPsizeRule(this); }
    }
}
