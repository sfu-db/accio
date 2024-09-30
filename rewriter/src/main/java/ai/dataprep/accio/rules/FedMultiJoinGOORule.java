package ai.dataprep.accio.rules;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.utils.*;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Value.Enclosing
public class FedMultiJoinGOORule extends RelRule<FedMultiJoinGOORule.Config> implements TransformationRule {
    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected FedMultiJoinGOORule(Config config) {
        super(config);
    }
    private static final Logger logger = LoggerFactory.getLogger(FedMultiJoinGOORule.class);

    protected void setCostByOutputCardinality(LVertex vertex) {
        assert vertex.isJoin;
        vertex.cost = vertex.rowCount;
    }

    protected LVertex constructNewJoinVertex(
            LoptMultiJoin multiJoin,
            RelBuilder relBuilder,
            RexBuilder rexBuilder,
            LVertex vertex1,
            LVertex vertex2,
            VariableUpdater variableUpdater) {
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

        // set convention
        FedConvention convention = FedConvention.LOCAL_INSTANCE;
        if (vertex1.convention == vertex2.convention) {
            convention = vertex1.convention;
        }

        // construct a new join (left: vertex1, right: vertex2)
        int[] newFactorOffsets = vertex1.factorOffets.clone();
        for (int i = 0; i < newFactorOffsets.length; ++i) {
            if (vertex2.factorOffets[i] >= 0) {
                newFactorOffsets[i] = vertex1.nFields + vertex2.factorOffets[i];
            }
        }
        // combine field domains
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

        LVertex vertex = new LVertex(convention, newJoin, newBitSet, newUsedFilters, newUnusedFilters, newFactorOffsets, vertex1.nFields + vertex2.nFields, newFieldDomain);
        vertex.setChildren(vertex1, vertex2);
        return vertex;
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
        final RelOptPlanner planner = call.getPlanner();
        VariableUpdater variableUpdater = new VariableUpdater(rexBuilder, multiJoin);

        // init base vertexes
        List<LVertex> vertexes = new ArrayList<>(multiJoin.getNumJoinFactors());
        ImmutableBitSet.Builder joinKeysBuilder = ImmutableBitSet.builder();
        for (RexNode filter: multiJoin.getJoinFilters()) {
            joinKeysBuilder.addAll(multiJoin.getFieldsRefByJoinFilter(filter));
        }
        ImmutableBitSet joinKeys = joinKeysBuilder.build();
        SingleSourcePushdownChecker pushdownChecker = new SingleSourcePushdownChecker(true);
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
            final RelNode rel = ((HepRelVertex)multiJoin.getJoinFactor(i)).getCurrentRel();
            ImmutableBitSet.Builder filterBitMap = ImmutableBitSet.builder();
            // add all filters that touch the vertex as it unusedFilters
            for (int j = 0; j < multiJoin.getJoinFilters().size(); ++j) {
                if (multiJoin.getFactorsRefByJoinFilter(multiJoin.getJoinFilters().get(j)).get(i)) {
                    filterBitMap.set(j);
                }
            }
            int[] factorOffsets = new int[multiJoin.getNumJoinFactors()];
            Arrays.fill(factorOffsets, -1);
            factorOffsets[i] = 0;
            ImmutableBitSet unusedFilters = filterBitMap.build();

            // [Assumption] all operations in a base factor are conducted in the source it is from
            pushdownChecker.go(rel);
            if (!pushdownChecker.convention.isPresent()) {
                logger.error("Base factor cannot be pushdown to single source!");
                return;
            }
            FedConvention convention = pushdownChecker.convention.orElse(FedConvention.LOCAL_INSTANCE);

            // collect domain size of join keys
            double[] fieldDomains = new double[multiJoin.getNumFieldsInJoinFactor(i)];
            Arrays.fill(fieldDomains, -1);
            for (int key: joinKeys) {
                if (key >= multiJoin.getJoinStart(i) && (
                        i == multiJoin.getNumJoinFactors()-1
                                || key < multiJoin.getJoinStart(i+1))) {
                    int fId = key - multiJoin.getJoinStart(i);
                    fieldDomains[fId] = convention.cardinalityEstimator.getDomainSizeDirectly(convention, rel, rel.getRowType().getFieldNames().get(fId));
                }
            }

            LVertex vertex = new LVertex(convention, rel, ImmutableBitSet.of(i), ImmutableBitSet.of(), unusedFilters, factorOffsets, multiJoin.getNumFieldsInJoinFactor(i), fieldDomains);
            Util.fillInRowCount(vertex, null, null, vertex.convention);
            pushdownChecker.clear();
            vertexes.add(vertex);
        }
        for (LVertex vertex: vertexes) {
            logger.trace("Base Factor: " + vertex);
        }

        // init candidate joins
        List<LVertex> candidateVertexes = new ArrayList<>();
        for (int i = 0; i < vertexes.size()-1; ++i) {
            LVertex vertex1 = vertexes.get(i);
            for (int j = i+1; j < vertexes.size(); ++j) {
                LVertex vertex2 = vertexes.get(j);

                LVertex newVertex = constructNewJoinVertex(multiJoin, relBuilder, rexBuilder, vertex1, vertex2, variableUpdater);
                if (newVertex == null) continue;

                Util.fillInRowCount(newVertex, null, null, newVertex.convention);
                setCostByOutputCardinality(newVertex);
                candidateVertexes.add(newVertex);
            }
        }

        // iteratively construct the next best join
        while (!candidateVertexes.isEmpty()) {
            for (LVertex vertex: candidateVertexes) {
                logger.trace("Candidate (score= " + vertex.cost + "): " + vertex);
            }
            LVertex bestNext = candidateVertexes.get(0);
            for (int i = 1; i < candidateVertexes.size(); ++i) {
                if (bestNext.cost > candidateVertexes.get(i).cost) {
                    bestNext = candidateVertexes.get(i);
                }
            }
            logger.trace("Next best (score= " + bestNext.cost + "): " + bestNext);

            // remove the two inputs from vertexes
            vertexes.remove(bestNext.left);
            vertexes.remove(bestNext.right);

            // remove candidate joins involve the inputs of the join
            List<LVertex> newCandidateVertexes = new ArrayList<>();
            for (LVertex vertex: candidateVertexes) {
                if (!vertex.factors.intersects(bestNext.factors)) {
                    newCandidateVertexes.add(vertex);
                }
            }

            // add candidate joins involve the new join
            for (LVertex vertex: vertexes) {
                LVertex newVertex = constructNewJoinVertex(multiJoin, relBuilder, rexBuilder, bestNext, vertex, variableUpdater);
                if (newVertex == null) continue;

                Util.fillInRowCount(newVertex, null, null, newVertex.convention);
                setCostByOutputCardinality(newVertex);
                newCandidateVertexes.add(newVertex);
            }

            vertexes.add(bestNext);
            candidateVertexes = newCandidateVertexes;
        }

        assert vertexes.size() == 1;
        LVertex finalVertex = vertexes.get(0);

        // create top project
        Mappings.TargetMapping mapping = Mappings.createIdentity(0);
        for (int i = 0; i < multiJoin.getNumJoinFactors(); ++i) {
            final Mappings.TargetMapping tmp = Mappings.offsetTarget(Mappings.offsetSource(
                    Mappings.createIdentity(multiJoin.getNumFieldsInJoinFactor(i)),
                    multiJoin.getJoinStart(i),
                    multiJoin.getNumTotalFields()), finalVertex.factorOffets[i]);
            mapping = Mappings.merge(mapping, tmp);
        }

        RelNode finalNode = dumpPlan(finalVertex, relBuilder);
        relBuilder.push(finalNode).project(relBuilder.fields(mapping));
        call.transformTo(relBuilder.build());
    }

    protected RelNode dumpPlan(LVertex vertex, RelBuilder relBuilder){
        if (!vertex.isJoin) {
            return vertex.rel;
        } else {
            RelNode left = dumpPlan(vertex.left, relBuilder);
            RelNode right = dumpPlan(vertex.right, relBuilder);
            RelNode join = relBuilder.push(left).push(right).join(JoinRelType.INNER, ((LogicalJoin)vertex.rel).getCondition()).build();
            return join;
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableFedMultiJoinGOORule.Config.builder().build()
                .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());
        @Override default FedMultiJoinGOORule toRule() {return new FedMultiJoinGOORule(this); }
    }
}
