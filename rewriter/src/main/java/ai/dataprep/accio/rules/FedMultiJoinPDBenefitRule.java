package ai.dataprep.accio.rules;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.stats.FactorGroupStatsInfo;
import ai.dataprep.accio.utils.PVertex;
import ai.dataprep.accio.utils.SingleSourcePushdownChecker;
import ai.dataprep.accio.utils.Util;
import ai.dataprep.accio.utils.VariableUpdater;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Value.Enclosing
public class FedMultiJoinPDBenefitRule extends RelRule<FedMultiJoinPDBenefitRule.Config> implements TransformationRule {
    private static final Logger logger = LoggerFactory.getLogger(FedMultiJoinPDBenefitRule.class);

    protected FedMultiJoinPDBenefitRule(Config config) {
        super(config);
    }

    static class PushdownWrapper {
        PVertex vertex;
        Double benefit;

        PushdownWrapper(PVertex vertex) {
            assert vertex.outConvention != FedConvention.LOCAL_INSTANCE && vertex.isJoin;
            this.vertex = vertex;
            Double joinLocal = vertex.convention.costEstimator.computeTransferCost(vertex.left.rowCount + vertex.right.rowCount)
                    + FedConvention.LOCAL_INSTANCE.costEstimator.computeInplaceJoin(vertex.rowCount, vertex.left.rowCount, vertex.right.rowCount);
            Double joinRemote = vertex.convention.costEstimator.computeInplaceJoin(vertex.rowCount, vertex.left.rowCount, vertex.right.rowCount)
                    + vertex.convention.costEstimator.computeTransferCost(vertex.rowCount);
            this.benefit = joinLocal / joinRemote;
        }
    }

    /**
     * Given a convention, a list of vertexes and edges, order using Greedy Operator Ordering (GOO) on the convention
     */
    protected PVertex joinOrdering(
            LoptMultiJoin multiJoin,
            RelBuilder relBuilder,
            RexBuilder rexBuilder,
            VariableUpdater variableUpdater,
            List<PVertex> allVertexes,
            List<PVertex> candidateJoins,
            HashMap<ImmutableBitSet, FactorGroupStatsInfo> rowCountCache,
            FedConvention convention,
            List<PVertex> baseVertexes) {
        // order vertex set with GOO
        while(!candidateJoins.isEmpty()) {
            // 1. pick the next join with minimum output
            PVertex nextBest = candidateJoins.get(0);
            for (int i = 1; i < candidateJoins.size(); ++i) {
                if (nextBest.rowCount > candidateJoins.get(i).rowCount) {
                    nextBest = candidateJoins.get(i);
                }
            }

            // 2. remove the two inputs of the chosen join from vertexes
            allVertexes.remove(nextBest.left);
            allVertexes.remove(nextBest.right);

            // 3. update candidate joins
            List<PVertex> newCandidateJoins = new ArrayList<>();
            for (PVertex vertex: candidateJoins) {
                if (!vertex.factors.intersects(nextBest.factors)) {
                    newCandidateJoins.add(vertex);
                }
            }
            for (PVertex vertex: allVertexes) {
                PVertex joinVertex = Util.constructJoinVertex(multiJoin, relBuilder, rexBuilder, nextBest, vertex, variableUpdater, convention);
                if (joinVertex != null) {
                    // get cardinality of new join vertex
                    if (joinVertex.convention != FedConvention.LOCAL_INSTANCE) {
                        Util.fillInRowCount(joinVertex, rowCountCache, joinVertex.factors , joinVertex.convention);
                    } else {
                        // try best to pushdown entire query for cardinality estimation
                        FedConvention c = null;
                        for (int i: joinVertex.factors) {
                            if (c == null) {
                                c = baseVertexes.get(i).convention;
                                continue;
                            }
                            if (c != baseVertexes.get(i).convention) {
                                c = FedConvention.LOCAL_INSTANCE;
                                break;
                            }
                        }
                        Util.fillInRowCount(joinVertex, rowCountCache, joinVertex.factors , c);
                    }
                    newCandidateJoins.add(joinVertex);
                }
            }
            candidateJoins = newCandidateJoins;

            // 4. add chosen vertex
            nextBest.cost = nextBest.left.cost+ nextBest.right.cost + convention.costEstimator.computeInplaceJoin(nextBest.rowCount, nextBest.left.rowCount, nextBest.right.rowCount);
            allVertexes.add(nextBest);
        }

        // return plan
        assert allVertexes.size() == 1;
        return allVertexes.get(0);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final MultiJoin multiJoinRel = call.rel(0);

        // check applicability: only work on all inner joins for now
        if (multiJoinRel.isFullOuterJoin()) {
            logger.error("Only support all inner joins for now!");
            return;
        }
        for (JoinRelType joinType: multiJoinRel.getJoinTypes()) {
            if (!joinType.equals(JoinRelType.INNER)) {
                logger.error("Only support all inner joins for now!");
                return;
            }
        }

        // init infrastructures
        final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);
        final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        final RelBuilder relBuilder = call.builder();
        final int nJoinFactors = multiJoin.getNumJoinFactors();
        final List<RexNode> joinFilters = multiJoin.getJoinFilters();
        HashMap<ImmutableBitSet, FactorGroupStatsInfo> rowCountCache = new HashMap<>();
        VariableUpdater variableUpdater = new VariableUpdater(rexBuilder, multiJoin);

        // init vertexes
        List<PVertex> vertexes = new ArrayList<>(nJoinFactors);
        ImmutableBitSet.Builder joinKeysBuilder = ImmutableBitSet.builder();
        for (RexNode filter: joinFilters) {
            joinKeysBuilder.addAll(multiJoin.getFieldsRefByJoinFilter(filter));
        }
        ImmutableBitSet joinKeys = joinKeysBuilder.build();
        SingleSourcePushdownChecker pushdownChecker = new SingleSourcePushdownChecker(true);
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
            ImmutableBitSet unusedFilters = filterBitMap.build();
            pushdownChecker.go(rel);
            // [Assumption] all operations in a base factor are conducted in the source it is from
            // since the plan is then fixed, we do not need to consider their cost (all base factor have 0 cost)
            if (!pushdownChecker.convention.isPresent()) {
                logger.error("Base factor cannot be pushdown to single source!");
                return;
            }
            FedConvention convention = pushdownChecker.convention.orElse(FedConvention.LOCAL_INSTANCE);
            pushdownChecker.clear();

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

            PVertex vertex = new PVertex(convention, convention, rel, bitSet, ImmutableBitSet.of(), unusedFilters, factorOffsets, multiJoin.getNumFieldsInJoinFactor(i), fieldDomains);
            Util.fillInRowCount(vertex, rowCountCache, bitSet, vertex.convention);
            vertex.cost = 0.0d;
            logger.debug(bitSet + "[" + vertex.rowCount + "] " + Arrays.toString(vertex.fieldDomain));
            vertexes.add(vertex);
        }

        // init candidate joins and pushdown benefit
        List<PVertex> candidateJoins = new ArrayList<>();
        List<PushdownWrapper> candidatePushdowns = new ArrayList<>();
        for (int i = 0; i < vertexes.size()-1; ++i) {
            PVertex v1 = vertexes.get(i);
            for (int j = i+1; j < vertexes.size(); ++j) {
                PVertex v2 = vertexes.get(j);
                PVertex joinVertex = Util.constructJoinVertex(multiJoin, relBuilder, rexBuilder, v1, v2, variableUpdater);
                if (joinVertex != null) {
                    Util.fillInRowCount(joinVertex, rowCountCache, joinVertex.factors, joinVertex.convention);
                    candidateJoins.add(joinVertex);
                    if (joinVertex.convention != FedConvention.LOCAL_INSTANCE && joinVertex.convention.supportOps.get("join")) {
                        candidatePushdowns.add(new PushdownWrapper(joinVertex));
                    }
                }
            }
        }

        List<PVertex> baseVertexes = new ArrayList<>(vertexes); // keep the base vertexes
        List<PVertex> baseCandidateJoins = new ArrayList<>(candidateJoins); // keep the base joins
        PVertex bestPlan = null;
        boolean checkNext = true;
        while(true) {
            if (checkNext) {
                // 1. centralized join ordering at local site
                // 1.1 copy vertexes and joins to local
                HashMap<ImmutableBitSet, PVertex> vertexMaps = new HashMap<>();
                for (PVertex vertex: vertexes) {
                    if (vertex.outConvention == FedConvention.LOCAL_INSTANCE) {
                        vertexMaps.put(vertex.factors, vertex);
                    } else {
                        // make sure all vertexes are locally available
                        vertexMaps.put(vertex.factors, vertex.fetchToLocal());
                    }
                }
                List<PVertex> copyCandidateJoins = new ArrayList<>(); // copy all joins as local
                for (PVertex join: candidateJoins) {
                    if (join.convention == FedConvention.LOCAL_INSTANCE
                            && ((PVertex)join.left).outConvention == FedConvention.LOCAL_INSTANCE
                            && ((PVertex)join.right).outConvention == FedConvention.LOCAL_INSTANCE) {
                        copyCandidateJoins.add(join);
                    } else {
                        copyCandidateJoins.add(join.copyAsLocal(vertexMaps.get(join.left.factors), vertexMaps.get(join.right.factors)));
                    }
                }
                List<PVertex> copyVertexes = new ArrayList<>(vertexMaps.values());
                // 1.2 join ordering and compare with the best plan
                PVertex candidatePlan = joinOrdering(multiJoin, relBuilder, rexBuilder, variableUpdater, copyVertexes, copyCandidateJoins, rowCountCache, FedConvention.LOCAL_INSTANCE, baseVertexes);
                logger.debug("Candidate: " + candidatePlan);
                if (bestPlan == null || bestPlan.cost > candidatePlan.cost) {
                    bestPlan = candidatePlan;
                    logger.debug("Better!");
                }
            } else {
                checkNext = true;
                logger.debug("skip checking since last benefit < 1.0");
            }

            // 2. if no pushdown join remaining, exit
            if (candidatePushdowns.isEmpty()) break;

            // 3. pick the join with maximum pushdown benefit
            PushdownWrapper nextPush = candidatePushdowns.get(0);
            for (int i = 1; i < candidatePushdowns.size(); ++i) {
                if (nextPush.benefit < candidatePushdowns.get(i).benefit) {
                    nextPush = candidatePushdowns.get(i);
                }
            }
            logger.debug("next push benefit: " + nextPush.benefit);
            if (nextPush.benefit < 1.0) {
                checkNext = false;
            }

            // 4. construct new vertex using centralized join ordering
            List<PVertex> localVertexes = new ArrayList<>();
            List<PVertex> localCandidateJoins = new ArrayList<>();
            for (int i: nextPush.vertex.factors) {
                localVertexes.add(baseVertexes.get(i));
            }
            for (PVertex join: baseCandidateJoins){
                // find join edges within the nextPush vertex group
                if (nextPush.vertex.factors.contains(join.factors)) {
                    assert join.convention != FedConvention.LOCAL_INSTANCE;
                    localCandidateJoins.add(join);
                }
            }
            PVertex newVertex = joinOrdering(multiJoin, relBuilder, rexBuilder, variableUpdater, localVertexes, localCandidateJoins, rowCountCache, nextPush.vertex.convention, baseVertexes);

            // 5. update vertexes and pushdown benefits
            vertexes.remove(nextPush.vertex.left);
            vertexes.remove(nextPush.vertex.right);
            List<PVertex> newCandidateJoins = new ArrayList<>();
            List<PushdownWrapper> newCandidatePushdowns = new ArrayList<>();
            for (PVertex vertex: candidateJoins) {
                if (!vertex.factors.intersects(newVertex.factors)) {
                    newCandidateJoins.add(vertex);
                }
            }
            for (PushdownWrapper wrapper: candidatePushdowns) {
                if (!wrapper.vertex.factors.intersects(newVertex.factors)) {
                    newCandidatePushdowns.add(wrapper);
                }
            }
            for (PVertex vertex: vertexes) {
                PVertex joinVertex = Util.constructJoinVertex(multiJoin, relBuilder, rexBuilder, newVertex, vertex, variableUpdater);
                if (joinVertex != null) {
                    Util.fillInRowCount(joinVertex, rowCountCache, joinVertex.factors, joinVertex.convention);
                    newCandidateJoins.add(joinVertex);
                    if (joinVertex.convention != FedConvention.LOCAL_INSTANCE) {
                        newCandidatePushdowns.add(new PushdownWrapper(joinVertex));
                    }
                }
            }
            candidateJoins = newCandidateJoins;
            candidatePushdowns = newCandidatePushdowns;
            vertexes.add(newVertex);
        }

        // create top project
        Mappings.TargetMapping mapping = Mappings.createIdentity(0);
        for (int i = 0; i < nJoinFactors; ++i) {
            final Mappings.TargetMapping tmp = Mappings.offsetTarget(Mappings.offsetSource(
                    Mappings.createIdentity(multiJoin.getNumFieldsInJoinFactor(i)),
                    multiJoin.getJoinStart(i),
                    multiJoin.getNumTotalFields()), bestPlan.factorOffets[i]);
            mapping = Mappings.merge(mapping, tmp);
        }

        RelNode finalNode;
        if (config.isPlain()) {
            finalNode = Util.dumpPlanPlain(multiJoin, relBuilder, rexBuilder, bestPlan, variableUpdater);
        } else {
            finalNode = Util.dumpPlan(bestPlan);
        }

        relBuilder.push(finalNode).project(relBuilder.fields(mapping));
        call.transformTo(relBuilder.build());
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableFedMultiJoinPDBenefitRule.Config.builder().build().withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());
        Config PLAIN = ImmutableFedMultiJoinPDBenefitRule.Config.builder().build().withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs()).withIsPlain(true);

        @Value.Default default boolean isPlain() {
            return false;
        }
        ImmutableFedMultiJoinPDBenefitRule.Config withIsPlain(boolean plain);

        @Override default FedMultiJoinPDBenefitRule toRule() {
            return new FedMultiJoinPDBenefitRule(this);
        }
    }
}
