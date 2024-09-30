package ai.dataprep.accio.rules;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.stats.FactorGroupStatsInfo;
import ai.dataprep.accio.utils.*;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
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
public class FedMultiJoinDPsizeRule
        extends RelRule<FedMultiJoinDPsizeRule.Config>
        implements TransformationRule {


    protected FedMultiJoinDPsizeRule(Config config) {
        super(config);
    }
    private static final Logger logger = LoggerFactory.getLogger(FedMultiJoinDPsizeRule.class);

    protected PVertex constructNewJoinVertex(
            RelBuilder relBuilder,
            RexBuilder rexBuilder,
            FedConvention convention,
            FedConvention outConvention,
            ImmutableBitSet newBitSet,
            PVertex vertex1,
            PVertex vertex2,
            List<RexNode> oldConditions,
            VariableUpdater variableUpdater,
            ImmutableBitSet newUsedFilters,
            ImmutableBitSet newUnusedFilters) {
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

        PVertex vertex = new PVertex(convention, outConvention, newJoin, newBitSet, newUsedFilters, newUnusedFilters, newFactorOffsets, vertex1.nFields + vertex2.nFields, newFieldDomain);
        vertex.setChildren(vertex1, vertex2);
        return vertex;
    }

    protected void addToLevelIfBetter(FedConvention curConvention, HashMap<ImmutableBitSet, PVertex> curLevel, ImmutableBitSet newBitSet, PVertex newVertex) {
        // adding the new join by comparing with existing ones
        if (curLevel.containsKey(newBitSet)) {
            PVertex oldVertex = curLevel.get(newBitSet);
            if (newVertex.cost < oldVertex.cost) {
                // add the new vertex, we can reuse `unusedFilters` and `nFields` from the old vertex since the set of factors is identical
                curLevel.put(newBitSet, newVertex);
                logger.trace(curConvention.toString() + newBitSet + "(better): " + newVertex);
            } else {
                logger.trace(curConvention.toString() + newBitSet + "(worse): " + newVertex);
            }
        } else {
            curLevel.put(newBitSet, newVertex);
            logger.trace(curConvention.toString() + newBitSet + "(init): " + newVertex);
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
        final int nJoinFactors = multiJoin.getNumJoinFactors();
        final List<RexNode> joinFilters = multiJoin.getJoinFilters();
        HashMap<ImmutableBitSet, FactorGroupStatsInfo> rowCountCache = new HashMap<>();
        VariableUpdater variableUpdater = new VariableUpdater(rexBuilder, multiJoin);
        List<HashMap<FedConvention, HashMap<ImmutableBitSet, PVertex>>> levels = new ArrayList<>();
        for (int i = 0; i <= nJoinFactors; ++i) {
            levels.add(new HashMap<>());
        }

        // init vertexes (level 1)
        Set<FedConvention> allConventions = new HashSet<>(List.of(FedConvention.LOCAL_INSTANCE));
        levels.get(1).put(FedConvention.LOCAL_INSTANCE, new HashMap<>());
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
            vertex.cost = 0.0;
            Util.fillInRowCount(vertex, rowCountCache, bitSet, vertex.convention);

            if (!allConventions.contains(convention)) {
                levels.get(1).put(convention, new HashMap<>());
                allConventions.add(convention);
            }
            levels.get(1).get(convention).put(bitSet, vertex);
            logger.trace(convention.toString() + bitSet + ": " + vertex);

            // if the factor is not already at local, add an option to transfer the base factor to local
            if (convention != FedConvention.LOCAL_INSTANCE) {
                PVertex localVertex = vertex.fetchToLocal();
                levels.get(1).get(FedConvention.LOCAL_INSTANCE).put(bitSet, localVertex);
                logger.trace(FedConvention.LOCAL_INSTANCE.toString() + bitSet + ": " + localVertex);
            }
            pushdownChecker.clear();
        }

        // conduct DPsize by enumerating the next level (n) with previous levels (1, 2, ..., n-1)
        for (int curLevel = 2; curLevel <= nJoinFactors; ++curLevel) {
            // initialize from the last level
            for (FedConvention convention: allConventions) {
                levels.get(curLevel).put(convention, new HashMap<>());
            }

            for (int s1 = 1; s1 < curLevel; ++s1) {
                int s2 = curLevel - s1;
                if (s2 < s1) break; // do not touch the same pair twice

                // enumerate pairs using local convention (since it maintains entire set of subplans)
                // that is, all subplan can be found locally (either computed locally or transferred to local)
                for (Map.Entry<ImmutableBitSet, PVertex> entry1: levels.get(s1).get(FedConvention.LOCAL_INSTANCE).entrySet()) {
                    for (Map.Entry<ImmutableBitSet, PVertex> entry2: levels.get(s2).get(FedConvention.LOCAL_INSTANCE).entrySet()) {
                        // do not touch the same pair twice if two entries are from the same level
                        if ((s1 == s2) && (entry1.getKey().compareTo(entry2.getKey()) >= 0)) continue;

                        // check two sets overlap
                        if (entry1.getKey().intersects(entry2.getKey())) continue;

                        // [NOTE]: the logic below is the same with `Util.constructJoinVertex`,
                        // we extract the join condition parts here to only check it once for every pair of factor sets

                        // check two sets can join (prune cross joins)
                        ImmutableBitSet newBitSet = entry1.getKey().union(entry2.getKey());
                        PVertex vertex1 = entry1.getValue();
                        PVertex vertex2 = entry2.getValue();
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

                        // construct a new join (left: vertex1, right: vertex2) for every convention if possible
                        // 1. construct join on local
                        // new unusedFilters should remove the join conditions from the unioned unused Filters
                        ImmutableBitSet currentJoinFilters = joinConditionBuilder.build();
                        ImmutableBitSet newUnusedFilters = vertex1.unusedFilters.union(vertex2.unusedFilters).except(currentJoinFilters);
                        ImmutableBitSet newUsedFilters = vertex1.usedFilters.union(vertex2.usedFilters).union(currentJoinFilters);
                        PVertex localVertex = constructNewJoinVertex(relBuilder, rexBuilder, FedConvention.LOCAL_INSTANCE, FedConvention.LOCAL_INSTANCE, newBitSet, vertex1, vertex2, oldConditions, variableUpdater, newUsedFilters, newUnusedFilters);
                        // [NOTE]: we do not compute the cardinality and cost for localVertex here since we may get a more accurate estimation from remote

                        // 2. check whether the same pair also exists in other conventions
                        for (FedConvention convention: allConventions) {
                            if (convention == FedConvention.LOCAL_INSTANCE) continue;
                            if (!levels.get(s1).get(convention).containsKey(entry1.getKey())) continue;
                            if (!levels.get(s2).get(convention).containsKey(entry2.getKey())) continue;

                            // 2.1 construct remote vertex
                            PVertex rVertex1 = levels.get(s1).get(convention).get(entry1.getKey());
                            PVertex rVertex2 = levels.get(s2).get(convention).get(entry2.getKey());
                            PVertex remoteVertex = constructNewJoinVertex(relBuilder, rexBuilder, convention, convention, newBitSet, rVertex1, rVertex2, oldConditions, variableUpdater, newUsedFilters, newUnusedFilters);
                            Util.fillInRowCount(remoteVertex, rowCountCache, newBitSet, remoteVertex.convention);
                            remoteVertex.cost = rVertex1.cost + rVertex2.cost + convention.costEstimator.computeInplaceJoin(remoteVertex.rowCount, rVertex1.rowCount, rVertex2.rowCount);

                            // 2.2 add remote vertex
                            addToLevelIfBetter(convention, levels.get(curLevel).get(convention), newBitSet, remoteVertex);

                            // 2.3 construct a new local vertex by transferring remote vertex to local, and compare with the best local vertex for now
                            PVertex toLocalVertex = remoteVertex.fetchToLocal();
                            addToLevelIfBetter(FedConvention.LOCAL_INSTANCE, levels.get(curLevel).get(FedConvention.LOCAL_INSTANCE), newBitSet, toLocalVertex);
                        }

                        // 3. examine local vertex
                        // [NOTE]: if the cardinality is already computed previously in step 2, the value in rowCountCache will be directly used
                        Util.fillInRowCount(localVertex, rowCountCache, newBitSet, localVertex.convention);
                        localVertex.cost = vertex1.cost + vertex2.cost + FedConvention.LOCAL_INSTANCE.costEstimator.computeInplaceJoin(localVertex.rowCount, vertex1.rowCount, vertex2.rowCount);
                        addToLevelIfBetter(FedConvention.LOCAL_INSTANCE, levels.get(curLevel).get(FedConvention.LOCAL_INSTANCE), newBitSet, localVertex);
                    }
                }
            }
        }

        // extract result
        assert levels.get(nJoinFactors).get(FedConvention.LOCAL_INSTANCE).size() == 1;
        PVertex finalVertex = levels.get(nJoinFactors).get(FedConvention.LOCAL_INSTANCE).values().iterator().next();

        // create top project
        Mappings.TargetMapping mapping = Mappings.createIdentity(0);
        for (int i = 0; i < nJoinFactors; ++i) {
            final Mappings.TargetMapping tmp = Mappings.offsetTarget(Mappings.offsetSource(
                    Mappings.createIdentity(multiJoin.getNumFieldsInJoinFactor(i)),
                    multiJoin.getJoinStart(i),
                    multiJoin.getNumTotalFields()), finalVertex.factorOffets[i]);
            mapping = Mappings.merge(mapping, tmp);
        }

        RelNode finalNode;
        if (config.isPlain()) {
            finalNode = Util.dumpPlanPlain(multiJoin, relBuilder, rexBuilder, finalVertex, variableUpdater);
        } else {
            finalNode = Util.dumpPlan(finalVertex);
        }
        relBuilder.push(finalNode).project(relBuilder.fields(mapping));
        call.transformTo(relBuilder.build());
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableFedMultiJoinDPsizeRule.Config.builder().build()
                .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());

        Config PLAIN = ImmutableFedMultiJoinDPsizeRule.Config.builder().build()
                .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs())
                .withIsPlain(true);

        @Value.Default default boolean isPlain() {
            return false;
        }
        ImmutableFedMultiJoinDPsizeRule.Config withIsPlain(boolean plain);

        @Override default FedMultiJoinDPsizeRule toRule() { return new FedMultiJoinDPsizeRule(this); }
    }
}
