package ai.dataprep.accio.rules;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedRel;
import ai.dataprep.accio.plan.FedRelRules;
import ai.dataprep.accio.plan.RemoteToLocalConverter;
import ai.dataprep.accio.utils.*;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
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

import java.util.*;

/*
* Baseline: Pushdown (non-cross product) join as many as possible.
* */

@Value.Enclosing
public class FedMultiJoinPushdownRule
        extends RelRule<FedMultiJoinPushdownRule.Config>
        implements TransformationRule {


    protected FedMultiJoinPushdownRule(Config config) {
        super(config);
    }
    private static final Logger logger = LoggerFactory.getLogger(FedMultiJoinPushdownRule.class);

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
        final int nJoinFactors = multiJoin.getNumJoinFactors();
        final List<RexNode> joinFilters = multiJoin.getJoinFilters();
        VariableUpdater variableUpdater = new VariableUpdater(rexBuilder, multiJoin);

        // init vertexes
        List<PVertex> vertexes = new ArrayList<>();
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
            int[] factorOffsets = new int[nJoinFactors];
            Arrays.fill(factorOffsets, -1);
            factorOffsets[i] = 0;
            ImmutableBitSet unusedFilters = filterBitMap.build();

            // do not need to get the real values since no cost estimation is needed
            double[] fieldDomains = new double[multiJoin.getNumFieldsInJoinFactor(i)];
            Arrays.fill(fieldDomains, -1);

            pushdownChecker.go(rel);
            // [Assumption] all operations in a base factor are conducted in the source it is from
            // since the plan is then fixed, we do not need to consider their cost (all base factor have 0 cost)
            if (!pushdownChecker.convention.isPresent()) {
                logger.warn("Base factor cannot be pushdown to single source!");
            }
            FedConvention convention = pushdownChecker.convention.orElse(FedConvention.LOCAL_INSTANCE);
            PVertex vertex = new PVertex(convention, convention, rel, ImmutableBitSet.of(i), ImmutableBitSet.of(), unusedFilters, factorOffsets, multiJoin.getNumFieldsInJoinFactor(i), fieldDomains);
            vertexes.add(vertex);
            pushdownChecker.clear();
        }

        // construct join trees from the same data source locally (without cross product)
        List<PVertex> singleSourceGroups = new ArrayList<>();
        PVertex current = null;
        while (!vertexes.isEmpty()) {
            if (current == null) {
                current = vertexes.remove(0);
            }
            // try to join with local factors
            // NOTE: only support filters with two factors for now in this baseline
            // TODO: extend to multi-factor join filter
            boolean hasJoinCounterpart = false;
            for (int i = 0; i < vertexes.size(); ++i) {
                PVertex candidate = vertexes.get(i);
                if (candidate.convention != current.convention) continue;
                PVertex join = Util.constructJoinVertex(multiJoin, relBuilder, rexBuilder, current, candidate, variableUpdater, current.convention);
                if (join != null) {
                    vertexes.remove(i);
                    current = join;
                    hasJoinCounterpart = true;
                    break;
                }
            }
            // continue to find the next local join factor
            if (hasJoinCounterpart) continue;
            // cannot find a local join factor
            current.outConvention = FedConvention.LOCAL_INSTANCE; // fetch result to local
            singleSourceGroups.add(current);
            current = null;
        }

        // add the last group if any
        if (current != null) {
            current.outConvention = FedConvention.LOCAL_INSTANCE; // fetch result to local
            singleSourceGroups.add(current);
        }

        // construct local joins from different data sources
        current = singleSourceGroups.remove(0);
        while(!singleSourceGroups.isEmpty()) {
            for (int i = 0; i < singleSourceGroups.size(); ++i) {
                PVertex join = Util.constructJoinVertex(multiJoin, relBuilder, rexBuilder, current, singleSourceGroups.get(i), variableUpdater, FedConvention.LOCAL_INSTANCE);
                if (join != null) {
                    singleSourceGroups.remove(i);
                    current = join;
                    break;
                }
            }
        }

        // create top project
        Mappings.TargetMapping mapping = Mappings.createIdentity(0);
        for (int i = 0; i < nJoinFactors; ++i) {
            final Mappings.TargetMapping tmp = Mappings.offsetTarget(Mappings.offsetSource(
                    Mappings.createIdentity(multiJoin.getNumFieldsInJoinFactor(i)),
                    multiJoin.getJoinStart(i),
                    multiJoin.getNumTotalFields()), current.factorOffets[i]);
            mapping = Mappings.merge(mapping, tmp);
        }

//        RelNode finalNode = dumpPlan(current);
        RelNode finalNode = dumpPlanPlain(multiJoin, relBuilder, rexBuilder, current, variableUpdater);
        relBuilder.push(finalNode).project(relBuilder.fields(mapping));
        call.transformTo(relBuilder.build());
    }

    protected RelNode dumpPlanPlain(LoptMultiJoin multiJoin,
                                    RelBuilder relBuilder,
                                    RexBuilder rexBuilder,
                                    PVertex vertex,
                                    VariableUpdater variableUpdater){
        if (vertex.convention != vertex.outConvention) {
            assert vertex.outConvention == FedConvention.LOCAL_INSTANCE;
            // all the vertex will be pushed down to remote and transfer the result back
            PhysicalPlanExtractor extractor = new PhysicalPlanExtractor(vertex.convention);
            RelNode rel = extractor.extract(Util.convertToPlain(multiJoin, relBuilder, rexBuilder, vertex, variableUpdater));
            ((FedRel)rel).setRowCount(100.0d);
            // fake row count, do not partition the pushdown query
            return new RemoteToLocalConverter(vertex.rel.getCluster(), RelTraitSet.createEmpty().plus(FedConvention.LOCAL_INSTANCE), rel, null, 100);
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
            fedJoin.setRowCount(100.0d); // fake row count
            return fedJoin;
        } catch (InvalidRelException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected RelNode dumpPlan(PVertex vertex){
        if (vertex.convention != vertex.outConvention) {
            assert vertex.outConvention == FedConvention.LOCAL_INSTANCE;
            // all the vertex will be pushed down to remote and transfer the result back
            PhysicalPlanExtractor extractor = new PhysicalPlanExtractor(vertex.convention);
            RelNode rel = extractor.extract(vertex.rel);
            ((FedRel)rel).setRowCount(100.0d);
            // fake row count, do not partition the pushdown query
            return new RemoteToLocalConverter(vertex.rel.getCluster(), RelTraitSet.createEmpty().plus(FedConvention.LOCAL_INSTANCE), rel, null, 100);
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
            fedJoin.setRowCount(100.0d); // fake row count
            return fedJoin;
        } catch (InvalidRelException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableFedMultiJoinPushdownRule.Config.builder().build()
                .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());

        @Override default FedMultiJoinPushdownRule toRule() { return new FedMultiJoinPushdownRule(this); }
    }
}
