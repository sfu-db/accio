package ai.dataprep.accio.cost;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedRelRules;
import ai.dataprep.accio.plan.FedTableScan;
import ai.dataprep.accio.plan.RemoteToLocalConverter;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;

public class BaseCostEstimator implements CostEstimator {
    public double SCAN_COST_MULTIPLIER;
    public double PROJECT_COST_MULTIPLIER;
    public double FILTER_COST_MULTIPLIER;
    public double JOIN_COST_MULTIPLIER;
    public double AGGREGATE_COST_MULTIPLIER;
    public double SORT_COST_MULTIPLIER;
    public double CONVERTER_COST_MULTIPLIER;

    public BaseCostEstimator(boolean isLocal) {
        SCAN_COST_MULTIPLIER = 0.0d;
        // prefer projection pushdown
        if (isLocal) {
            PROJECT_COST_MULTIPLIER = 1.0d;
        } else {
            PROJECT_COST_MULTIPLIER = 0.0d;
        }
        FILTER_COST_MULTIPLIER = 0.0d;
        JOIN_COST_MULTIPLIER = 1.0d;
        AGGREGATE_COST_MULTIPLIER = 1.0d;
        SORT_COST_MULTIPLIER = 1.0d;
        CONVERTER_COST_MULTIPLIER = 1.0d;
    }


    @Override
    public void updateWithConfig(JSONObject jsonConfig) {
        if (jsonConfig.has("costParams")) {
            JSONObject params = jsonConfig.getJSONObject("costParams");
            if (params.has("scan")) {
                SCAN_COST_MULTIPLIER = params.getDouble("scan");
            }
            if (params.has("project")) {
                PROJECT_COST_MULTIPLIER = params.getDouble("project");
            }
            if (params.has("filter")) {
                FILTER_COST_MULTIPLIER = params.getDouble("filter");
            }
            if (params.has("join")) {
                JOIN_COST_MULTIPLIER = params.getDouble("join");
            }
            if (params.has("agg")) {
                AGGREGATE_COST_MULTIPLIER = params.getDouble("agg");
            }
            if (params.has("sort")) {
                SORT_COST_MULTIPLIER = params.getDouble("sort");
            }
            if (params.has("trans")) {
                CONVERTER_COST_MULTIPLIER = params.getDouble("trans");
            }
        }
    }

    @Override
    public @Nullable RelOptCost computeCost(FedRelRules.FedRelJoin rel, RelOptPlanner planner, RelMetadataQuery mq) {
        // Assume HashJoin O(M+N)
//        final double leftRowCount = mq.getRowCount(rel.getLeft());
//        final double rightRowCount = mq.getRowCount(rel.getRight());
        // use C_out
        final double resRowCount = mq.getRowCount(rel);
        return planner.getCostFactory().makeCost(computeInplaceJoin(resRowCount, null, null), 0, 0);
    }

    @Override
    public @Nullable RelOptCost computeCost(FedRelRules.FedRelProject rel, RelOptPlanner planner, RelMetadataQuery mq) {
        double dRows = mq.getRowCount(rel.getInput());
        RelOptCost cost = planner.getCostFactory().makeCost(dRows, 0, 0).multiplyBy(PROJECT_COST_MULTIPLIER);
        return cost;
    }

    @Override
    public @Nullable RelOptCost computeCost(FedRelRules.FedRelFilter rel, RelOptPlanner planner, RelMetadataQuery mq) {
        double dRows = mq.getRowCount(rel.getInput());
        return planner.getCostFactory().makeCost(dRows, 0, 0).multiplyBy(FILTER_COST_MULTIPLIER);
    }

    @Override
    public @Nullable RelOptCost computeCost(FedRelRules.FedRelAggregate rel, RelOptPlanner planner, RelMetadataQuery mq) {
        // === Copy Begin ===
        // === Copied from `Aggregate.computeSelfCost()`
        // === but replacing using `mq.getRowCount(this)` with `mq.getRowCount(getInput())`
        double rowCount = mq.getRowCount(rel.getInput());
        // Aggregates with more aggregate functions cost a bit more
        float multiplier = 1f + (float) rel.getAggCallList().size() * 0.125f;
        for (AggregateCall aggCall : rel.getAggCallList()) {
            if (aggCall.getAggregation().getName().equals("SUM")) {
                // Pretend that SUM costs a little bit more than $SUM0,
                // to make things deterministic.
                multiplier += 0.0125f;
            }
        }
        // === Copy End ===
        return planner.getCostFactory().makeCost(rowCount * multiplier, 0, 0).multiplyBy(AGGREGATE_COST_MULTIPLIER);
    }

    @Override
    public @Nullable RelOptCost computeCost(FedRelRules.FedRelSort rel, RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost cost = rel.getLogicalCost(planner, mq);
        if (cost == null) {
            return null;
        }
        return cost.multiplyBy(SORT_COST_MULTIPLIER);
    }

    @Override
    public @Nullable RelOptCost computeCost(FedTableScan rel, RelOptPlanner planner, RelMetadataQuery mq) {
        double dRows = mq.getRowCount(rel);
        return planner.getCostFactory().makeCost(dRows, 1, 0).multiplyBy(SCAN_COST_MULTIPLIER);
    }

    @Override
    public @Nullable RelOptCost computeCost(FedConvention inConvention, RemoteToLocalConverter rel, RelOptPlanner planner, RelMetadataQuery mq) {
        if (rel.getPartitionScheme() != null) {
            return inConvention.planPartitioner.computeCostAfterPartition(rel, planner, mq).multiplyBy(CONVERTER_COST_MULTIPLIER);
        }

        double dRows = rel.getRowCount();
        RelOptCost cost = planner.getCostFactory().makeCost(computeTransferCost(dRows), 0, 0);
        return cost;
    }

    @Override
    public Double computeTransferCost(Double rowCount) {
        return rowCount * CONVERTER_COST_MULTIPLIER;
    }

    @Override
    public Double computeInplaceJoin(Double resCount, Double leftRowCount, Double rightRowCount) {
        // use C_out to estimate join cost
        return resCount * JOIN_COST_MULTIPLIER;
    }
}
