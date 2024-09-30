package ai.dataprep.accio.cost;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedRelRules;
import ai.dataprep.accio.plan.FedTableScan;
import ai.dataprep.accio.plan.RemoteToLocalConverter;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;

public interface CostEstimator {
    void updateWithConfig(JSONObject jsonConfig);

    // cost computation for each individual operator
    @Nullable RelOptCost computeCost(FedRelRules.FedRelJoin rel, RelOptPlanner planner, RelMetadataQuery mq);
    @Nullable RelOptCost computeCost(FedRelRules.FedRelProject rel, RelOptPlanner planner, RelMetadataQuery mq);
    @Nullable RelOptCost computeCost(FedRelRules.FedRelFilter rel, RelOptPlanner planner, RelMetadataQuery mq);
    @Nullable RelOptCost computeCost(FedRelRules.FedRelAggregate rel, RelOptPlanner planner, RelMetadataQuery mq);
    @Nullable RelOptCost computeCost(FedRelRules.FedRelSort rel, RelOptPlanner planner, RelMetadataQuery mq);
    @Nullable RelOptCost computeCost(FedTableScan rel, RelOptPlanner planner, RelMetadataQuery mq);
    @Nullable RelOptCost computeCost(FedConvention inConvention, RemoteToLocalConverter rel, RelOptPlanner planner, RelMetadataQuery mq);

    // specifically made for join pushdown rules
    Double computeTransferCost(Double rowCount);
    Double computeInplaceJoin(Double resCount, Double leftRowCount, Double rightRowCount);
}
