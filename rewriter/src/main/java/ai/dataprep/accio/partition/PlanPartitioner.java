package ai.dataprep.accio.partition;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.RemoteToLocalConverter;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.json.JSONObject;

import javax.annotation.Nullable;

public interface PlanPartitioner {
    void updateWithConfig(JSONObject jsonConfig);

    @Nullable
    PartitionScheme getPartitionScheme(FedConvention convention, RelNode node, double dRows);
    String doPartitionPlan(FedConvention convention, RelNode node, PartitionScheme partitionScheme);

    RelOptCost computeCostAfterPartition(RemoteToLocalConverter rel, RelOptPlanner planner, RelMetadataQuery mq);
    Double computeCostAfterPartition(Double rowCount, PartitionScheme partitionScheme);

    interface PartitionScheme {
        int getPartitionNum();
        int getTableNum();
    }
}
