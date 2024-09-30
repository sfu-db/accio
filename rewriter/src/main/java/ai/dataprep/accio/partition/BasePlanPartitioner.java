package ai.dataprep.accio.partition;

import ai.dataprep.accio.plan.RemoteToLocalConverter;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public abstract class BasePlanPartitioner implements PlanPartitioner{
    @Override
    public RelOptCost computeCostAfterPartition(RemoteToLocalConverter rel, RelOptPlanner planner, RelMetadataQuery mq) {
        PartitionScheme partitionScheme = rel.getPartitionScheme();
        double dRows = rel.getRowCount();
        RelOptCost cost = planner.getCostFactory().makeCost(computeCostAfterPartition(dRows, partitionScheme), 0, 0);
        return cost;
    }

    @Override
    public Double computeCostAfterPartition(Double rowCount, PartitionScheme partitionScheme) {
        if (partitionScheme.getPartitionNum() <= 1) {
            return rowCount;
        }

        // ratio = 1 / (log_{1+y}(x) + 1)
        // x: # partition y: # tables
        double parallelCostRatio = 1.0 / partitionScheme.getPartitionNum();

        return rowCount * parallelCostRatio;
    }

    public class BasePartitionScheme implements PartitionScheme {
        protected int partNum = 1;
        protected int tableNum = 1;

        @Override
        public int getPartitionNum() {
            return partNum;
        }

        @Override
        public int getTableNum() {
            return tableNum;
        }
    }
}
