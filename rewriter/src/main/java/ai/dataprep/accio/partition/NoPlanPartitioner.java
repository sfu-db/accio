package ai.dataprep.accio.partition;

import ai.dataprep.accio.plan.FedConvention;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;

public class NoPlanPartitioner extends BasePlanPartitioner {
    @Override
    public void updateWithConfig(JSONObject jsonConfig) {}

    @Override
    public @Nullable PartitionScheme getPartitionScheme(FedConvention convention, RelNode node, double dRows) {
        return null;
    }

    @Override
    public String doPartitionPlan(FedConvention convention, RelNode node, PartitionScheme partitionScheme) {
        return null;
    }
}
