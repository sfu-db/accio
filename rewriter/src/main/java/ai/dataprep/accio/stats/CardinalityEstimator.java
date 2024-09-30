package ai.dataprep.accio.stats;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;

import java.util.List;

public interface CardinalityEstimator {
    void updateWithConfig(JSONObject jsonConfig);

    // cardinality estimation for each individual operator
    @Nullable Double getRowCount(FedConvention convention, Join rel, RelMetadataQuery mq);
    @Nullable Double getRowCount(FedConvention convention, Filter rel, RelMetadataQuery mq);
    @Nullable Double getRowCount(FedConvention convention, Aggregate rel, RelMetadataQuery mq);
    @Nullable Double getRowCount(FedTableScan rel, RelMetadataQuery mq);
    boolean supportOpInSubPlan(RelNode rel);

    // specifically made for join pushdown rules
    @Nullable Double getRowCountDirectly(FedConvention convention, RelNode rel);
    @Nullable Double getDomainSizeDirectly(FedConvention convention, RelNode rel, String column);
    List<@Nullable Double> getRowCountsDirectlyByBatch(FedConvention convention, List<RelNode> rels);
}
