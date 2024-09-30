package ai.dataprep.accio.stats;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;

import java.util.List;
import java.util.Optional;

public class BaseCardinalityEstimator implements CardinalityEstimator {
    @Override
    public void updateWithConfig(JSONObject jsonConfig) { }

    @Override
    public @Nullable Double getRowCount(FedConvention convention, Join rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    @Override
    public @Nullable Double getRowCount(FedConvention convention, Filter rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    @Override
    public @Nullable Double getRowCount(FedConvention convention, Aggregate rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    @Override
    public @Nullable Double getRowCount(FedTableScan rel, RelMetadataQuery mq) {
        // assume all relations have 100 rows by default
        return 100.0d;
    }

    @Override
    public boolean supportOpInSubPlan(RelNode rel) {
        return true;
    }

    @Override
    public @Nullable Double getRowCountDirectly(FedConvention convention, RelNode rel) {
        return null;
    }

    @Override
    public @Nullable Double getDomainSizeDirectly(FedConvention convention, RelNode rel, String column) {
        return this.getRowCountDirectly(convention, rel);
    }

    @Override
    public List<@Nullable Double> getRowCountsDirectlyByBatch(FedConvention convention, List<RelNode> rels) {
        return null;
    }


}
