package ai.dataprep.accio.plan;

import ai.dataprep.accio.partition.PlanPartitioner;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import javax.annotation.Nullable;
import java.util.List;

public class RemoteToLocalConverter extends ConverterImpl {
    protected long rowCount;
    protected PlanPartitioner.PartitionScheme partitionScheme;

    public long getRowCount() { return rowCount; }
    public PlanPartitioner.PartitionScheme getPartitionScheme() { return partitionScheme; }

    /**
     * Creates a ConverterImpl.
     *
     * @param cluster  planner's cluster
     * @param traits   the output traits of this converter
     * @param child    child rel (provides input traits)
     */
    public RemoteToLocalConverter(RelOptCluster cluster, RelTraitSet traits, RelNode child, PlanPartitioner.PartitionScheme partitionScheme, long rowCount) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, child);
        this.partitionScheme = partitionScheme;
        this.rowCount = rowCount;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new RemoteToLocalConverter(
                getCluster(), traitSet, sole(inputs), partitionScheme, rowCount);
    }

    @Override public @Nullable
    RelOptCost computeSelfCost(RelOptPlanner planner,
                               RelMetadataQuery mq) {
        FedConvention inConvention = (FedConvention)getInput().getConvention();
        return inConvention.costEstimator.computeCost(inConvention, this, planner, mq);
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return rowCount;
    }
}

