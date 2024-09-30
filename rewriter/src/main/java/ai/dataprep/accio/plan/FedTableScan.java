package ai.dataprep.accio.plan;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class FedTableScan extends TableScan implements FedRel {
    public final FedTable fedTable;

    public String getTableName() {
        return fedTable.getTableName();
    }

    protected FedTableScan(RelOptCluster cluster, List<RelHint> hints, RelOptTable table, FedTable fedTable, FedConvention convention) {
        super(cluster, cluster.traitSetOf(convention), hints, table);
        this.fedTable = fedTable;
    }

    public FedTableScan copyWithNewCluster(RelOptCluster cluster) {
        return new FedTableScan(cluster, this.hints, this.table, this.fedTable, (FedConvention)this.getConvention());
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        Double rowCount = fedTable.getRowCount();
        if (rowCount != null) {
            return rowCount;
        }
        rowCount = ((FedConvention)getConvention()).cardinalityEstimator.getRowCount(this, mq);
        fedTable.setRowCount(rowCount);
        return rowCount;
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                                          RelMetadataQuery mq) {
        return ((FedConvention)getConvention()).costEstimator.computeCost(this, planner, mq);
    }

    @Override
    public void setRowCount(Double rowCount) {
        fedTable.setRowCount(rowCount);
    }
}
