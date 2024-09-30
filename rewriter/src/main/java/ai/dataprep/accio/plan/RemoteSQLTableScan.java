package ai.dataprep.accio.plan;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.Collections;

public class RemoteSQLTableScan extends TableScan implements FedRel {
    public final RemoteSQLTable remoteSQLTable;

    public static RemoteSQLTableScan create(String name, RelNode node) {
        RemoteSQLTable remoteSQLTable = new RemoteSQLTable(Collections.singletonList(name), node.getRowType());
        return new RemoteSQLTableScan(node.getCluster(), node.getTraitSet(), remoteSQLTable);
    }

    protected RemoteSQLTableScan(RelOptCluster cluster, RelTraitSet traitSet, RemoteSQLTable table) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.remoteSQLTable = table;
    }

    @Override
    public void setRowCount(Double rowCount) {
        // do nothing
    }
}
