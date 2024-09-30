package ai.dataprep.accio.utils;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;

public class LocalOnlyChecker extends RelVisitor {
    private boolean hasRemote = false;

    public boolean isHasRemote() {return hasRemote;}

    @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        if (node instanceof FedTableScan && node.getConvention() != FedConvention.LOCAL_INSTANCE) {
            this.hasRemote = true;
            return; // no need to continue
        }
        // Traverse child node
        super.visit(node, ordinal, parent);
    }
}
