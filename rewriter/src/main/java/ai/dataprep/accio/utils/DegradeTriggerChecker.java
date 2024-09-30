package ai.dataprep.accio.utils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DegradeTriggerChecker extends RelVisitor {
    private boolean degrade = false;
    private boolean inCorrelate = false;

    public boolean shouldDegrade() {return degrade;}

    @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        boolean localInCorrelate = false;
        if (node instanceof LogicalCorrelate) {
            if (this.inCorrelate) {
                degrade = true;
                return;
            }
            this.inCorrelate = true;
            localInCorrelate = true;
        }
        // Traverse child node
        super.visit(node, ordinal, parent);

        if (localInCorrelate) this.inCorrelate = false;
    }
}
