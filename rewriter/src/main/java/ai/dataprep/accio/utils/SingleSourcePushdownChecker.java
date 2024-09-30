package ai.dataprep.accio.utils;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTableScan;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Optional;

public class SingleSourcePushdownChecker extends RelVisitor {
    public Optional<FedConvention> convention = Optional.empty();
    boolean notSupported = false;
    boolean forCard;

    public SingleSourcePushdownChecker(boolean forCard) {
        this.forCard = forCard;
    }

    public void clear() {
        this.convention = Optional.empty();
        this.notSupported = false;
    }

    @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        if (node instanceof HepRelVertex) {
            node = ((HepRelVertex) node).getCurrentRel();
        } else if (node instanceof RelSubset) {
            node = ((RelSubset) node).getOriginal();
        }

        // Traverse child node
        super.visit(node, ordinal, parent);

        if (notSupported) return;

        if (node instanceof TableScan) {
            if (!(node instanceof FedTableScan)) {
                convention = Optional.empty();
                notSupported = true;
                return; // contain local table
            }
            if (convention.isPresent()) {
                if (!convention.get().equals(node.getConvention())){
                    convention = Optional.empty();
                    notSupported = true;
                    return; // data from different sources, no need to continue
                }
            } else {
                convention = Optional.of((FedConvention) node.getConvention());
            }
        } else {
            if (convention.isPresent()
                    && (!convention.get().supportOperation(node)
                        || (this.forCard && !convention.get().cardinalityEstimator.supportOpInSubPlan(node))
                    )
            ) {
                convention = Optional.empty();
                notSupported = true;
                return; // has operator that is not supported by underlying source
            }
        }
    }
}
