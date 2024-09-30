package ai.dataprep.accio.plan;

import org.apache.calcite.rel.RelNode;

public interface FedRel extends RelNode {
    void setRowCount(Double rowCount);
}
