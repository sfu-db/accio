package ai.dataprep.accio;

import ai.dataprep.accio.plan.FedRel;
import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.RemoteSQLTableScan;
import ai.dataprep.accio.plan.RemoteToLocalConverter;
import ai.dataprep.accio.sql.FedRelSqlImplementor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DBSourceVisitor extends RelVisitor {

    private static final Logger logger = LoggerFactory.getLogger(DBSourceVisitor.class);

    protected FederatedPlan plan = new FederatedPlan();

    protected class SubPlan {
        public RelNode node;
        public RemoteSQLTableScan scan;

        public SubPlan(RelNode node, RemoteSQLTableScan scan) {
            this.node = node;
            this.scan = scan;
        }

        boolean isEqual(RelNode node1) {
            return this.node.deepEquals(node1);
        }
    }
    protected List<SubPlan> traversedSubPlans = new ArrayList<>();
    public FederatedPlan getPlan() {
        return plan;
    }

    protected boolean isSubPlanNew(RelNode node, int ordinal, RelNode parent) {
        // Check whether there is a same sub-plan already generated
        for (SubPlan subPlan: this.traversedSubPlans) {
            if (subPlan.isEqual(node)) {
                parent.replaceInput(ordinal, subPlan.scan);
                return false;
            }
        }
        return true;
    }

    protected void addSubPlan(RelNode node, RemoteSQLTableScan scan) {
        this.traversedSubPlans.add(new SubPlan(node, scan));
    }

    @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        if (node instanceof RemoteToLocalConverter) {
            RemoteToLocalConverter converter = (RemoteToLocalConverter) node;
            final FedRel child = (FedRel) converter.getInput(0);
            if (isSubPlanNew(child, ordinal, parent)) {
                // Convert subtree to sql, do not go deeper
                final FedConvention convention =
                        (FedConvention) requireNonNull(child.getConvention(),
                                () -> "child.getConvention() is null for " + child);
                String resSql = FedRelSqlImplementor.ConvertToRemoteSql(convention, child, converter.getPartitionScheme());
                String dbName = convention.getName();
                dbName = plan.add(dbName, resSql, converter.getRowCount());

                // Replace subplan with new table for SQL query
                RemoteSQLTableScan scan = RemoteSQLTableScan.create(dbName, child);
                if (parent != null) {
                    parent.replaceInput(ordinal, scan);
                } else {
                    this.replaceRoot(scan);
                }

                // Add to cache
                addSubPlan(child, scan);
            }
        }
        else {
            // Traverse child node
            super.visit(node, ordinal, parent);
        }
    }
}
