package ai.dataprep.accio.utils;

import ai.dataprep.accio.DBSourceVisitor;
import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTableScan;
import ai.dataprep.accio.plan.RemoteSQLTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class NaivePlanGenerator extends DBSourceVisitor {
    private class DeepTraverser extends RexVisitorImpl {
        public DeepTraverser(NaivePlanGenerator relVisitor) {
            super(true);
            this.relVisitor = relVisitor;
        }

        NaivePlanGenerator relVisitor;

        @Override
        public Object visitSubQuery(RexSubQuery subQuery) {
            if (subQuery.rel != null) {
                this.relVisitor.go(subQuery.rel);
            }

            return super.visitSubQuery(subQuery);
        }
    }

    public NaivePlanGenerator() {
        super();
        this.traverser = new DeepTraverser(this);
        this.tableAliasMap = new HashMap<>();
    }
    private DeepTraverser traverser;
    private HashMap<String, String> tableAliasMap;

    public String rewrite(String sql) {
        for (Map.Entry<String, String> entry: this.tableAliasMap.entrySet()) {
            sql = sql.replaceAll(entry.getValue(), entry.getKey());
        }
        return sql;
    }

    @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        if (node instanceof LogicalFilter) {
            RexNode condition = ((LogicalFilter) node).getCondition();
            condition.accept(this.traverser);
        }

        if (node instanceof FedTableScan) {
            if (isSubPlanNew(node, ordinal, parent)) {
                FedTableScan scan = (FedTableScan) node;
                final FedConvention convention =
                        (FedConvention) requireNonNull(scan.getConvention(),
                                () -> "child.getConvention() is null for " + scan);
                String dbName = convention.getName();
                String originFullName = dbName + "\\." + scan.fedTable.tableName;
                dbName = plan.add(dbName, "select * from " + scan.fedTable.tableName, 0);

                this.tableAliasMap.put(dbName, originFullName);

                RemoteSQLTableScan newScan = RemoteSQLTableScan.create(dbName, node);
                addSubPlan(scan, newScan);
            }
        } else {
            // Traverse child node
            super.visit(node, ordinal, parent);
        }
    }
}
