package ai.dataprep.accio.partition;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedRelRules;
import ai.dataprep.accio.plan.FedTableScan;
import ai.dataprep.accio.sql.FedRelSqlImplementor;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;

import javax.sql.DataSource;
import java.util.*;

/*
* Partition on the largest table for SPJ queries
* */

public abstract class LargestTablePlanPartitioner extends BasePlanPartitioner{
    protected int MAX_ROW_PER_PARTITION = 1000000;
    protected int MAX_PARTITION_SINGLE_TABLE = 16;
    public TableCollector partitionChecker = new TableCollector();

    public abstract List<String> getPartitionFilters(FedConvention convention, PartitionScheme partitionScheme);

    // invoke at most once for each table
    public abstract @Nullable String getPartitionColumn(FedTableScan tableScan);

    @Override
    public void updateWithConfig(JSONObject jsonConfig) {
        if (jsonConfig.has("partition")) {
            JSONObject params = jsonConfig.getJSONObject("partition");
            if (params.has("max_partition_per_table")) {
                MAX_PARTITION_SINGLE_TABLE = params.getInt("max_partition_per_table");
            }
            if (params.has("max_row_per_partition")) {
                MAX_ROW_PER_PARTITION = params.getInt("max_row_per_partition");
            }
        }
    }

    @Override
    public Double computeCostAfterPartition(Double rowCount, PartitionScheme partitionScheme) {
        if (partitionScheme.getPartitionNum() <= 1) {
            return rowCount;
        }

        // ratio = 1 / (log_{1+y}(x) + 1)
        // x: # partition y: # tables
        double parallelCostRatio = 1.0 / ((Math.log(partitionScheme.getPartitionNum()) / Math.log(1+partitionScheme.getTableNum())) + 1);

        return rowCount * parallelCostRatio;
    }

    @Override
    public @Nullable PartitionScheme getPartitionScheme(FedConvention convention, RelNode node, double dRows) {
        DataSource dataSource = convention.dataSource;
        if (dataSource == null) {
            return null;
        }
        if (dRows < MAX_ROW_PER_PARTITION) {
            return null;
        }

        partitionChecker.reset();
        partitionChecker.go(node);

        TableCollector.SubPlan s = partitionChecker.getLargest();
        int numTables = partitionChecker.scans.size();
        if (s == null) {
            return null;
        }
        double maxPart = MAX_PARTITION_SINGLE_TABLE / Math.pow(4, numTables-1);
        if (maxPart <= 1) {
            return null;
        }

        // decide partition number based on largest table instead of output cardinality
        int partNum = Math.min((int) Math.ceil(s.scan.fedTable.getRowCount()/MAX_ROW_PER_PARTITION), (int) maxPart);
        if (partNum <= 1) {
            return null;
        }
        return new LargestTablePartitionScheme(partNum, numTables, s.column, s.scan);
    }

    @Override
    public String doPartitionPlan(FedConvention convention, RelNode node, PartitionScheme partitionScheme) {
        LargestTablePlanPartitioner.LargestTablePartitionScheme scheme = (LargestTablePlanPartitioner.LargestTablePartitionScheme)partitionScheme;
        List<String> filters = getPartitionFilters(convention, partitionScheme);

        // Create a filter as placeholder on top of partition base table
        AddPartitionFilterToBaseTableVisitor partitioner = new AddPartitionFilterToBaseTableVisitor(scheme.scan);
        node = partitioner.go(node);

        // Convert to SQL
        SqlDialect dialect = ((FedConvention) node.getConvention()).dialect;
        FedRelSqlImplementor sqlConverter = new FedRelSqlImplementor(dialect);
        RelNode finalPlan = sqlConverter.prepare(node, dialect);
        RelToSqlConverter.Result res = sqlConverter.visitRoot(finalPlan);
        SqlNode resSqlNode = res.asQueryOrValues();
        String sql = resSqlNode.toSqlString(dialect).getSql();

        String partSql = "";
        for (String filterString: filters) {
            if (!partSql.isEmpty()) {
                partSql += ";\n";
            }
            partSql += sql.replaceAll("\\?", filterString);
        }
        return partSql;
    }

    public class LargestTablePartitionScheme extends BasePartitionScheme {
        public String column;
        public FedTableScan scan;

        public LargestTablePartitionScheme(int partNum, int tableNum, String column, FedTableScan scan) {
            this.partNum = partNum;
            this.tableNum = tableNum;
            this.column = column;
            this.scan = scan;
        }
    }

    public class TableCollector extends RelVisitor {
        public class SubPlan {
            FedTableScan scan;
            String column;

            public SubPlan(FedTableScan scan) {
                this.scan = scan;
            }
        }

        public class SubPlanComparator implements Comparator<SubPlan> {
            @Override
            public int compare(SubPlan o1, SubPlan o2) {
                // descending
                return (int) (o2.scan.fedTable.getRowCount() - o1.scan.fedTable.getRowCount());
            }
        }

        public ArrayList<SubPlan> scans = new ArrayList<>();
        public HashMap<FedTableScan, String> planCache = new HashMap<>();

        public void reset() {
            this.scans = new ArrayList<>();
        }

        public @Nullable SubPlan getLargest() {
            Collections.sort(scans, new SubPlanComparator());
            for (SubPlan s: scans) {
                if (!planCache.containsKey(s.scan)) {
                    planCache.put(s.scan, getPartitionColumn(s.scan));
                }
                if (planCache.get(s.scan) != null) {
                    s.column = planCache.get(s.scan);
                    return s;
                }
            }
            return null;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            if (node instanceof Aggregate || node instanceof Sort) {
                return;
            }

            if (node instanceof RelSubset) {
                if (((RelSubset) node).getBest() == null) {
                    // this means it is not a feasible path
                    this.scans.clear();
                    return;
                }
                this.visit(((RelSubset) node).getOriginal(), ordinal, parent);
            } else if (node instanceof FedTableScan) {
                this.scans.add(new SubPlan((FedTableScan)node));
            } else {
                super.visit(node, ordinal, parent);
            }
        }
    }

    public static class AddPartitionFilterToBaseTableVisitor extends RelVisitor {
        final public FedTableScan scan;

        public AddPartitionFilterToBaseTableVisitor(FedTableScan scan) {
            this.scan = scan;
        }

        // place a filter as placeholder on top of the partition table
        @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            if (node instanceof FedTableScan) {
                if (scan.equals(node)) {
                    final RexBuilder rexBuilder = node.getCluster().getRexBuilder();
                    RelDataType type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.SYMBOL);
                    RexNode condition = new RexDynamicParam(type, 0);
                    FedRelRules.FedRelFilter filter = new FedRelRules.FedRelFilter(node.getCluster(), node.getTraitSet(), node, condition);

                    if (parent == null) {
                        replaceRoot(filter);
                    } else {
                        parent.replaceInput(ordinal, filter);
                    }
                }
            }
            else {
                // Traverse child node
                super.visit(node, ordinal, parent);
            }
        }
    }
}
