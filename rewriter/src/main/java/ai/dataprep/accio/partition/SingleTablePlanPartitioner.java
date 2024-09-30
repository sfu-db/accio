package ai.dataprep.accio.partition;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedRelRules;
import ai.dataprep.accio.plan.FedTableScan;
import ai.dataprep.accio.sql.FedRelSqlImplementor;
import ai.dataprep.accio.utils.Util;
import org.apache.calcite.plan.hep.HepRelVertex;
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

public abstract class SingleTablePlanPartitioner extends BasePlanPartitioner{
    protected int MAX_ROW_PER_PARTITION = 100000;
    protected int MAX_PARTITION_SINGLE_TABLE = 256;
    protected int MAX_PARALLELISM = 32;
    protected String OUTPUT_FORMAT = "LIST_OF_QUERIES";

    public TableCollector partitionChecker = new TableCollector();

    public abstract List<String> getPartitionFilters(FedConvention convention, PartitionScheme partitionScheme);

    // invoke at most once for each table
    public abstract @Nullable String getPartitionColumn(RelNode node, FedTableScan tableScan);

    public abstract JSONObject getPartitionInfo(FedConvention convention, PartitionScheme partitionScheme);

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
            if (params.has("max_parallelism")) {
                MAX_PARALLELISM = params.getInt("max_parallelism");
            }
            if (params.has("output_format")) {
                OUTPUT_FORMAT = params.getString("output_format").toUpperCase();
            }
        }
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

        // only work for single table subquery
        if (partitionChecker.isSingle) {
            String partColumn = getPartitionColumn(node, partitionChecker.scan);
            if (partColumn != null) {
                int partNum = Math.min((int) Math.ceil(partitionChecker.scan.estimateRowCount(partitionChecker.scan.getCluster().getMetadataQuery())/MAX_ROW_PER_PARTITION), MAX_PARTITION_SINGLE_TABLE);
                return new SingleTablePartitionScheme(partNum, partColumn, partitionChecker.scan);
            }
        }
        return null;
    }

    @Override
    public String doPartitionPlan(FedConvention convention, RelNode node, PartitionScheme partitionScheme) {
        SingleTablePartitionScheme scheme = (SingleTablePartitionScheme) partitionScheme;

        if (OUTPUT_FORMAT.equals("SPARK_INFO")) {
            // Convert to SQL
            SqlDialect dialect = ((FedConvention) node.getConvention()).dialect;
            FedRelSqlImplementor sqlConverter = new FedRelSqlImplementor(dialect);
            RelNode finalPlan = sqlConverter.prepare(node, dialect);
            RelToSqlConverter.Result res = sqlConverter.visitRoot(finalPlan);
            SqlNode resSqlNode = res.asQueryOrValues();
            String sql = resSqlNode.toSqlString(dialect).getSql();

            JSONObject partInfo = getPartitionInfo(convention, partitionScheme);
            String partSql = "";
            partSql += (scheme.column + "|" + scheme.partNum + "|" + partInfo.getString("lowerBound") + "|" + partInfo.getString("upperBound") + "|" + "(" + sql + ") as subq");
            return partSql;
        }

        List<String> filters = getPartitionFilters(convention, partitionScheme);

        // Deep copy the plan tree
        // otherwise we may modify other places of the overall plan that points to the same RelNode
        RelNode copyNode = Util.deepCopy(node);

        // Create a filter as placeholder on top of partition base table
        AddPartitionFilterToBaseTableVisitor partitioner = new AddPartitionFilterToBaseTableVisitor(scheme.scan);
        copyNode = partitioner.go(copyNode);

        // Convert to SQL
        SqlDialect dialect = ((FedConvention) copyNode.getConvention()).dialect;
        FedRelSqlImplementor sqlConverter = new FedRelSqlImplementor(dialect);
        RelNode finalPlan = sqlConverter.prepare(copyNode, dialect);
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

    @Override
    public Double computeCostAfterPartition(Double rowCount, PartitionScheme partitionScheme) {
        if (partitionScheme.getPartitionNum() <= 1) {
            return rowCount;
        }

        double parallelCostRatio = 1.0 / Math.min(MAX_PARALLELISM, partitionScheme.getPartitionNum());
        return rowCount * parallelCostRatio;
    }

    public class SingleTablePartitionScheme extends BasePartitionScheme {
        public String column;
        public FedTableScan scan;

        public SingleTablePartitionScheme(int partNum, String column, FedTableScan scan) {
            this.partNum = partNum;
            this.column = column;
            this.scan = scan;
        }
    }

    public class TableCollector extends RelVisitor {
        public FedTableScan scan = null;
        public boolean isSingle = true;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            if (!isSingle || node instanceof Aggregate || node instanceof Sort) {
                isSingle = false;
                return;
            }

            if (node instanceof RelSubset) {
                this.visit(((RelSubset) node).getOriginal(), ordinal, parent);
            } else if (node instanceof HepRelVertex) {
                this.visit(((HepRelVertex) node).getCurrentRel(), ordinal, parent);
            } else if (node instanceof FedTableScan) {
                if (this.scan != null) {
                    this.isSingle = false;
                    return;
                }
                this.scan = (FedTableScan)node;
                return;
            } else {
                // visit children
                super.visit(node, ordinal, parent);
            }
        }

        public void reset() {
            this.isSingle = true;
            this.scan = null;
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
