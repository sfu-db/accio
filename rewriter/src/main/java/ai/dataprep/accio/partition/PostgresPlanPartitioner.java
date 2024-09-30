package ai.dataprep.accio.partition;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresPlanPartitioner extends SingleTablePlanPartitioner{
    static final String PART_COL = "ctid";

    @Override
    public List<String> getPartitionFilters(FedConvention convention, PartitionScheme partitionScheme) {
        SingleTablePlanPartitioner.SingleTablePartitionScheme scheme = (SingleTablePlanPartitioner.SingleTablePartitionScheme)partitionScheme;

        DataSource dataSource = convention.dataSource;
        assert dataSource != null && scheme.getPartitionNum() > 1;

        // collect # pages
        int maxPage = -1;
        try {
            Connection connection = dataSource.getConnection();
            String sql = "SELECT GREATEST(relpages, 1) FROM pg_class WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '"
                    + scheme.scan.fedTable.schemaName + "') AND relname = '" + scheme.scan.fedTable.tableName + "'";
            ResultSet rs = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
            rs.next();
            maxPage = rs.getInt(1);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        // partition based on `CTID`
        assert maxPage > 0;
        List<String> filters = new ArrayList<>();

        int step = maxPage / scheme.getPartitionNum();
        int lrange = 0, rrange = step;

        filters.add(PART_COL + " <= '(" + rrange + ", 0)'::tid");
        lrange = rrange;
        for (int i = 0; i < scheme.getPartitionNum()-2; ++i) {
            rrange += step;
            filters.add(PART_COL + " BETWEEN '(" + lrange + ", 0)'::tid AND '(" + rrange + ",0)'::tid");
            lrange = rrange;
        }
        filters.add(PART_COL + ">= '(" + lrange + ", 0)'::tid");

        return filters;
    }

    @Override
    public @Nullable String getPartitionColumn(RelNode node, FedTableScan tableScan) {
        if (OUTPUT_FORMAT.equals("SPARK_INFO")){
            RelMetadataQuery mq = node.getCluster().getMetadataQuery();
            for (int i = 0; i < node.getRowType().getFieldCount(); ++i) {
                RelColumnOrigin col = mq.getColumnOrigin(node, i);
                if (col.isDerived()) {
                    continue; // do not partition on derived column
                }
                RelDataTypeField field = tableScan.getRowType().getFieldList().get(col.getOriginColumnOrdinal());
                switch (field.getType().getSqlTypeName().getFamily()) {
                    case NUMERIC:
                    case DATE:
                    case TIME:
                    case TIMESTAMP:
                        return node.getRowType().getFieldNames().get(i);
                    default:
                        return null;
                }
            }
        }

        return PART_COL;
    }

    @Override
    public JSONObject getPartitionInfo(FedConvention convention, PartitionScheme partitionScheme) {
        SingleTablePlanPartitioner.SingleTablePartitionScheme scheme = (SingleTablePlanPartitioner.SingleTablePartitionScheme)partitionScheme;

        DataSource dataSource = convention.dataSource;
        assert dataSource != null && scheme.getPartitionNum() > 1;

        // collect # pages
        String minVal = "";
        String maxVal = "";
        try {
            Connection connection = dataSource.getConnection();
            String sql = "SELECT histogram_bounds FROM pg_stats WHERE schemaname = '"
                    + scheme.scan.fedTable.schemaName + "' AND tablename = '" + scheme.scan.fedTable.tableName + "' AND attname = '" + scheme.column + "'";
            ResultSet rs = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
            rs.next();
            String a = rs.getArray(1).toString();
            String[] vals = a.substring(1, a.length()-1).split(",");
            minVal = vals[0];
            maxVal = vals[vals.length-1];
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        JSONObject partInfo = new JSONObject();
        partInfo.put("lowerBound", minVal);
        partInfo.put("upperBound", maxVal);
        return partInfo;
    }
}
