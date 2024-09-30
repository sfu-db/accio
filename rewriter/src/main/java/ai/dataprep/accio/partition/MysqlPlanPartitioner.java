package ai.dataprep.accio.partition;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTable;
import ai.dataprep.accio.plan.FedTableScan;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONObject;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MysqlPlanPartitioner extends SingleTablePlanPartitioner {
    @Override
    public List<String> getPartitionFilters(FedConvention convention, PartitionScheme partitionScheme) {
        SingleTablePlanPartitioner.SingleTablePartitionScheme scheme = (SingleTablePlanPartitioner.SingleTablePartitionScheme)partitionScheme;

        DataSource dataSource = convention.dataSource;
        assert dataSource != null && scheme.getPartitionNum() > 1;

        int min_id, max_id = 0;
        try {
            Connection connection = dataSource.getConnection();
            String sql = "SELECT MIN(" + scheme.column + "), MAX(" + scheme.column + ") FROM " + scheme.scan.fedTable.tableName;
            ResultSet rs = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
            rs.next();
            min_id = rs.getInt(1);
            max_id = rs.getInt(2);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        List<String> filters = new ArrayList<>();

        int step = (max_id - min_id) / scheme.getPartitionNum();
        int lrange = 0, rrange = step;

        filters.add(scheme.column + " <= " + rrange);
        lrange = rrange;
        for (int i = 0; i < scheme.getPartitionNum()-2; ++i) {
            rrange += step;
            filters.add(scheme.column + " > " + lrange + " AND " + scheme.column + " <= " + rrange);
            lrange = rrange;
        }
        filters.add(scheme.column + " > " + lrange );

        return filters;
    }

    @Override
    public @Nullable String getPartitionColumn(RelNode node, FedTableScan tableScan) {
        DataSource dataSource = ((FedConvention)tableScan.getConvention()).dataSource;
        FedTable table = tableScan.fedTable;

        if (!OUTPUT_FORMAT.equals("LIST_OF_QUERIES")) {
            return null; // do not support other output format
        }

        if (dataSource == null) {
            return null;
        }

        try {
            Connection connection = dataSource.getConnection();
            // TODO: only partition on integer primary keys for now
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_KEY='PRI' AND COLUMN_TYPE LIKE '%int%' AND TABLE_SCHEMA = '"
                    + table.catalogName + "' AND TABLE_NAME = '" + table.tableName + "'";
            ResultSet rs = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery(sql);

            String column = null;
            if (rs.next()) {
                column = rs.getString(1);
            }
            connection.close();
            return column;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public JSONObject getPartitionInfo(FedConvention convention, PartitionScheme partitionScheme) {
        return null;
    }
}
