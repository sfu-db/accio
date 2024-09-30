package ai.dataprep.accio.stats;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTable;
import ai.dataprep.accio.plan.FedTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DuckdbCardinalityEstimator extends BaseCardinalityEstimator {
    @Override
    public @Nullable Double getRowCount(FedTableScan rel, RelMetadataQuery mq) {
        DataSource dataSource = ((FedConvention)rel.getConvention()).dataSource;
        FedTable table = rel.fedTable;

        if (dataSource == null) {
            return super.getRowCount(rel, mq);
        }

        try {
            Connection connection = dataSource.getConnection();
            String sql;
            sql = "SELECT estimated_size FROM duckdb_tables WHERE schema_name = '"
                    + table.schemaName + "' AND table_name = '" + table.tableName + "'";
            ResultSet rs = connection.createStatement().executeQuery(sql);
            rs.next();
            Double res = rs.getDouble(1);
            connection.close();
            return res;
        } catch (SQLException e) {
            e.printStackTrace();
            return super.getRowCount(rel, mq);
        }
    }
}
