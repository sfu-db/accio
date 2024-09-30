package ai.dataprep.accio.stats;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTable;
import ai.dataprep.accio.plan.FedTableScan;
import ai.dataprep.accio.sql.FedRelSqlImplementor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class MysqlCardinalityEstimator extends BaseCardinalityEstimator {
    @Override
    public boolean supportOpInSubPlan(RelNode rel) {
        // only support estimation on SPJ queries
        return (rel instanceof Project) || (rel instanceof Filter) || (rel instanceof Join);
    }

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
            sql = "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"
                    + table.catalogName + "' AND TABLE_NAME = '" + table.tableName + "'";
            ResultSet rs = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
            rs.next();
            Double res = rs.getDouble(1);
            connection.close();
            return res;
        } catch (SQLException e) {
            e.printStackTrace();
            return super.getRowCount(rel, mq);
        }
    }

    // TODO: found issue in explain result: cannot use histograms when query is nested
    public Optional<Double> getRowCountViaExplain(FedConvention convention, RelNode rel) {
        DataSource dataSource = convention.dataSource;
        if (dataSource == null) {
            return Optional.empty();
        }
        String sql = FedRelSqlImplementor.ConvertToRemoteSql2(convention, rel);

        try {
            Connection connection = dataSource.getConnection();
            ResultSet rs = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery("EXPLAIN FORMAT=TRADITIONAL " + sql);
            Double rowCount = 1.0;
            while (rs.next()) {
                rowCount *= rs.getDouble("rows") * 0.01 * rs.getDouble("filtered");
            }
            connection.close();
            return Optional.of(rowCount);
        }catch (SQLException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    @Override
    public @Nullable Double getRowCount(FedConvention convention, Join rel, RelMetadataQuery mq) {
        return getRowCountViaExplain(convention, rel).orElse(super.getRowCount(convention, rel, mq));
    }

    @Override
    public @Nullable Double getRowCount(FedConvention convention, Filter rel, RelMetadataQuery mq) {
        return getRowCountViaExplain(convention, rel).orElse(super.getRowCount(convention, rel, mq));
    }
}
