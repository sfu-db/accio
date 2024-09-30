package ai.dataprep.accio.stats;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedTable;
import ai.dataprep.accio.plan.FedTableScan;
import ai.dataprep.accio.sql.FedRelSqlImplementor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class PostgresCardinalityCollector extends BaseCardinalityEstimator {
    HashMap<Integer, Double> resultCache;

    public PostgresCardinalityCollector() {
        resultCache = new HashMap<>();
    }

    @Override
    public @Nullable Double getRowCount(FedTableScan rel, RelMetadataQuery mq) {
        DataSource dataSource = ((FedConvention)rel.getConvention()).dataSource;
        FedTable table = rel.fedTable;
        if (dataSource == null) {
            return super.getRowCount(rel, mq);
        }

        String sql;
        sql = "SELECT COUNT(*) FROM " + table.tableName;
        int hash = sql.hashCode();
        if (resultCache.containsKey(hash)) {
            return resultCache.get(hash);
        }

        try {
            Connection connection = dataSource.getConnection();
            ResultSet rs = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
            rs.next();
            Double res = rs.getDouble(1);
            connection.close();
            resultCache.put(hash, res);
            return res;
        } catch (SQLException e) {
            e.printStackTrace();
            return super.getRowCount(rel, mq);
        }
    }

    public Optional<Double> getRowCountViaExplain(FedConvention convention, RelNode rel) {
        DataSource dataSource = convention.dataSource;
        if (dataSource == null) {
            return Optional.empty();
        }
        String sql = "SELECT COUNT(*) FROM (" + FedRelSqlImplementor.ConvertToRemoteSql2(convention, rel) + ") tmp";
        int hash = sql.hashCode();
        if (resultCache.containsKey(hash)) {
            return Optional.of(resultCache.get(hash));
        }

        try {
            Connection connection = dataSource.getConnection();
            ResultSet rs = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
            rs.next();
            Double res = rs.getDouble(1);
            connection.close();
            resultCache.put(hash, res);
            return Optional.of(res);
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

    @Override
    public @Nullable Double getRowCount(FedConvention convention, Aggregate rel, RelMetadataQuery mq) {
        return getRowCountViaExplain(convention, rel).orElse(super.getRowCount(convention, rel, mq));
    }

    @Override
    public @Nullable Double getRowCountDirectly(FedConvention convention, RelNode rel){
        return getRowCountViaExplain(convention, rel).orElse(null);
    }

    @Override
    public @Nullable Double getDomainSizeDirectly(FedConvention convention, RelNode rel, String column) {
        DataSource dataSource = convention.dataSource;
        if (dataSource == null) {
            return null;
        }
        String sql = "SELECT COUNT(*) FROM ( SELECT DISTINCT(\"" + column + "\") FROM (" + FedRelSqlImplementor.ConvertToRemoteSql2(convention, rel) + ") tmp ) tmp2";
        int hash = sql.hashCode();
        if (resultCache.containsKey(hash)) {
            return resultCache.get(hash);
        }

        try {
            Connection connection = dataSource.getConnection();
            ResultSet rs = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
            rs.next();
            Double res = rs.getDouble(1);
            connection.close();
            resultCache.put(hash, res);
            return res;
        }catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<@Nullable Double> getRowCountsDirectlyByBatch(FedConvention convention, List<RelNode> rels) {
        DataSource dataSource = convention.dataSource;
        if (dataSource == null) {
            return null;
        }

        List<@Nullable Double> rowCounts = new ArrayList<>();
        List<String> sqls = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();
        int[] hashCodes = new int[rels.size()];
        for (int i = 0; i < rels.size(); ++i) {
            String sql = "SELECT COUNT(*) FROM (" + FedRelSqlImplementor.ConvertToRemoteSql2(convention, rels.get(i)) + ") tmp";
            int hash = sql.hashCode();
            if (!resultCache.containsKey(hash)) {
                sqls.add(sql);
                indexes.add(i);
                rowCounts.add(null);
                hashCodes[i] = hash;
            } else {
                rowCounts.add(resultCache.get(hash));
            }
        }

        if (sqls.isEmpty()) {
            return rowCounts;
        }

        try {
            Connection connection = dataSource.getConnection();
            Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            boolean hasMoreResultSets = stmt.execute(String.join(";", sqls));
            int idx = 0;
            while (hasMoreResultSets) {
                ResultSet rs = stmt.getResultSet();
                rs.next();
                Double rowCount = rs.getDouble(1);
                resultCache.put(hashCodes[indexes.get(idx)], rowCount);
                rowCounts.set(indexes.get(idx++), rowCount);
                hasMoreResultSets = stmt.getMoreResults();
            }
            assert idx == rels.size();
            connection.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return rowCounts;
    }
}
