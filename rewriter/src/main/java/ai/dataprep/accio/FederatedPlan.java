package ai.dataprep.accio;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class FederatedPlan {
    public List<DBExecutionInfo> plan = new ArrayList<>();
    private HashMap<String, Integer> counter = new HashMap<>();

    public String add(String dbName, String sql) {
        return add(dbName, sql, 0);
    }

    public String add(String dbName, String sql, long cardinality) {
        Integer count = counter.getOrDefault(dbName, 0);
        String aliasDbName = dbName;
        if (count > 0) {
            aliasDbName = dbName + "_" + count;
        }
        DBExecutionInfo info = new DBExecutionInfo(dbName, aliasDbName, sql, cardinality);
        plan.add(info);
        counter.put(info.dbName, count+1);
        return aliasDbName;
    }

    public int getCount() {
        return plan.size();
    }

    public String getDBName(int idx) {
        return plan.get(idx).dbName;
    }

    public String getAliasDBName(int idx) {
        return plan.get(idx).aliasDbName;
    }

    public String getSql(int idx) {
        return plan.get(idx).sql;
    }

    public long getCardinality(int idx) {
        return plan.get(idx).cardinality;
    }

    @Override
    public String toString() {
        return String.join("\n\n", plan.stream().map(DBExecutionInfo::toString).collect(Collectors.toList()));
    }

    public static class DBExecutionInfo {
        public String dbName;
        public String aliasDbName;
        public String sql;
        public long cardinality;

        public DBExecutionInfo(String dbName, String aliasDbName, String sql, long cardinality) {
            this.dbName = dbName;
            this.aliasDbName = aliasDbName;
            this.sql = sql;
            this.cardinality = cardinality;
        }

        @Override
        public String toString() {
            return dbName + "[" + aliasDbName + "][CE=" + cardinality + "]: " + sql + ";";
        }
    }
}
