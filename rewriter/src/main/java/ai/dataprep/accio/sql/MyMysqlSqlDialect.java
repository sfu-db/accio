package ai.dataprep.accio.sql;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

public class MyMysqlSqlDialect extends MysqlSqlDialect {
    private final int majorVersion;

    /**
     * Creates a MysqlSqlDialect.
     *
     * @param context
     */
    public MyMysqlSqlDialect(Context context) {
        super(context);
        majorVersion = context.databaseMajorVersion();
    }

    @Override public boolean supportsAggregateFunction(SqlKind kind) {
        switch (kind) {
            case AVG:
            case COUNT:
            case SUM:
            case SUM0:
            case MIN:
            case MAX:
            case SINGLE_VALUE:
            case LITERAL_AGG:
                return true;
            case ROLLUP:
                // MySQL 5 does not support standard "GROUP BY ROLLUP(x, y)",
                // only the non-standard "GROUP BY x, y WITH ROLLUP".
                return majorVersion >= 8;
            default:
                break;
        }
        return false;
    }
}
