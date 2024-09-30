package ai.dataprep.accio.sql;

import ai.dataprep.accio.plan.RemoteToLocalConverter;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.*;

public class DataFusionSqlImplementor extends RelToSqlConverter {
    /**
     * Creates a RelToSqlConverter.
     *
     * @param dialect
     */
    public DataFusionSqlImplementor(SqlDialect dialect) {
        super(dialect);
    }

    public Result visit(EnumerableLimit e) {
        final Result x = visitInput(e, 0);
        final Builder builder = x.builder(e);
        builder.setFetch(builder.context.toSql(null, e.fetch));
        return builder.result();
    }

    public Result visit(RemoteToLocalConverter e) {
        return dispatch(e.getInput());
    }

}
