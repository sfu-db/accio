package ai.dataprep.accio.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

public class MyPostgresqlSqlDialect extends PostgresqlSqlDialect {
    /**
     * Creates a PostgresqlSqlDialect.
     *
     * @param context
     */
    public MyPostgresqlSqlDialect(Context context) {
        super(context);
    }

    @Override public void unparseCall(SqlWriter writer, SqlCall call,
                                      int leftPrec, int rightPrec) {
        switch (call.getKind()) {
            case IS_NOT_TRUE: {
                // XXX: this is a temporary hack
                // our `RexSimplify.simplifyNot` will convert `NOT(IS TRUE($1)` to `IS NOT TURE($1)`
                // which is not correct in postgres
                // need to convert to "col <> TRUE OR col IS NULL"
                SqlNode operand = call.getOperandList().get(0);
                writer.print("( ");
                operand.unparse(writer, leftPrec, rightPrec);
                writer.print("<> TRUE OR ");
                operand.unparse(writer, leftPrec, rightPrec);
                writer.print("IS NULL ) ");
                break;
            }
            case IS_NOT_FALSE: {
                SqlNode operand = call.getOperandList().get(0);
                writer.print("( ");
                operand.unparse(writer, leftPrec, rightPrec);
                writer.print("<> FALSE OR ");
                operand.unparse(writer, leftPrec, rightPrec);
                writer.print("IS NULL ) ");
                break;
            }
//            case MIN:
//                // exists/in queries will be converted to select key, min(true) ... group by key with join
//                // postgres does not support min(true), need to remove the "min()"
//                SqlNode operand = call.getOperandList().get(0);
//                if (operand.toString().equals("1")) {
//                    writer.print("(");
//                    super.unparseCall(writer, call, leftPrec, rightPrec);
//                    writer.print(" = 1)");
//                    break;
//                }
            default:
                super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

    @Override public boolean supportsAggregateFunction(SqlKind kind) {
        switch (kind) {
            case COUNT:
            case SUM:
            case SUM0:
            case MIN:
            case MAX:
            case AVG:
            case SINGLE_VALUE:
            case LITERAL_AGG:
                return true;
            default:
                break;
        }
        return false;
    }
}
