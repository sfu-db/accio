package ai.dataprep.accio.sql;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.util.Pair;

public class MyClickHouseSqlDialect extends ClickHouseSqlDialect {
    /**
     * Creates a ClickHouseSqlDialect.
     *
     * @param context
     */
    public MyClickHouseSqlDialect(Context context) {
        super(context);
    }

    @Override public boolean supportsAggregateFunction(SqlKind kind) {
        switch (kind) {
            case COUNT:
            case SUM:
            case SUM0:
            case MIN:
            case MAX:
            case AVG:
                return true;
            default:
                break;
        }
        return false;
    }

    @Override public void unparseCall(SqlWriter writer, SqlCall call,
                                      int leftPrec, int rightPrec) {
        switch (call.getKind()) {
            case IS_NOT_TRUE: {
                // clickhouse does not support "col IS NOT TRUE" syntax
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
            case CASE: { // Clickhouse cannot take `Int` as condition for `WHEN/IF`, has to convert it to `UInt8`
                SqlCase kase = (SqlCase) call;
                final SqlWriter.Frame frame =
                        writer.startList(SqlWriter.FrameTypeEnum.CASE, "CASE", "END");
                assert kase.getWhenOperands().size() == kase.getThenOperands().size();
                if (kase.getValueOperand() != null) {
                    kase.getValueOperand().unparse(writer, 0, 0);
                }
                for (Pair<SqlNode, SqlNode> pair : Pair.zip(kase.getWhenOperands(), kase.getThenOperands())) {
                    writer.sep("WHEN");
                    writer.print("toUInt8(");
                    pair.left.unparse(writer, 0, 0);
                    writer.print(")");
                    writer.sep("THEN");
                    pair.right.unparse(writer, 0, 0);
                }

                SqlNode elseExpr = kase.getElseOperand();
                if (elseExpr != null) {
                    writer.sep("ELSE");
                    elseExpr.unparse(writer, 0, 0);
                }
                writer.endList(frame);
                break;
            }
            case CAST: {
                // HACK: CAST(1 = var AS BOOLEAN) may trigger error: "Cannot convert NULL value to non-Nullable type" in clickhouse
                if (call.getOperandList().get(0).toString().startsWith("1 = ")) {
                    writer.print("(");
                    super.unparseCall(writer, (SqlCall) call.getOperandList().get(0), leftPrec, rightPrec);
                    writer.print(")");
                    break;
                }
            }
            default:
                super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
}
