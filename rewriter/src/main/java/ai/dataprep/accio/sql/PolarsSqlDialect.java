package ai.dataprep.accio.sql;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;

import static java.util.Objects.requireNonNull;

public class PolarsSqlDialect extends PostgresqlSqlDialect {
    /**
     * Creates a PostgresqlSqlDialect.
     *
     * @param context
     */
    public PolarsSqlDialect(Context context) {
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
            case SELECT: {
                SqlSelect select = (SqlSelect) call;
                HashMap<SqlNode, SqlNode> aliasMap = new HashMap<>();
                for (int i = 0; i < select.getSelectList().size(); ++i) {
                    SqlNode node = select.getSelectList().get(i);
                    if (node.getKind() == SqlKind.AS) {
                        SqlBasicCall alias = (SqlBasicCall) node;
                        aliasMap.put(alias.operand(0), alias.operand(1));
                    }
                }
                // HACK to solve: order by using the alias name
                if (select.hasOrderBy()) {
                    SqlNodeList list = select.getOrderList();
                    for (int i = 0; i < list.size(); ++i) {
                        SqlNode node = list.get(i);
                        if (node.getKind() == SqlKind.DESCENDING) {
                            SqlNode node1 = ((SqlBasicCall) node).operand(0);
                            if (aliasMap.containsKey(node1)) {
                                ((SqlBasicCall) node).setOperand(0, aliasMap.get(node1));
                            }
                        }
                        else if (aliasMap.containsKey(node)) {
                            list.set(i, aliasMap.get(node));
                        }
                    }
                    select.setOrderBy(list);
                }
                super.unparseCall(writer, call, leftPrec, rightPrec);
                break;
            }
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
