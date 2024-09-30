package ai.dataprep.accio.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.*;

public class UdfRTrim extends SqlFunction {

    public static final UdfRTrim INSTANCE = new UdfRTrim();

    public UdfRTrim() {
        super("RTRIM",
                SqlKind.TRIM,
                ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
                        .andThen(SqlTypeTransforms.TO_VARYING),
                null,
                OperandTypes.STRING,
                SqlFunctionCategory.STRING);
    }

    //~ Methods ----------------------------------------------------------------


}
