package ai.dataprep.accio.plan;

import com.google.common.base.Suppliers;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class FedTable extends AbstractTable implements ScannableTable, TranslatableTable {
    private final Supplier<RelProtoDataType> protoRowTypeSupplier =
            Suppliers.memoize(this::supplyProto)::get;
    private RelDataType relDataType = null;
    private List<String> colNames = null;

    public final FedSchema fedSchema;
    public final String catalogName;
    public final String schemaName;
    public final String tableName;
    public final Schema.TableType tableType;

    // Stats
    private Double rowCount;

    FedTable(FedSchema fedSchema, String catalogName,
             String schemaName, String tableName,
             Schema.TableType tableType) {
        this.fedSchema = requireNonNull(fedSchema, "fedSchema");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName");
        this.tableType = requireNonNull(tableType, "tableType");
    }

    FedTable(FedSchema fedSchema, String catalogName,
             String schemaName, String tableName,
             Schema.TableType tableType, List<String> colNames) {
        this.fedSchema = requireNonNull(fedSchema, "fedSchema");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName");
        this.tableType = requireNonNull(tableType, "tableType");
        this.colNames = colNames;
    }

    public String getTableName() {
        return String.join(".", this.catalogName, this.schemaName, this.tableName);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (relDataType != null) {
            return relDataType;
        }
        if (colNames != null) {
            final List<RelDataType> types = new ArrayList<>();
            final List<String> names = new ArrayList<>();
            for (String colName : this.colNames) {
                names.add(colName);
                // Use ANY (Nullable) as all columns, reduce information needed
                // Validation of the related part relies on local executor
                // TODO: support real types as input (currently it may throw error on converting type ANY)
                types.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true));
            }
            relDataType = typeFactory.createStructType(Pair.zip(names, types));
        } else {
            relDataType =  protoRowTypeSupplier.get().apply(typeFactory);
        }
        return relDataType;
    }

    private RelProtoDataType supplyProto() {
        try {
            return fedSchema.getRelDataType(
                    catalogName,
                    schemaName,
                    tableName);
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Exception while reading definition of table '" + tableName
                            + "'", e);
        }
    }

    /** Returns the table name, qualified with catalog and schema name if
     * applicable, as a parse tree node ({@link SqlIdentifier}). */
    public SqlIdentifier tableName() {
        final List<String> names = new ArrayList<>(3);
        if (catalogName != null) {
            names.add(catalogName);
        }
        if (schemaName != null) {
            names.add(schemaName);
        }
        names.add(tableName);
        return new SqlIdentifier(names, SqlParserPos.ZERO);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new FedTableScan(context.getCluster(), context.getTableHints(), relOptTable, this,
                fedSchema.fedConvention);
    }

    public Double getRowCount() {
        return rowCount;
    }

    public void setRowCount(Double rowCount) {
        this.rowCount = rowCount;
    }

}

//public class ColumnStatistics {
//    public Optional<Double> numDistinct;
//    public Optional<Double> nullFraction;
//    public Optional<Double> avgWidth;
//    public Optional<Double> minValue; // not accurate (from historgram bound)
//    public Optional<Double> maxValue; // not accurate (from historgram bound)
//}
