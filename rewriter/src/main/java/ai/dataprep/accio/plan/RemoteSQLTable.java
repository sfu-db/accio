package ai.dataprep.accio.plan;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class RemoteSQLTable implements RelOptTable {
    List<String> name;
    RelDataType rowType;

    public RemoteSQLTable(List<String> name, RelDataType rowType) {
        this.name = name;
        this.rowType = rowType;
    }

    @Override
    public List<String> getQualifiedName() {
        return name;
    }

    @Override
    public double getRowCount() {
        return 0;
    }

    @Override
    public RelDataType getRowType() {
        return rowType;
    }

    @Override
    public @Nullable RelOptSchema getRelOptSchema() {
        return null;
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        return null;
    }

    @Override
    public @Nullable List<RelCollation> getCollationList() {
        return null;
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        return null;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        return null;
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        return null;
    }

    @Override
    public @Nullable Expression getExpression(Class clazz) {
        return null;
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        return null;
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        return null;
    }

    @Override
    public <C> @Nullable C unwrap(Class<C> aClass) {
        return null;
    }
}
