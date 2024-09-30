package ai.dataprep.accio.plan;

import ai.dataprep.accio.sql.MyPostgresqlSqlDialect;
import com.google.common.collect.*;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static java.util.Objects.requireNonNull;

public class FedSchema implements Schema {
    private @Nullable ImmutableMap<String, FedTable> tableMap;
    public @Nullable final JdbcSchema jdbcSchema;
    public final FedConvention fedConvention;
    final @Nullable String catalog;
    final @Nullable String schema;

    private static final Ordering<Iterable<Integer>> VERSION_ORDERING =
            Ordering.<Integer>natural().lexicographical();

    /**
     * Creates a JDBC schema.
     *
     * @param dataSource Data source
     * @param dialect    SQL dialect
     * @param convention Calling convention
     * @param catalog    Catalog name, or null
     * @param schema     Schema name pattern
     */
    public FedSchema(DataSource dataSource, SqlDialect dialect, FedConvention convention, @Nullable String catalog, @Nullable String schema) {
        this.jdbcSchema = new JdbcSchema(dataSource, dialect, new JdbcConvention(convention.dialect, null, convention.getName()), catalog, schema);
        this.fedConvention = convention;
        this.catalog = catalog;
        this.schema = schema;
    }

    public FedSchema(FedConvention convention, HashMap<String, List<String>> manualSchema, @Nullable String catalog, @Nullable String schema) {
        this.jdbcSchema = null;
        this.fedConvention = convention;
        this.catalog = catalog;
        this.schema = schema;

        final ImmutableMap.Builder<String, FedTable> builder = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry: manualSchema.entrySet()) {
            String tableName = entry.getKey();
            final FedTable table = new FedTable(this, this.catalog, this.schema, tableName, TableType.TABLE, entry.getValue());
            builder.put(tableName, table);
        }
        this.tableMap = builder.build();
    }

    public FedSchema(FedConvention convention, JdbcSchema jdbcSchema, ImmutableMap<String, FedTable> tableMap, @Nullable String catalog, @Nullable String schema) {
        this.fedConvention = convention;
        this.jdbcSchema = jdbcSchema;
        this.catalog = catalog;
        this.schema = schema;
        this.tableMap = tableMap;
    }

    public DataSource getDataSource() { return jdbcSchema == null? null : jdbcSchema.getDataSource(); }

    public static SqlDialect createDialect(DataSource dataSource) {
        if (dataSource == null) {
            return PostgresqlSqlDialect.DEFAULT;
        }
        SqlDialect dialect = JdbcSchema.createDialect(SqlDialectFactoryImpl.INSTANCE, dataSource);
        if (dialect instanceof PostgresqlSqlDialect) {
            dialect = new MyPostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT);
        }
        return dialect;
    }

    @Override
    public @Nullable Table getTable(String name) {
        return getTableMap().get(name);
    }

    private ImmutableMap<String, FedTable> getTableMap() {
        if (tableMap == null) {
            // compute tables in the first time
            tableMap = computeTables();
        }
        return tableMap;
    }

    @Override
    public Set<String> getTableNames() {
        return getTableMap().keySet();
    }



    private ImmutableMap<String, FedTable> computeTables() {
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = jdbcSchema.getDataSource().getConnection();
            final Pair<@Nullable String, @Nullable String> catalogSchema = getCatalogSchema(connection);
            final String catalog = catalogSchema.left;
            final String schema = catalogSchema.right;
            final Iterable<MetaImpl.MetaTable> tableDefs;

            final List<MetaImpl.MetaTable> tableDefList = new ArrayList<>();
            final DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getTables(catalog, schema, null, null);
            while (resultSet.next()) {
                final String catalogName = resultSet.getString(1);
                final String schemaName = resultSet.getString(2);
                final String tableName = resultSet.getString(3);
                final String tableTypeName = resultSet.getString(4);
                tableDefList.add(
                        new MetaImpl.MetaTable(catalogName, schemaName, tableName,
                                tableTypeName));
            }
            tableDefs = tableDefList;

            final ImmutableMap.Builder<String, FedTable> builder =
                    ImmutableMap.builder();
            for (MetaImpl.MetaTable tableDef : tableDefs) {
                // skip tables that contain information
                // a bit hard code for postgres style
                // TODO: find a more db-agnostic way
                if (tableDef.tableSchem != null && (tableDef.tableSchem.equalsIgnoreCase("system") || tableDef.tableSchem.equalsIgnoreCase("information_schema")))
                    continue;

                // Clean up table type. In particular, this ensures that 'SYSTEM TABLE',
                // returned by Phoenix among others, maps to TableType.SYSTEM_TABLE.
                // We know enum constants are upper-case without spaces, so we can't
                // make things worse.
                //
                // PostgreSQL returns tableTypeName==null for pg_toast* tables
                // This can happen if you start JdbcSchema off a "public" PG schema
                // The tables are not designed to be queried by users, however we do
                // not filter them as we keep all the other table types.
                String tableTypeName2 =
                        tableDef.tableType == null
                                ? null
                                : tableDef.tableType.toUpperCase(Locale.ROOT).replace(' ', '_');
                // DuckDB returns `BASE_TABLE` instead of `TABLE`
                if (tableTypeName2.equals("BASE_TABLE")) {
                    tableTypeName2 = "TABLE";
                }
                final TableType tableType =
                        Util.enumVal(TableType.OTHER, tableTypeName2);
                if (tableType == TableType.OTHER  && tableTypeName2 != null) {
                    System.out.println("Unknown table type: " + tableTypeName2);
                }
                if (tableType == TableType.SYSTEM_TABLE || tableType == TableType.SYSTEM_VIEW)
                    continue;
                final FedTable table =
                        new FedTable(this, tableDef.tableCat, tableDef.tableSchem,
                                tableDef.tableName, tableType);
                builder.put(tableDef.tableName, table);
            }
            return builder.build();
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Exception while reading tables", e);
        } finally {
            close(connection, null, resultSet);
        }
    }

    /* ============ COPY FROM JdbcSchema ============ */
    /** Returns [major, minor] version from a database metadata. */
    private static List<Integer> version(DatabaseMetaData metaData) throws SQLException {
        return ImmutableList.of(metaData.getJDBCMajorVersion(),
                metaData.getJDBCMinorVersion());
    }

    /** Returns a pair of (catalog, schema) for the current connection. */
    private Pair<@Nullable String, @Nullable String> getCatalogSchema(Connection connection)
            throws SQLException {
        final DatabaseMetaData metaData = connection.getMetaData();
        final List<Integer> version41 = ImmutableList.of(4, 1); // JDBC 4.1
        String catalog = this.catalog;
        String schema = this.schema;
        final boolean jdbc41OrAbove =
                VERSION_ORDERING.compare(version(metaData), version41) >= 0;
        if (catalog == null && jdbc41OrAbove) {
            // From JDBC 4.1, catalog and schema can be retrieved from the connection
            // object, hence try to get it from there if it was not specified by user
            catalog = connection.getCatalog();
        }
        if (schema == null && jdbc41OrAbove) {
            schema = connection.getSchema();
            if ("".equals(schema)) {
                schema = null; // PostgreSQL returns useless "" sometimes
            }
        }
        if ((catalog == null || schema == null)
                && metaData.getDatabaseProductName().equals("PostgreSQL")) {
            final String sql = "select current_database(), current_schema()";
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                if (resultSet.next()) {
                    catalog = resultSet.getString(1);
                    schema = resultSet.getString(2);
                }
            }
        }
        return Pair.of(catalog, schema);
    }

    RelProtoDataType getRelDataType(String catalogName, String schemaName,
                                    String tableName) throws SQLException {
        Connection connection = null;
        try {
            connection = getDataSource().getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            return getRelDataType(metaData, catalogName, schemaName, tableName);
        } finally {
            close(connection, null, null);
        }
    }

    RelProtoDataType getRelDataType(DatabaseMetaData metaData, String catalogName,
                                    String schemaName, String tableName) throws SQLException {
        final ResultSet resultSet =
                metaData.getColumns(catalogName, schemaName, tableName, null);

        // Temporary type factory, just for the duration of this method. Allowable
        // because we're creating a proto-type, not a type; before being used, the
        // proto-type will be copied into a real type factory.
        final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        while (resultSet.next()) {
            final String columnName = requireNonNull(resultSet.getString(4), "columnName");
            int dataType = resultSet.getInt(5);
            // HACK: for mysql, we convert all `CHAR` to `VARCHAR`
            // since the result returned by rust mysql driver is trimed (which is used in connectorx)
            if (dataType == Types.CHAR && fedConvention.dialect instanceof MysqlSqlDialect) {
                dataType = Types.VARCHAR;
            }

            final String typeString = resultSet.getString(6);
            final int precision;
            final int scale;
            switch (SqlType.valueOf(dataType)) {
                case TIMESTAMP:
                case TIME:
                    precision = resultSet.getInt(9); // SCALE
                    scale = 0;
                    break;
                default:
                    precision = resultSet.getInt(7); // SIZE
                    scale = resultSet.getInt(9); // SCALE
                    break;
            }
            RelDataType sqlType =
                    sqlType(typeFactory, dataType, precision, scale, typeString);
            boolean nullable = resultSet.getInt(11) != DatabaseMetaData.columnNoNulls;
            fieldInfo.add(columnName, sqlType).nullable(nullable);
        }
        resultSet.close();
        return RelDataTypeImpl.proto(fieldInfo.build());
    }

    private static RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
                                       int precision, int scale, @Nullable String typeString) {
        // Fall back to ANY if type is unknown
        final SqlTypeName sqlTypeName =
                Util.first(SqlTypeName.getNameForJdbcType(dataType), SqlTypeName.ANY);
        switch (sqlTypeName) {
            case ARRAY:
                RelDataType component = null;
                if (typeString != null && typeString.endsWith(" ARRAY")) {
                    // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type
                    // "INTEGER".
                    final String remaining = typeString.substring(0,
                            typeString.length() - " ARRAY".length());
                    component = parseTypeString(typeFactory, remaining);
                }
                if (component == null) {
                    component = typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.ANY), true);
                }
                return typeFactory.createArrayType(component, -1);
            default:
                break;
        }
        if (precision >= 0
                && scale >= 0
                && sqlTypeName.allowsPrecScale(true, true)) {
            return typeFactory.createSqlType(sqlTypeName, precision, scale);
        } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
            return typeFactory.createSqlType(sqlTypeName, precision);
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType(sqlTypeName);
        }
    }

    /** Given "INTEGER", returns BasicSqlType(INTEGER).
     * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
     * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2). */
    private static RelDataType parseTypeString(RelDataTypeFactory typeFactory,
                                               String typeString) {
        int precision = -1;
        int scale = -1;
        int open = typeString.indexOf("(");
        if (open >= 0) {
            int close = typeString.indexOf(")", open);
            if (close >= 0) {
                String rest = typeString.substring(open + 1, close);
                typeString = typeString.substring(0, open);
                int comma = rest.indexOf(",");
                if (comma >= 0) {
                    precision = Integer.parseInt(rest.substring(0, comma));
                    scale = Integer.parseInt(rest.substring(comma));
                } else {
                    precision = Integer.parseInt(rest);
                }
            }
        }
        try {
            final SqlTypeName typeName = SqlTypeName.valueOf(typeString);
            return typeName.allowsPrecScale(true, true)
                    ? typeFactory.createSqlType(typeName, precision, scale)
                    : typeName.allowsPrecScale(true, false)
                    ? typeFactory.createSqlType(typeName, precision)
                    : typeFactory.createSqlType(typeName);
        } catch (IllegalArgumentException e) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.ANY), true);
        }
    }

    private static void close(
            @Nullable Connection connection,
            @Nullable Statement statement,
            @Nullable ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
    /* ============ COPY FROM JdbcSchema ============ */

    /* ============ DERIVE FROM JdbcSchema ============ */
    protected Map<String, RelProtoDataType> getTypes() {
        // TODO: populate map from JDBC metadata
        return ImmutableMap.of();
    }

    @Override
    public @Nullable RelProtoDataType getType(String name) {
        return jdbcSchema != null ? jdbcSchema.getType(name) : getTypes().get(name);
    }

    @Override
    public Set<String> getTypeNames() {
        return jdbcSchema != null ? jdbcSchema.getTypeNames() : getTypes().keySet();
    }

    protected Multimap<String, Function> getFunctions() {
        // TODO: populate map from JDBC metadata
        return ImmutableMultimap.of();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return jdbcSchema != null ? jdbcSchema.getFunctions(name) : getFunctions().get(name);
    }

    @Override
    public Set<String> getFunctionNames() {
        return jdbcSchema != null ? jdbcSchema.getFunctionNames() : getFunctions().keySet();
    }

    @Override
    public @Nullable Schema getSubSchema(String name) {
        return jdbcSchema != null ? jdbcSchema.getSubSchema(name) : null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return jdbcSchema != null ? jdbcSchema.getSubSchemaNames() : ImmutableSet.of();
    }

    @Override
    public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
        if (jdbcSchema != null) {
            jdbcSchema.getExpression(parentSchema, name);
        }
        requireNonNull(parentSchema, "parentSchema must not be null for JdbcSchema");
        return Schemas.subSchemaExpression(parentSchema, name, FedSchema.class);
    }

    @Override
    public boolean isMutable() {
        return jdbcSchema != null ? jdbcSchema.isMutable() : false;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return jdbcSchema != null ?
                new FedSchema(this.fedConvention, (JdbcSchema)jdbcSchema.snapshot(version), this.getTableMap(), this.catalog, this.schema)
                : new FedSchema(this.fedConvention, null, this.getTableMap(), this.catalog, this.schema);
    }
    /* ============ DERIVE FROM JdbcSchema ============ */
}
