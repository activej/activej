package io.activej.dataflow.calcite.jdbc.server;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

import java.util.List;
import java.util.Map;

abstract class LimitedMeta implements Meta {
	@Override
	public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getCatalogs(ConnectionHandle ch) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getTableTypes(ConnectionHandle ch) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getProcedures(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getProcedureColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getColumnPrivileges(ConnectionHandle ch, String catalog, String schema, String table, Pat columnNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getTablePrivileges(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getBestRowIdentifier(ConnectionHandle ch, String catalog, String schema, String table, int scope, boolean nullable) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getVersionColumns(ConnectionHandle ch, String catalog, String schema, String table) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getPrimaryKeys(ConnectionHandle ch, String catalog, String schema, String table) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getImportedKeys(ConnectionHandle ch, String catalog, String schema, String table) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getExportedKeys(ConnectionHandle ch, String catalog, String schema, String table) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getCrossReference(ConnectionHandle ch, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getTypeInfo(ConnectionHandle ch) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getIndexInfo(ConnectionHandle ch, String catalog, String schema, String table, boolean unique, boolean approximate) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getUDTs(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getSuperTypes(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getSuperTables(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getAttributes(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getClientInfoProperties(ConnectionHandle ch) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getFunctions(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getFunctionColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaResultSet getPseudoColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Object> createIterable(StatementHandle stmt, QueryState state, Signature signature, List<TypedValue> parameters, Frame firstFrame) {
		throw new UnsupportedOperationException();
	}

	@Override
	public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, PrepareCallback callback) throws NoSuchStatementException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h, List<String> sqlCommands) throws NoSuchStatementException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ExecuteBatchResult executeBatch(StatementHandle h, List<List<TypedValue>> parameterValues) throws NoSuchStatementException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, long maxRowCount) throws NoSuchStatementException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
		throw new UnsupportedOperationException();
	}

	@Override
	public StatementHandle createStatement(ConnectionHandle ch) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void closeStatement(StatementHandle h) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void openConnection(ConnectionHandle ch, Map<String, String> info) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void closeConnection(ConnectionHandle ch) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean syncResults(StatementHandle sh, QueryState state, long offset) throws NoSuchStatementException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void commit(ConnectionHandle ch) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void rollback(ConnectionHandle ch) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
		throw new UnsupportedOperationException();
	}
}
