package io.activej.dataflow.jdbc.driver;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

public class DataflowJdbc41Factory implements AvaticaFactory {
	@Override
	public int getJdbcMajorVersion() {
		return 4;
	}

	@Override
	public int getJdbcMinorVersion() {
		return 1;
	}

	@Override
	public AvaticaConnection newConnection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) {
		return new DataflowJdbc41Connection(driver, factory, url, info);
	}

	@Override
	public AvaticaStatement newStatement(AvaticaConnection connection, StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
		return new DataflowJdbc41Statement(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection, StatementHandle h, Meta.Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return new DataflowJdbc41PreparedStatement(connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public AvaticaResultSet newResultSet(AvaticaStatement statement, QueryState state, Meta.Signature signature, TimeZone timeZone, Meta.Frame firstFrame) throws SQLException {
		ResultSetMetaData resultSetMetaData = newResultSetMetaData(statement, signature);
		return new DataflowResultSet(statement, signature, resultSetMetaData, timeZone, firstFrame);
	}

	@Override
	public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
		return new DataflowJdbc41DatabaseMetaData(connection);
	}

	@Override
	public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement, Meta.Signature signature) {
		return new AvaticaResultSetMetaData(statement, null, signature);
	}

	private static final class DataflowJdbc41Connection extends AvaticaConnection {
		private DataflowJdbc41Connection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) {
			super(driver, factory, url, info);
		}
	}

	private static class DataflowJdbc41DatabaseMetaData extends AvaticaDatabaseMetaData {
		private DataflowJdbc41DatabaseMetaData(AvaticaConnection connection) {
			super(connection);
		}
	}

	private static class DataflowJdbc41Statement extends AvaticaStatement {
		private DataflowJdbc41Statement(AvaticaConnection connection,
				StatementHandle h, int resultSetType, int resultSetConcurrency,
				int resultSetHoldability) {
			super(connection, h, resultSetType, resultSetConcurrency,
					resultSetHoldability);
		}
	}

	private static class DataflowJdbc41PreparedStatement extends AvaticaPreparedStatement {
		DataflowJdbc41PreparedStatement(AvaticaConnection connection,
				@Nullable StatementHandle h, Meta.Signature signature,
				int resultSetType, int resultSetConcurrency, int resultSetHoldability)
				throws SQLException {
			super(connection, h, signature, resultSetType, resultSetConcurrency,
					resultSetHoldability);
		}
	}
}
