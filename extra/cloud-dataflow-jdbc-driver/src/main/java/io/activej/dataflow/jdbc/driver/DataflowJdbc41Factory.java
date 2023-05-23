package io.activej.dataflow.jdbc.driver;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.remote.CommonsHttpClientPoolCache;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.util.Timeout;
import org.jetbrains.annotations.Nullable;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

@SuppressWarnings("unused") // Used through reflection
public class DataflowJdbc41Factory implements AvaticaFactory {
	public static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");
	public static final Calendar UTC_CALENDAR = Calendar.getInstance(UTC_TIME_ZONE);

	private static final String SOCKET_TIMEOUT_PARAM = "socketTimeout=";
	private static final String DATAFLOW_HTTP_SOCKET_TIMEOUT = "dataflow.http.socketTimeout";

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
		String urlPart = url.substring(Driver.CONNECT_STRING_PREFIX.length());
		if (info.put(BuiltInConnectionProperty.URL.name(), urlPart) != null) {
			throw new IllegalArgumentException("URL should be set using connect string, not properties");
		}
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

	public static final class DataflowJdbc41Connection extends AvaticaConnection {
		private DataflowJdbc41Connection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) {
			super(driver, factory, url, info);
			initSocketTimeout(getSocketTimeout(config().url(), info));
		}

		private void initSocketTimeout(@Nullable Timeout timeout) {
			if (timeout == null) return;
			PoolingHttpClientConnectionManager pool = CommonsHttpClientPoolCache.getPool(config());
			DataflowJdbc41Factory.setSocketTimeout(pool, timeout);
		}

		@Override
		public TimeZone getTimeZone() {
			return UTC_TIME_ZONE;
		}
	}

	public static class DataflowJdbc41DatabaseMetaData extends AvaticaDatabaseMetaData {
		private DataflowJdbc41DatabaseMetaData(AvaticaConnection connection) {
			super(connection);
		}
	}

	public static class DataflowJdbc41Statement extends AvaticaStatement {
		private DataflowJdbc41Statement(
			AvaticaConnection connection, StatementHandle h, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability
		) {
			super(connection, h, resultSetType, resultSetConcurrency,
				resultSetHoldability);
		}
	}

	public static class DataflowJdbc41PreparedStatement extends AvaticaPreparedStatement {
		DataflowJdbc41PreparedStatement(AvaticaConnection connection,
			@Nullable StatementHandle h, Meta.Signature signature,
			int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
			super(connection, h, signature, resultSetType, resultSetConcurrency,
				resultSetHoldability);
		}

		@Override
		protected Calendar getCalendar() {
			return UTC_CALENDAR;
		}
	}

	private static @Nullable Timeout getSocketTimeout(String url, Properties info) {
		Timeout timeout = parseSocketTimeout(url);
		if (timeout != null) {
			return timeout;
		}
		Object timeoutProperty = info.get(CustomConnectionProperty.SOCKET_TIMEOUT.name());
		if (timeoutProperty != null && !timeoutProperty.toString().isEmpty()) {
			return Timeout.ofMilliseconds(CustomConnectionProperty.SOCKET_TIMEOUT.wrap(info).getLong());
		} else {
			String property = System.getProperty(DATAFLOW_HTTP_SOCKET_TIMEOUT);
			if (property != null) {
				return Timeout.ofMilliseconds(Long.parseLong(property));
			}
		}
		return null;
	}

	private static @Nullable Timeout parseSocketTimeout(String url) {
		String query;
		try {
			query = new URL(url).getQuery();
		} catch (MalformedURLException ignored) {
			return null;
		}
		if (query == null) return null;
		for (String param : query.split("&")) {
			if (param.startsWith(SOCKET_TIMEOUT_PARAM)) {
				return Timeout.ofMilliseconds(Long.parseLong(param.substring(SOCKET_TIMEOUT_PARAM.length())));
			}
		}
		return null;
	}

	private static void setSocketTimeout(PoolingHttpClientConnectionManager pool, Timeout timeout) {
		if (timeout == null) return;
		SocketConfig config = pool.getDefaultSocketConfig();
		SocketConfig socketConfig = (config == null ? SocketConfig.custom() : SocketConfig.copy(config))
			.setSoTimeout(timeout)
			.build();
		pool.setDefaultSocketConfig(socketConfig);
	}
}
