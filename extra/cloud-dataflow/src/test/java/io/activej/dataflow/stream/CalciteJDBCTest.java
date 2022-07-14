package io.activej.dataflow.stream;

import io.activej.common.exception.FatalErrorHandler;
import io.activej.dataflow.calcite.CalciteSqlDataflow;
import io.activej.dataflow.calcite.inject.CalciteModule;
import io.activej.dataflow.calcite.jdbc.DataflowMeta;
import io.activej.dataflow.calcite.jdbc.Driver;
import io.activej.eventloop.Eventloop;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.junit.BeforeClass;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertTrue;

public class CalciteJDBCTest extends AbstractCalciteTest {

	private int port;
	private Eventloop serverEventloop;
	private HttpServer jdbcServer;

	@BeforeClass
	public static void beforeClass() throws Exception {
		Class.forName("io.activej.dataflow.calcite.jdbc.Driver");
	}

	@Override
	protected Module getAdditionalServerModule() {
		port = getFreePort();
		return ModuleBuilder.create()
				.install(new CalciteModule())
				.bind(HttpServer.class).to((eventloop, calciteSqlDataflow) -> new HttpServer.Builder<>()
								.withHandler(new AvaticaJsonHandler(new LocalService(new DataflowMeta(eventloop, calciteSqlDataflow))))
								.withPort(port)
								.build(),
						Eventloop.class, CalciteSqlDataflow.class)
				.bind(Eventloop.class).to(() -> Eventloop.create().withFatalErrorHandler(FatalErrorHandler.rethrow()))
				.build();
	}

	@Override
	protected void onSetUp() throws Exception {
		jdbcServer = serverInjector.getInstance(HttpServer.class);
		serverEventloop = serverInjector.getInstance(Eventloop.class);
		serverEventloop.keepAlive(true);

		jdbcServer.start();

		new Thread(serverEventloop).start();

		serverEventloop.submit(() -> server.listen()).get();
	}

	@Override
	protected void onTearDown() {
		serverEventloop.keepAlive(false);
		jdbcServer.stop();
	}

	@Override
	protected QueryResult query(String sql) {
		Properties connectionProperties = new Properties();
		connectionProperties.put("url", "http://localhost:" + port);

		try (
				Connection connection = DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX, connectionProperties);
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(sql)
		) {
			return toQueryResult(resultSet);
		} catch (Exception e) {
			throw new AssertionError(e);
		}
	}

	@Override
	protected QueryResult queryPrepared(String sql, ParamsSetter setter) {
		Properties connectionProperties = new Properties();
		connectionProperties.put("url", "http://localhost:" + port);

		try (Connection connection = DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX, connectionProperties)) {
			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				setter.setValues(statement);
				try (ResultSet resultSet = statement.executeQuery()) {
					return toQueryResult(resultSet);
				}
			}
		} catch (Exception e) {
			throw new AssertionError(e);
		}
	}

	private QueryResult toQueryResult(ResultSet resultSet) throws SQLException {
		List<String> columnNames = new ArrayList<>();
		List<Object[]> columnValues = new ArrayList<>();

		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		assertTrue(columnCount > 0);

		for (int i = 1; i <= columnCount; i++) {
			columnNames.add(metaData.getColumnName(i));
		}

		while (resultSet.next()) {
			Object[] columnValue = new Object[columnCount];

			for (int i = 0; i < columnCount; i++) {
				Object object = resultSet.getObject(i + 1);
				columnValue[i] = object;
			}

			columnValues.add(columnValue);
		}

		if (columnNames.isEmpty()) return QueryResult.empty();

		return new QueryResult(columnNames, columnValues);
	}
}
