package io.activej.dataflow.calcite.jdbc;

import io.activej.common.exception.FatalErrorHandler;
import io.activej.dataflow.calcite.AbstractCalciteTest;
import io.activej.dataflow.calcite.SqlDataflow_Reactive;
import io.activej.dataflow.calcite.inject.CalciteClientModule;
import io.activej.dataflow.jdbc.driver.Driver;
import io.activej.dataflow.jdbc.driver.utils.InstantHolder;
import io.activej.eventloop.Eventloop;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.TestUtils;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertTrue;

public class CalciteJDBCTest extends AbstractCalciteTest {

	private int port;
	private Eventloop server1Eventloop;
	private Eventloop server2Eventloop;
	private HttpServer jdbcServer;

	@Override
	protected Module getAdditionalServerModule() {
		return ModuleBuilder.create()
				.install(CalciteClientModule.create())
				.bind(int.class).to(TestUtils::getFreePort)
				.bind(HttpServer.class).to((reactor, port, calciteSqlDataflow) -> new HttpServer.Builder<>()
								.withHandler(new AvaticaJsonHandler(new LocalService(DataflowMeta.create(reactor, calciteSqlDataflow))))
								.withPort(port)
								.build(),
						NioReactor.class, int.class, SqlDataflow_Reactive.class)
				.bind(Eventloop.class).to(() -> Eventloop.create().withFatalErrorHandler(FatalErrorHandler.rethrow()))
				.bind(NioReactor.class).to(Eventloop.class)
				.build();
	}

	@Override
	protected void onSetUp() throws Exception {
		jdbcServer = (ThreadLocalRandom.current().nextBoolean() ? server1Injector : server2Injector).getInstance(HttpServer.class);
		server1Eventloop = server1Injector.getInstance(Eventloop.class);
		server2Eventloop = server2Injector.getInstance(Eventloop.class);
		server1Eventloop.keepAlive(true);
		server2Eventloop.keepAlive(true);

		port = jdbcServer.getPort();

		jdbcServer.start();

		new Thread(server1Eventloop).start();
		new Thread(server2Eventloop).start();

		server1Eventloop.submit(() -> server1.listen()).get();
		server2Eventloop.submit(() -> server2.listen()).get();
	}

	@Override
	protected void onTearDown() {
		server1Eventloop.keepAlive(false);
		server2Eventloop.keepAlive(false);
		jdbcServer.stop();
	}

	@Override
	protected QueryResult query(String sql) {
		try (
				Connection connection = DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX + "http://localhost:" + port);
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
		try (Connection connection = DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX + "http://localhost:" + port)) {
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

	@Override
	protected List<QueryResult> queryPreparedRepeated(String sql, ParamsSetter... paramsSetters) {
		List<QueryResult> results = new ArrayList<>(paramsSetters.length);
		try (Connection connection = DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX + "http://localhost:" + port)) {
			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				for (ParamsSetter setter : paramsSetters) {
					setter.setValues(statement);
					try (ResultSet resultSet = statement.executeQuery()) {
						results.add(toQueryResult(resultSet));
					}
				}
			}
		} catch (Exception e) {
			throw new AssertionError(e);
		}

		return results;
	}

	@Override
	protected InstantHolder wrapInstant(Instant instant) {
		return new InstantHolder(instant);
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

	@Test
	@Ignore("Never stops, for debug purposes only")
	public void startServer() throws InterruptedException {
		System.out.println("Server port: " + port);
		Thread.currentThread().join();
	}
}
