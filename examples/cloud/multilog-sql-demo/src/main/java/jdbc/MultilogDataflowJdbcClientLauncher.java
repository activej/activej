package jdbc;

import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.dataflow.calcite.jdbc.client.Driver;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import misc.PrintUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

import static jdbc.MultilogDataflowJdbcServerLauncher.DEFAULT_JDBC_SERVER_PORT;

public final class MultilogDataflowJdbcClientLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "dataflow-jdbc-client.properties";

	@Inject
	String url;

	@Provides
	Config config() {
		return Config.create()
				.with("dataflow.jdbc.url", "http://localhost:" + DEFAULT_JDBC_SERVER_PORT)
				.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

	@Provides
	String url(Config config) {
		return config.get("dataflow.jdbc.url");
	}

	@Override
	protected Module getModule() {
		return ConfigModule.create();
	}

	@Override
	protected void run() throws Exception {
		Scanner scanIn = new Scanner(System.in);

		Properties connectionProperties = new Properties();
		connectionProperties.put("url", url);

		try (
				Connection connection = DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX, connectionProperties);
				Statement statement = connection.createStatement()
		) {
			while (true) {
				System.out.println("Enter your query:");
				System.out.print("> ");
				String query = scanIn.nextLine();
				if (query.isEmpty()) {
					System.out.println("Exiting...");
					break;
				}

				try (ResultSet resultSet = statement.executeQuery(query)) {
					PrintUtils.printResultSet(resultSet);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		new MultilogDataflowJdbcClientLauncher().launch(args);
	}
}
