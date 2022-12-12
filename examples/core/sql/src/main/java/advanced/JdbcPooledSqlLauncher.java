package advanced;

import advanced.util.Utils;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class JdbcPooledSqlLauncher extends Launcher {
	public static final String DATASOURCE_PROPERTIES = "advanced/mysql-pooled.properties";

	public static final String USER_TABLE_SCRIPT = "advanced/ddl/user.sql";
	public static final String NEW_USER_TABLE_SCRIPT = "advanced/ddl/new_user.sql";
	public static final String INIT_TABLES_SCRIPT = "advanced/init.sql";

	public static final String TABLE_FROM = "user";
	public static final String TABLE_TO = "new_user";

	@Inject
	DataSource dataSource;

	@Provides
	Config config() {
		return Config.ofClassPathProperties(DATASOURCE_PROPERTIES, true)
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected void onStart() throws IOException, SQLException {
		Utils.initialize(dataSource, USER_TABLE_SCRIPT, NEW_USER_TABLE_SCRIPT, INIT_TABLES_SCRIPT);

		System.out.println("TABLES BEFORE:");
		Utils.printTables(dataSource, TABLE_FROM, TABLE_TO);
	}

	@Override
	protected void run() throws SQLException {
		logger.info("Copying data from table \"{}\" to table \"{}\"...", TABLE_FROM, TABLE_TO);

		try (Connection connection = dataSource.getConnection();
			 Statement statement = connection.createStatement()
		) {
			statement.executeUpdate("" +
					"INSERT INTO " + TABLE_TO + " " +
					"SELECT * " +
					"FROM " + TABLE_FROM);
		}
	}

	@Override
	protected void onStop() throws SQLException {
		System.out.println("TABLES AFTER:");
		Utils.printTables(dataSource, TABLE_FROM, TABLE_TO);
	}

	@Override
	protected Module getModule() {
		return Modules.combine(
				ConfigModule.create(),
				DataSourcePooledModule.create()
		);
	}

	public static void main(String[] args) throws Exception {
		new JdbcPooledSqlLauncher().launch(args);
	}
}
