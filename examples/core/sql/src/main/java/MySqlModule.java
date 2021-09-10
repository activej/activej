import com.mysql.cj.jdbc.MysqlDataSource;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This module provides a MySql {@link DataSource}
 * <p>
 * It requires a 'mysql.properties' file to be stored in a classpath.
 * The properties file may be created out of 'mysql.properties.template' file.
 */
public class MySqlModule extends AbstractModule {

	public static final String MYSQL_PROPERTIES_FILE = "mysql.properties";
	public static final String INIT_SCRIPT = "init.sql";

	@Provides
	DataSource dataSourceMySql() throws IOException, SQLException {
		Properties properties = new Properties();
		InputStream stream = getClass().getResourceAsStream(MYSQL_PROPERTIES_FILE);

		if (stream == null) {
			throw new RuntimeException("Create a 'mysql.properties' file out of 'mysql.properties.template' " +
					"and add it to resources directory");
		}

		properties.load(stream);

		MysqlDataSource dataSource = new MysqlDataSource();
		dataSource.setUrl("jdbc:mysql://" + properties.getProperty("server") + '/' + properties.getProperty("db"));
		dataSource.setUser(properties.getProperty("user"));
		dataSource.setPassword(properties.getProperty("password"));
		dataSource.setServerTimezone(properties.getProperty("timeZone"));
		dataSource.setAllowMultiQueries(true);

		initialize(dataSource);

		return dataSource;
	}

	private void initialize(MysqlDataSource dataSource) throws SQLException, IOException {
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(new String(loadResource(), UTF_8));
				statement.execute("TRUNCATE TABLE user");
			}
		}
	}

	private static byte[] loadResource() throws IOException {
		try (InputStream stream = Thread.currentThread()
				.getContextClassLoader()
				.getResourceAsStream(MySqlModule.INIT_SCRIPT)
		) {
			assert stream != null;

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int size;
			while ((size = stream.read(buffer)) != -1) {
				baos.write(buffer, 0, size);
			}
			return baos.toByteArray();
		}
	}

}
