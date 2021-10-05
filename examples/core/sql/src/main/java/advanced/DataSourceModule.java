package advanced;

import advanced.util.HikariConfigConverter;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.zaxxer.hikari.HikariDataSource;
import io.activej.config.Config;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;

import static io.activej.common.Checks.checkState;

public final class DataSourceModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(DataSourceModule.class);

	public static final HikariConfigConverter HIKARI_CONFIG_CONVERTER = HikariConfigConverter.create()
			.withAllowMultiQueries();

	private DataSourceModule() {
	}

	public static DataSourceModule create() {
		return new DataSourceModule();
	}

	@Provides
	DataSource dataSource(Config config) throws SQLException {
		if (config.hasChild("mysql")) {
			checkState(!config.hasChild("hikari"), "Set either 'mysql.*' or 'hikari.*' properties");
			logger.info("Using MySQL data source");
			return mysqlDataSource(config.getChild("mysql"));
		}

		if (config.hasChild("hikari")) {
			checkState(!config.hasChild("mysql"), "Set either 'mysql.*' or 'hikari.*' properties");
			logger.info("Using HikariCP data source");
			return new HikariDataSource(config.get(HIKARI_CONFIG_CONVERTER, "hikari"));
		}

		JdbcDataSource dataSource = new JdbcDataSource();
		dataSource.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
		logger.info("Using H2 data source");
		return dataSource;
	}

	private static DataSource mysqlDataSource(Config config) throws SQLException {
		MysqlDataSource dataSource = new MysqlDataSource();
		dataSource.setUrl("jdbc:mysql://" + config.get("server") + '/' + config.get("db"));
		dataSource.setUser(config.get("user"));
		dataSource.setPassword(config.get("password"));
		dataSource.setServerTimezone(config.get("timeZone"));
		dataSource.setAllowMultiQueries(true);
		return dataSource;
	}
}
