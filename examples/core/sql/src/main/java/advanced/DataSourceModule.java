package advanced;

import com.mysql.cj.jdbc.MysqlDataSource;
import io.activej.config.Config;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;

import javax.sql.DataSource;
import java.sql.SQLException;

public final class DataSourceModule extends AbstractModule {
	private DataSourceModule() {
	}

	public static DataSourceModule create() {
		return new DataSourceModule();
	}

	@Provides
	DataSource dataSource(Config config) throws SQLException {
		config = config.getChild("mysql");
		MysqlDataSource dataSource = new MysqlDataSource();

		dataSource.setUrl("jdbc:mysql://" + config.get("server") + '/' + config.get("db"));
		dataSource.setUser(config.get("user"));
		dataSource.setPassword(config.get("password"));
		dataSource.setServerTimezone(config.get("timeZone"));
		dataSource.setAllowMultiQueries(true);
		return dataSource;
	}
}
