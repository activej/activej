package advanced;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;

import javax.sql.DataSource;

public final class DataSourcePooledModule extends AbstractModule {

	private DataSourcePooledModule() {
	}

	public static DataSourcePooledModule create() {
		return new DataSourcePooledModule();
	}

	@Provides
	DataSource dataSource() {
		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1");
		return new HikariDataSource(hikariConfig);
	}
}
