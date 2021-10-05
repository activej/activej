package advanced;

import advanced.util.HikariConfigConverter;
import com.zaxxer.hikari.HikariDataSource;
import io.activej.config.Config;
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
	DataSource dataSource(Config config) {
		HikariConfigConverter converter = HikariConfigConverter.create().withAllowMultiQueries();
		return new HikariDataSource(config.get(converter, "hikari"));
	}
}
