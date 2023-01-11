package advanced;

import advanced.util.ConfigConverter_Hikari;
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
		ConfigConverter_Hikari converter = ConfigConverter_Hikari.create().withAllowMultiQueries();
		return new HikariDataSource(config.get(converter, "hikari"));
	}
}
