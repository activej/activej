package advanced;

import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;

public final class DataSourceModule extends AbstractModule {
	private DataSourceModule() {
	}

	public static DataSourceModule create() {
		return new DataSourceModule();
	}

	@Provides
	DataSource dataSource() {
		JdbcDataSource dataSource = new JdbcDataSource();
		dataSource.setURL("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1");
		return dataSource;
	}

}
