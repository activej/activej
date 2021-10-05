package ${groupId};

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
		dataSource.setURL("jdbc:h2:mem:demo");
		return dataSource;
	}
}
