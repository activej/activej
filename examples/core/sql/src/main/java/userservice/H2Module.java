package userservice;

import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;

/**
 * This module provides an H2 {@link DataSource}
 * <p>
 * It uses an in-memory H2 database
 */
public class H2Module extends AbstractModule {

	public static final String INIT_SCRIPT = "userservice/init.sql";

	@Provides
	DataSource dataSourceH2() {
		JdbcDataSource dataSource = new JdbcDataSource();
		dataSource.setURL("jdbc:h2:mem:db1;INIT=runscript from 'classpath:" + INIT_SCRIPT + "';DB_CLOSE_DELAY=-1");
		return dataSource;
	}
}
