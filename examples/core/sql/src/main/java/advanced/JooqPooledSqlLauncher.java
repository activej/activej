package advanced;

import advanced.util.Utils;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.io.IOException;

import static advanced.jooq.model.tables.NewUser.NEW_USER;
import static advanced.jooq.model.tables.User.USER;

public final class JooqPooledSqlLauncher extends Launcher {
	public static final String DATASOURCE_PROPERTIES = "advanced/mysql-pooled.properties";

	public static final String USER_TABLE_SCRIPT = "advanced/ddl/user.sql";
	public static final String NEW_USER_TABLE_SCRIPT = "advanced/ddl/new_user.sql";
	public static final String INIT_TABLES_SCRIPT = "advanced/init.sql";

	@Inject
	DSLContext context;

	@Provides
	Config config() {
		return Config.ofClassPathProperties(DATASOURCE_PROPERTIES, true)
			.overrideWith(Config.ofSystemProperties("config"));
	}

	@Provides
	DSLContext dslContext(DataSource dataSource) {
		return DSL.using(dataSource, SQLDialect.MYSQL, new Settings().withExecuteLogging(false));
	}

	@Override
	protected void onStart() throws IOException {
		Utils.initialize(context, USER_TABLE_SCRIPT, NEW_USER_TABLE_SCRIPT, INIT_TABLES_SCRIPT);

		System.out.println("TABLES BEFORE:");
		Utils.printTables(context, USER, NEW_USER);
	}

	@Override
	protected void run() {
		logger.info("Copying data from table {} to table {}...", USER, NEW_USER);

		context.insertInto(NEW_USER)
			.select(context.select().from(USER))
			.execute();
	}

	@Override
	protected void onStop() {
		System.out.println("TABLES AFTER:");
		Utils.printTables(context, USER, NEW_USER);
	}

	@Override
	protected Module getModule() {
		return Modules.combine(
			ConfigModule.create(),
			DataSourcePooledModule.create()
		);
	}

	public static void main(String[] args) throws Exception {
		new JooqPooledSqlLauncher().launch(args);
	}
}
