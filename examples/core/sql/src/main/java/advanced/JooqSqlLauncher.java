package advanced;

import advanced.util.Utils;
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

import static advanced.jooq.model.Tables.NEW_USERS;
import static advanced.jooq.model.Tables.USERS;

public final class JooqSqlLauncher extends Launcher {
	public static final String USER_TABLE_SCRIPT = "advanced/ddl/user.sql";
	public static final String NEW_USER_TABLE_SCRIPT = "advanced/ddl/new_user.sql";
	public static final String INIT_TABLES_SCRIPT = "advanced/init.sql";

	@Inject
	DSLContext context;

	@Provides
	DSLContext dslContext(DataSource dataSource) {
		return DSL.using(dataSource, SQLDialect.MYSQL, new Settings().withExecuteLogging(false));
	}

	@Override
	protected void onStart() throws IOException {
		Utils.initialize(context, USER_TABLE_SCRIPT, NEW_USER_TABLE_SCRIPT, INIT_TABLES_SCRIPT);

		System.out.println("TABLES BEFORE:");
		Utils.printTables(context, USERS, NEW_USERS);
	}

	@Override
	protected void run() {
		logger.info("Copying data from table {} to table {}...", USERS, NEW_USERS);

		context.insertInto(NEW_USERS)
			.select(context.select().from(USERS))
			.execute();
	}

	@Override
	protected void onStop() {
		System.out.println("TABLES AFTER:");
		Utils.printTables(context, USERS, NEW_USERS);
	}

	@Override
	protected Module getModule() {
		return Modules.combine(
			ConfigModule.create(),
			DataSourceModule.create()
		);
	}

	public static void main(String[] args) throws Exception {
		new JooqSqlLauncher().launch(args);
	}
}
