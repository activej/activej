package advanced.util;

import org.jooq.DSLContext;
import org.jooq.Table;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class Utils {
	private static final String HORIZONTAL_BORDER = "+---+------------+------------+";

	public static void printTables(DataSource dataSource, String... tables) throws SQLException {
		if (tables.length == 0) return;

		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				for (String table : tables) {
					ResultSet resultSet = statement.executeQuery("SELECT * FROM " + table);


					System.out.println('\"' + table + "\":\n" + HORIZONTAL_BORDER);
					ResultSetMetaData metaData = resultSet.getMetaData();
					System.out.printf("|%3s|%12s|%12s|%n",
							metaData.getColumnName(1),
							metaData.getColumnName(2),
							metaData.getColumnName(3));

					System.out.println(HORIZONTAL_BORDER);

					boolean isEmpty = true;
					while (resultSet.next()) {
						isEmpty = false;
						System.out.printf("|%3s|%12s|%12s|%n",
								resultSet.getInt(1),
								resultSet.getString(2),
								resultSet.getString(3));
					}

					System.out.println(isEmpty ? '\n' : HORIZONTAL_BORDER + '\n');
				}
			}
		}
	}

	public static void printTables(DSLContext context, Table<?>... tables) {
		for (Table<?> table : tables) {
			System.out.println(table + ":");
			System.out.println(context.select().from(table).fetch());
		}
	}

	public static void initialize(DataSource dataSource, String... initScripts) throws SQLException, IOException {
		for (String initScript : initScripts) {
			try (Connection connection = dataSource.getConnection()) {
				try (Statement statement = connection.createStatement()) {
					statement.execute(new String(loadResource(initScript), UTF_8));
				}
			}
		}
	}

	public static void initialize(DSLContext context, String... initScripts) throws IOException {
		for (String initScript : initScripts) {
			context.execute(new String(loadResource(initScript), UTF_8));
		}
	}

	private static byte[] loadResource(String initScript) throws IOException {
		try (InputStream stream = Thread.currentThread()
				.getContextClassLoader()
				.getResourceAsStream(initScript)
		) {
			assert stream != null;
			return stream.readAllBytes();
		}
	}
}
