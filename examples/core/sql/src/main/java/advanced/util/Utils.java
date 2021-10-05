package advanced.util;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;

public final class Utils {
	private static final String HORIZONTAL_BORDER = String.join("", nCopies(31, "-"));

	public static void printTables(DataSource dataSource, String... tables) throws SQLException {
		if (tables.length == 0) return;

		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				for (String table : tables) {
					ResultSet resultSet = statement.executeQuery("SELECT * FROM " + table);


					System.out.println(table + ":\n" + HORIZONTAL_BORDER);
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

					if (isEmpty) {
						System.out.printf("|%29s|%n", "(empty)");
					}
					System.out.println(HORIZONTAL_BORDER + '\n');
				}
			}
		}
	}

	public static void initialize(DataSource dataSource, String initScript) throws SQLException, IOException {
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(new String(loadResource(initScript), UTF_8));
			}
		}
	}

	private static byte[] loadResource(String initScript) throws IOException {
		try (InputStream stream = Thread.currentThread()
				.getContextClassLoader()
				.getResourceAsStream(initScript)
		) {
			assert stream != null;

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int size;
			while ((size = stream.read(buffer)) != -1) {
				baos.write(buffer, 0, size);
			}
			return baos.toByteArray();
		}
	}
}
