package io.activej.test;

import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestUtilsTest {
	@Test
	public void dataSource() throws IOException, SQLException {
		DataSource dataSource = TestUtils.dataSource("test.properties");
		try (
			Connection connection = dataSource.getConnection();
			Statement statement = connection.createStatement()
		) {
			statement.execute("""
				SELECT 1;
				SELECT 2;
				""");
		}
	}
}
