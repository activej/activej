package io.activej.cube.ot.sql;

import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import static io.activej.common.sql.SqlUtils.executeScript;
import static io.activej.cube.TestUtils.dataSource;
import static org.junit.Assert.assertEquals;

public class SqlAtomicSequenceTest {

	private DataSource dataSource;
	private SqlAtomicSequence sequence;

	@Before
	public void before() throws IOException, SQLException, ExecutionException, InterruptedException {
		dataSource = dataSource("test.properties");
		executeScript(dataSource, getClass().getPackage().getName() + "/" + getClass().getSimpleName() + ".sql");
		sequence = SqlAtomicSequence.ofLastInsertID("sequence", "next");
	}

	@Test
	public void testSqlAtomicSequence() throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			assertEquals(1, sequence.getAndAdd(connection, 1));
			assertEquals(2, sequence.getAndAdd(connection, 1));
			assertEquals(3, sequence.getAndAdd(connection, 1));
			assertEquals(4, sequence.getAndAdd(connection, 10));
			assertEquals(14, sequence.getAndAdd(connection, 10));
			assertEquals(24, sequence.getAndAdd(connection, 10));
			assertEquals(134, sequence.addAndGet(connection, 100));
			assertEquals(234, sequence.addAndGet(connection, 100));
		}
	}
}
