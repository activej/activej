package io.activej.aggregation;

import io.activej.ot.util.IdGeneratorSql;
import io.activej.ot.util.SqlAtomicSequence;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static io.activej.common.sql.SqlUtils.executeScript;
import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.test.TestUtils.dataSource;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;

public class IdGeneratorSqlTest {
	private DataSource dataSource;
	private SqlAtomicSequence sequence;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Before
	public void before() throws IOException, SQLException {
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

	@Test
	public void testIdGeneratorSql() {
		IdGeneratorSql idGeneratorSql = IdGeneratorSql.create(getCurrentReactor(), newSingleThreadExecutor(), dataSource, sequence);

		assertEquals(1, (long) await(idGeneratorSql.get()));
		assertEquals(2, (long) await(idGeneratorSql.get()));
		assertEquals(3, (long) await(idGeneratorSql.get()));
	}

	@Test
	public void testIdGeneratorSql10() {
		IdGeneratorSql idGeneratorSql = IdGeneratorSql.create(getCurrentReactor(), newSingleThreadExecutor(), dataSource, sequence)
				.withStride(10);
		for (int i = 1; i <= 25; i++) {
			assertEquals(i, (long) await(idGeneratorSql.get()));
		}
		assertEquals(31, idGeneratorSql.getLimit());
	}

}
