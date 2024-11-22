package io.activej.cube.service;

import io.activej.cube.aggregation.ChunksAlreadyLockedException;
import io.activej.reactor.Reactor;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.LongStream;

import static io.activej.common.collection.CollectionUtils.union;
import static io.activej.cube.TestUtils.dataSource;
import static io.activej.cube.linear.CubeSqlNaming.DEFAULT_SQL_NAMING;
import static io.activej.cube.service.MySqlChunkLocker.DEFAULT_LOCK_TTL;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MySqlChunkLockerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();
	public static final String AGGREGATION_ID = "test_aggregation";

	private DataSource dataSource;
	private MySqlChunkLocker lockerA;
	private MySqlChunkLocker lockerB;

	@Before
	public void before() throws IOException, SQLException {
		dataSource = dataSource("test.properties");
		Executor executor = Executors.newSingleThreadExecutor();

		Reactor reactor = Reactor.getCurrentReactor();
		lockerA = MySqlChunkLocker.create(reactor, executor, dataSource, AGGREGATION_ID);
		lockerB = MySqlChunkLocker.create(reactor, executor, dataSource, AGGREGATION_ID);

		lockerA.initialize();
		lockerA.truncateTables();

		try (
			Connection connection = dataSource.getConnection();
			PreparedStatement ps = connection.prepareStatement(DEFAULT_SQL_NAMING.sql("""
					INSERT INTO {chunk}
					(`id`, `aggregation`, `measures`, `min_key`, `max_key`, `item_count`, `added_revision`)
					VALUES $values
					""")
				.replace("$values", String.join(",", nCopies(100, "(?,?,?,?,?,?,?)"))))
		) {
			Set<Long> chunkIds = LongStream.range(0, 100).boxed().collect(toSet());

			int index = 1;
			for (Long chunkId : chunkIds) {
				ps.setLong(index++, chunkId);
				ps.setString(index++, AGGREGATION_ID);
				ps.setString(index++, "measures");
				ps.setString(index++, "min key");
				ps.setString(index++, "max key");
				ps.setInt(index++, 100);
				ps.setLong(index++, 1);
			}
			ps.executeUpdate();
		}
	}

	@Test
	public void lock() {
		assertTrue(await(lockerA.getLockedChunks()).isEmpty());
		assertTrue(await(lockerB.getLockedChunks()).isEmpty());

		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));
		assertEquals(lockedByA, await(lockerB.getLockedChunks()));

		Set<Long> lockedByB = Set.of(4L, 5L, 6L);
		await(lockerB.lockChunks(lockedByB));

		Set<Long> lockedByBoth = union(lockedByA, lockedByB);

		assertEquals(lockedByBoth, await(lockerA.getLockedChunks()));
		assertEquals(lockedByBoth, await(lockerB.getLockedChunks()));
	}

	@Test
	public void release() {
		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));
		Set<Long> lockedByB = Set.of(4L, 5L, 6L);
		await(lockerB.lockChunks(lockedByB));

		Set<Long> lockedByBoth = union(lockedByA, lockedByB);

		assertEquals(lockedByBoth, await(lockerA.getLockedChunks()));
		assertEquals(lockedByBoth, await(lockerB.getLockedChunks()));

		await(lockerA.releaseChunks(lockedByA));

		assertEquals(lockedByB, await(lockerA.getLockedChunks()));
		assertEquals(lockedByB, await(lockerB.getLockedChunks()));

		await(lockerB.releaseChunks(lockedByB));

		assertTrue(await(lockerA.getLockedChunks()).isEmpty());
		assertTrue(await(lockerB.getLockedChunks()).isEmpty());
	}

	@Test
	public void lockAlreadyLockedShouldThrowError() {
		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));
		assertEquals(lockedByA, await(lockerB.getLockedChunks()));

		Set<Long> lockedByB = Set.of(4L, 5L, 6L, 1L);

		Exception exception = awaitException(lockerB.lockChunks(lockedByB));
		assertThat(exception, instanceOf(ChunksAlreadyLockedException.class));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));
		assertEquals(lockedByA, await(lockerB.getLockedChunks()));
	}

	@Test
	public void releaseChunksLockedByOtherShouldNotRelease() {
		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		Set<Long> lockedByB = Set.of(4L, 5L, 6L);
		Set<Long> locked = union(lockedByA, lockedByB);
		await(lockerA.lockChunks(lockedByA));
		assertEquals(lockedByA, await(lockerA.getLockedChunks()));

		await(lockerB.lockChunks(lockedByB));
		assertEquals(locked, await(lockerA.getLockedChunks()));

		await(lockerB.releaseChunks(lockedByA));
		assertEquals(locked, await(lockerA.getLockedChunks()));

		await(lockerA.releaseChunks(lockedByB));
		assertEquals(locked, await(lockerA.getLockedChunks()));

		await(lockerA.releaseChunks(lockedByA));
		await(lockerB.releaseChunks(lockedByB));

		assertTrue(await(lockerB.getLockedChunks()).isEmpty());
		assertTrue(await(lockerA.getLockedChunks()).isEmpty());
	}

	@Test
	public void getLockedChunksShouldNotReturnExpiredChunks() {
		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));

		expireLockedChunk(2L);

		assertEquals(Set.of(1L, 3L), await(lockerA.getLockedChunks()));
	}

	@Test
	public void lockShouldOverrideExpiredChunks() {
		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));

		Set<Long> locked2 = Set.of(1L, 4L);
		Exception exception = awaitException(lockerA.lockChunks(locked2));
		assertThat(exception, instanceOf(ChunksAlreadyLockedException.class));

		expireLockedChunk(1L);

		await(lockerA.lockChunks(locked2));

		assertEquals(union(lockedByA, locked2), await(lockerA.getLockedChunks()));
	}

	private void expireLockedChunk(long chunkId) {
		try (
			Connection connection = dataSource.getConnection();
			PreparedStatement ps = connection.prepareStatement(DEFAULT_SQL_NAMING.sql("""
				UPDATE {chunk}
				SET `locked_at` = `locked_at` - INTERVAL ? SECOND
				WHERE `id` = ?
				"""))
		) {
			ps.setLong(1, DEFAULT_LOCK_TTL.getSeconds() + 1);
			ps.setString(2, String.valueOf(chunkId));

			assertEquals(1, ps.executeUpdate());
		} catch (SQLException exceptions) {
			throw new AssertionError(exceptions);
		}
	}

}
