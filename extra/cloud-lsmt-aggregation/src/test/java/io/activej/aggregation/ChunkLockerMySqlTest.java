package io.activej.aggregation;

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

import static io.activej.aggregation.ChunkLockerMySql.DEFAULT_LOCK_TABLE;
import static io.activej.aggregation.ChunkLockerMySql.DEFAULT_LOCK_TTL;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.inject.util.Utils.union;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.dataSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChunkLockerMySqlTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private DataSource dataSource;
	private ChunkLockerMySql<Long> lockerA;
	private ChunkLockerMySql<Long> lockerB;

	@Before
	public void before() throws IOException, SQLException {
		dataSource = dataSource("test.properties");
		Executor executor = Executors.newSingleThreadExecutor();

		lockerA = ChunkLockerMySql.create(executor, dataSource, ChunkIdCodec.ofLong(), "test_aggregation");
		lockerB = ChunkLockerMySql.create(executor, dataSource, ChunkIdCodec.ofLong(), "test_aggregation");

		lockerA.initialize();
		lockerA.truncateTables();
	}

	@Test
	public void lock() {
		assertTrue(await(lockerA.getLockedChunks()).isEmpty());
		assertTrue(await(lockerB.getLockedChunks()).isEmpty());

		Set<Long> lockedByA = set(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));
		assertEquals(lockedByA, await(lockerB.getLockedChunks()));

		Set<Long> lockedByB = set(4L, 5L, 6L);
		await(lockerB.lockChunks(lockedByB));

		Set<Long> lockedByBoth = union(lockedByA, lockedByB);

		assertEquals(lockedByBoth, await(lockerA.getLockedChunks()));
		assertEquals(lockedByBoth, await(lockerB.getLockedChunks()));
	}

	@Test
	public void release() {
		Set<Long> lockedByA = set(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));
		Set<Long> lockedByB = set(4L, 5L, 6L);
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
		Set<Long> lockedByA = set(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));
		assertEquals(lockedByA, await(lockerB.getLockedChunks()));

		Set<Long> lockedByB = set(4L, 5L, 6L, 1L);

		Throwable exception = awaitException(lockerB.lockChunks(lockedByB));
		assertThat(exception, instanceOf(ChunksAlreadyLockedException.class));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));
		assertEquals(lockedByA, await(lockerB.getLockedChunks()));
	}

	@Test
	public void releaseChunksLockedByOtherShouldNotRelease() {
		Set<Long> lockedByA = set(1L, 2L, 3L);
		Set<Long> lockedByB = set(4L, 5L, 6L);
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
		Set<Long> lockedByA = set(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));

		expireLockedChunk(2L);

		assertEquals(set(1L, 3L), await(lockerA.getLockedChunks()));
	}

	@Test
	public void lockShouldOverrideExpiredChunks() {
		Set<Long> lockedByA = set(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));

		Set<Long> locked2 = set(1L, 4L);
		Throwable exception = awaitException(lockerA.lockChunks(locked2));
		assertThat(exception, instanceOf(ChunksAlreadyLockedException.class));

		expireLockedChunk(1L);

		await(lockerA.lockChunks(locked2));

		assertEquals(union(lockedByA, locked2), await(lockerA.getLockedChunks()));
	}

	private void expireLockedChunk(long chunkId) {
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement ps = connection.prepareStatement(
					"UPDATE `" + DEFAULT_LOCK_TABLE + "` " +
							"SET `locked_at` = `locked_at` - INTERVAL ? SECOND " +
							"WHERE `chunk_id` = ?"
			)) {
				ps.setLong(1, DEFAULT_LOCK_TTL.getSeconds() + 1);
				ps.setString(2, String.valueOf(chunkId));

				assertEquals(1, ps.executeUpdate());
			}
		} catch (SQLException throwables) {
			throw new AssertionError(throwables);
		}
	}


}
