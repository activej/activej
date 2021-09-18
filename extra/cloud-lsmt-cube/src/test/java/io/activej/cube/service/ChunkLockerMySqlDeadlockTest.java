package io.activej.cube.service;

import io.activej.aggregation.ChunkIdCodec;
import io.activej.aggregation.ChunkLocker;
import io.activej.aggregation.ChunksAlreadyLockedException;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static io.activej.common.exception.FatalErrorHandler.haltOnError;
import static io.activej.cube.service.ChunkLockerMySql.CHUNK_TABLE;
import static io.activej.test.TestUtils.dataSource;
import static java.util.stream.Collectors.toSet;

public class ChunkLockerMySqlDeadlockTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();
	public static final String AGGREGATION_ID = "test_aggregation";
	public static final int MAX_CHUNK_ID = 1000;

	private ChunkLockerMySql<Long> lockerA;
	private ChunkLockerMySql<Long> lockerB;

	@Before
	public void before() throws IOException, SQLException {
		DataSource dataSource = dataSource("test.properties");

		lockerA = ChunkLockerMySql.create(Executors.newSingleThreadExecutor(), dataSource, ChunkIdCodec.ofLong(), AGGREGATION_ID)
				.withLockedTtl(Duration.ofSeconds(1));
		lockerB = ChunkLockerMySql.create(Executors.newSingleThreadExecutor(), dataSource, ChunkIdCodec.ofLong(), AGGREGATION_ID)
				.withLockedTtl(Duration.ofSeconds(1));

		lockerA.initialize();
		lockerA.truncateTables();

		try (Connection connection = dataSource.getConnection()) {
			Set<Long> chunkIds = LongStream.range(0, MAX_CHUNK_ID).boxed().collect(toSet());
			try (PreparedStatement ps = connection.prepareStatement("" +
					"INSERT INTO " + CHUNK_TABLE +
					" (`id`, `aggregation`, `measures`, `min_key`, `max_key`, `item_count`, `added_revision`) " +
					"VALUES (?,?,?,?,?,?,?)")) {
				for (Long chunkId : chunkIds) {
					ps.setLong(1, chunkId);
					ps.setString(2, AGGREGATION_ID);
					ps.setString(3, "measures");
					ps.setString(4, "min key");
					ps.setString(5, "max key");
					ps.setInt(6, 100);
					ps.setLong(7, 1);
					ps.executeUpdate();
				}
			}
		}
	}

	@Test
	@Ignore("Runs until stopped or deadlock occurs")
	public void deadlock() throws InterruptedException {
		Thread t1 = new Thread(run(lockerA));
		Thread t2 = new Thread(run(lockerB));

		t1.start();
		t2.start();

		t1.join();
		t2.join();
	}

	private static Runnable run(ChunkLocker<Long> locker) {
		return () -> {
			Eventloop eventloop = Eventloop.create()
					.withCurrentThread()
					.withFatalErrorHandler(haltOnError());
			ThreadLocalRandom random = ThreadLocalRandom.current();

			action(random, locker);
			eventloop.run();
		};
	}

	private static void action(ThreadLocalRandom random, ChunkLocker<Long> locker) {
		Set<Long> chunkIds = new HashSet<>();
		for (int i = 0; i < 100; i++) {
			chunkIds.add(random.nextLong(MAX_CHUNK_ID));
		}
		locker.getLockedChunks()
				.then(chunks -> {
					chunkIds.removeAll(chunks);
					return locker.lockChunks(chunkIds);
				})
				.then(() -> locker.releaseChunks(chunkIds))
				.then((v, e) -> {
					if (e == null || e instanceof ChunksAlreadyLockedException) return Promise.complete();
					e.printStackTrace();
					throw new RuntimeException(e);
				})
				.whenResult(() -> action(random, locker));
	}
}
