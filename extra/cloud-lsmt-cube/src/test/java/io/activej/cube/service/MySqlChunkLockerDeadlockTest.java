package io.activej.cube.service;

import io.activej.cube.aggregation.ChunksAlreadyLockedException;
import io.activej.cube.aggregation.IChunkLocker;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
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

import static io.activej.common.exception.FatalErrorHandlers.haltOnError;
import static io.activej.cube.TestUtils.dataSource;
import static io.activej.cube.linear.CubeSqlNaming.DEFAULT_SQL_NAMING;
import static java.util.stream.Collectors.toSet;

public class MySqlChunkLockerDeadlockTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();
	public static final String AGGREGATION_ID = "test_aggregation";
	public static final int MAX_CHUNK_ID = 1000;

	private MySqlChunkLocker lockerA;
	private MySqlChunkLocker lockerB;

	@Before
	public void before() throws IOException, SQLException {
		DataSource dataSource = dataSource("test.properties");

		Reactor reactor = Reactor.getCurrentReactor();
		lockerA = MySqlChunkLocker.builder(reactor, Executors.newSingleThreadExecutor(), dataSource, AGGREGATION_ID)
			.withLockedTtl(Duration.ofSeconds(1))
			.build();
		lockerB = MySqlChunkLocker.builder(reactor, Executors.newSingleThreadExecutor(), dataSource, AGGREGATION_ID)
			.withLockedTtl(Duration.ofSeconds(1))
			.build();

		lockerA.initialize();
		lockerA.truncateTables();

		try (
			Connection connection = dataSource.getConnection();
			PreparedStatement ps = connection.prepareStatement(DEFAULT_SQL_NAMING.sql("""
				INSERT INTO {chunkTable}
				(`id`, `aggregation`, `measures`, `min_key`, `max_key`, `item_count`, `added_revision`)
				VALUES (?,?,?,?,?,?,?)
				"""))
		) {
			Set<Long> chunkIds = LongStream.range(0, MAX_CHUNK_ID).boxed().collect(toSet());
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

	private static Runnable run(IChunkLocker locker) {
		return () -> {
			Eventloop eventloop = Eventloop.builder()
				.withCurrentThread()
				.withFatalErrorHandler(haltOnError())
				.build();
			ThreadLocalRandom random = ThreadLocalRandom.current();

			action(random, locker);
			eventloop.run();
		};
	}

	private static void action(ThreadLocalRandom random, IChunkLocker locker) {
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
