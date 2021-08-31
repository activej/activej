package io.activej.cube.service;

import io.activej.aggregation.ChunkIdCodec;
import io.activej.aggregation.ChunkLocker;
import io.activej.aggregation.ChunksAlreadyLockedException;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.error.FatalErrorHandlers;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.test.TestUtils.dataSource;

public class ChunkLockerMySqlDeadlockTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private ChunkLockerMySql<Long> lockerA;
	private ChunkLockerMySql<Long> lockerB;

	@Before
	public void before() throws IOException, SQLException {
		DataSource dataSource = dataSource("test.properties");

		lockerA = ChunkLockerMySql.create(Executors.newSingleThreadExecutor(), dataSource, ChunkIdCodec.ofLong(), "test_aggregation")
				.withLockedTtl(Duration.ofSeconds(1));
		lockerB = ChunkLockerMySql.create(Executors.newSingleThreadExecutor(), dataSource, ChunkIdCodec.ofLong(), "test_aggregation")
				.withLockedTtl(Duration.ofSeconds(1));

		lockerA.initialize();
		lockerA.truncateTables();
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
					.withFatalErrorHandler(FatalErrorHandlers.exitOnAnyError());
			ThreadLocalRandom random = ThreadLocalRandom.current();

			action(random, locker);
			eventloop.run();
		};
	}

	private static void action(ThreadLocalRandom random, ChunkLocker<Long> locker) {
		Set<Long> chunkIds = new HashSet<>();
		for (int i = 0; i < 100; i++) {
			chunkIds.add(random.nextLong(1000));
		}
//		locker.getLockedChunks()
//				.then(chunks -> {
//					chunkIds.removeAll(chunks);
		locker.lockChunks(chunkIds)
//				.then(() -> locker.releaseChunks(chunkIds))
				.thenEx((v, e) -> {
					if (e == null || e instanceof ChunksAlreadyLockedException) return Promise.complete();
					e.printStackTrace();
					throw new RuntimeException(e);
				})
				.whenResult(() -> action(random, locker));
	}
}
