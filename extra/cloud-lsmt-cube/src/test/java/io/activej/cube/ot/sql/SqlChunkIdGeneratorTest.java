package io.activej.cube.ot.sql;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.activej.common.sql.SqlUtils.executeScript;
import static io.activej.test.TestUtils.dataSource;
import static org.junit.Assert.assertEquals;

public class SqlChunkIdGeneratorTest {

	private DataSource dataSource;
	private SqlAtomicSequence sequence;

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	public static final int N_THREADS = 10;
	public static final int N_CHUNKS = 10_000;
	public static final int N_ITERATIONS = 100;

	private List<Thread> threads;
	private List<Reactor> reactors;
	private List<SqlChunkIdGenerator> chunkIdGenerators;

	private Map<Integer, List<Long>> idsMap;

	@Before
	public void before() throws IOException, SQLException, ExecutionException, InterruptedException {
		dataSource = dataSource("test.properties");
		executeScript(dataSource, getClass().getPackage().getName() + "/" + getClass().getSimpleName() + ".sql");
		sequence = SqlAtomicSequence.ofLastInsertID("sequence", "next");

		threads = new ArrayList<>(N_THREADS);
		reactors = new ArrayList<>(N_THREADS);
		chunkIdGenerators = new ArrayList<>(N_THREADS);

		for (int i = 0; i < N_THREADS; i++) {
			Eventloop eventloop = Eventloop.create();
			threads.add(new Thread(eventloop));
			chunkIdGenerators.add(SqlChunkIdGenerator.create(eventloop, Executors.newSingleThreadExecutor(), dataSource, sequence));
			reactors.add(eventloop);
		}

		idsMap = new ConcurrentHashMap<>();
	}

	@Test
	public void test() throws ExecutionException, InterruptedException {
		CompletableFuture<?>[] futures = new CompletableFuture[N_THREADS];
		for (int i = 0; i < N_THREADS; i++) {
			Reactor reactor = reactors.get(i);
			SqlChunkIdGenerator idGenerator = chunkIdGenerators.get(i);
			int finalI = i;
			futures[i] = reactor.submit(() -> Promises.until(
				0,
				j -> Promises.toList(IntStream.range(0, N_CHUNKS)
						.mapToObj($ -> idGenerator.createProtoChunkId()))
					.then(idGenerator::convertToActualChunkIds)
					.map(chunkIds -> {
						idsMap.computeIfAbsent(finalI, $ -> new ArrayList<>()).addAll(chunkIds);
						return j + 1;
					}),
				j -> j == N_ITERATIONS));
		}

		for (Thread thread : threads) {
			thread.start();
		}

		CompletableFuture.allOf(futures).get();

		assertEquals(N_THREADS, idsMap.size());

		long expectedSize = N_THREADS * N_CHUNKS * N_ITERATIONS;
		assertEquals(expectedSize, idsMap.values().stream().mapToLong(Collection::size).sum());

		Set<Long> expectedIds = LongStream.range(1, expectedSize + 1).boxed().collect(Collectors.toSet());
		Set<Long> actualIds = idsMap.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());

		assertEquals(expectedIds, actualIds);
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
