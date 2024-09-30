package io.activej.cube.aggregation;

import io.activej.cube.etcd.EtcdChunkIdGenerator;
import io.activej.cube.ot.sql.SqlAtomicSequence;
import io.activej.cube.ot.sql.SqlAtomicSequenceTest;
import io.activej.cube.ot.sql.SqlChunkIdGenerator;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.test.rules.DescriptionRule;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.sql.DataSource;
import java.io.IOException;
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
import static io.activej.cube.TestUtils.dataSource;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("resource")
@RunWith(Parameterized.class)
public class ChunkIdGeneratorTest {

	@Rule
	public final DescriptionRule descriptionRule = new DescriptionRule();

	@Parameter()
	public String testName;

	@Parameter(1)
	public ChunkIdGeneratorFactory chunkIdGeneratorFactory;

	public static final int N_THREADS = 10;
	public static final int N_CHUNKS = 10_000;

	private List<Thread> threads;
	private List<Reactor> reactors;
	private List<ChunkIdGenerator> chunkIdGenerators;

	private Map<Integer, List<Long>> idsMap;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return List.of(
			new Object[]{"MySql chunk ID generator", new ChunkIdGeneratorFactory() {
				private DataSource dataSource;
				private SqlAtomicSequence sequence;

				@Override
				public void initialize(Description description) throws SQLException, IOException {
					dataSource = dataSource("test.properties");
					Class<SqlAtomicSequenceTest> sqlAtomicSequenceTestClass = SqlAtomicSequenceTest.class;
					executeScript(dataSource, sqlAtomicSequenceTestClass.getPackage().getName() + "/" + sqlAtomicSequenceTestClass.getSimpleName() + ".sql");
					sequence = SqlAtomicSequence.ofLastInsertID("sequence", "next");
				}

				@Override
				public ChunkIdGenerator create(Reactor reactor, Description description) {
					return SqlChunkIdGenerator.create(reactor, Executors.newSingleThreadExecutor(), dataSource, sequence);
				}
			}},
			new Object[]{"etcd chunk ID generator", new ChunkIdGeneratorFactory() {
				@Override
				public void initialize(Description description) throws ExecutionException, InterruptedException {
					createEtcdClient().getKVClient().put(createKey(description), byteSequenceFrom("1")).get();
				}

				@Override
				public ChunkIdGenerator create(Reactor reactor, Description description) {
					Client etcdClient = createEtcdClient();
					return EtcdChunkIdGenerator.create(reactor, etcdClient.getKVClient(), createKey(description));
				}

				private static ByteSequence createKey(Description description) {
					return byteSequenceFrom("test." + description.getClassName() + "#" + description.getMethodName());
				}

				private static Client createEtcdClient() {
					return Client.builder().waitForReady(false).endpoints("http://127.0.0.1:2379").build();
				}
			}}
		);
	}

	@Before
	public void before() throws Exception {
		Description description = descriptionRule.getDescription();
		chunkIdGeneratorFactory.initialize(description);

		threads = new ArrayList<>(N_THREADS);
		reactors = new ArrayList<>(N_THREADS);
		chunkIdGenerators = new ArrayList<>(N_THREADS);

		for (int i = 0; i < N_THREADS; i++) {
			Eventloop eventloop = Eventloop.create();
			threads.add(new Thread(eventloop));
			chunkIdGenerators.add(chunkIdGeneratorFactory.create(eventloop, description));
			reactors.add(eventloop);
		}

		idsMap = new ConcurrentHashMap<>();
	}

	@Test
	public void test10Iterations() throws ExecutionException, InterruptedException {
		doTest(10);
	}

	@Test
	@Ignore("Takes too long")
	public void test100Iterations() throws ExecutionException, InterruptedException {
		doTest(100);
	}

	private void doTest(int numberOfIterations) throws InterruptedException, ExecutionException {
		CompletableFuture<?>[] futures = new CompletableFuture[N_THREADS];
		for (int i = 0; i < N_THREADS; i++) {
			Reactor reactor = reactors.get(i);
			ChunkIdGenerator idGenerator = chunkIdGenerators.get(i);
			int finalI = i;
			futures[i] = reactor.submit(() -> Promises.until(
				0,
				j -> Promises.toList(IntStream.range(0, N_CHUNKS)
						.mapToObj($ -> idGenerator.createProtoChunkId()))
					.map(Set::copyOf)
					.then(idGenerator::convertToActualChunkIds)
					.map(chunkIds -> {
						idsMap.computeIfAbsent(finalI, $ -> new ArrayList<>()).addAll(chunkIds.values());
						return j + 1;
					}),
				j -> j == numberOfIterations));
		}

		for (Thread thread : threads) {
			thread.start();
		}

		CompletableFuture.allOf(futures).get();

		assertEquals(N_THREADS, idsMap.size());

		long expectedSize = (long) N_THREADS * N_CHUNKS * numberOfIterations;
		assertEquals(expectedSize, idsMap.values().stream().mapToLong(Collection::size).sum());

		Set<Long> expectedIds = LongStream.range(1, expectedSize + 1).boxed().collect(Collectors.toSet());
		Set<Long> actualIds = idsMap.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());

		assertEquals(expectedIds, actualIds);
	}

	public interface ChunkIdGeneratorFactory {
		void initialize(Description description) throws Exception;

		ChunkIdGenerator create(Reactor reactor, Description description);
	}
}
