package io.activej.ot.repository;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonWriter;
import io.activej.eventloop.Eventloop;
import io.activej.ot.IdGeneratorStub;
import io.activej.ot.OTCommit;
import io.activej.ot.system.OTSystem;
import io.activej.ot.utils.TestAdd;
import io.activej.ot.utils.TestOp;
import io.activej.ot.utils.TestOpState;
import io.activej.ot.utils.TestSet;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

import static io.activej.common.Utils.first;
import static io.activej.ot.OTAlgorithms.*;
import static io.activej.ot.OTCommit.ofCommit;
import static io.activej.ot.OTCommit.ofRoot;
import static io.activej.ot.utils.Utils.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.dataSource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

@Ignore
public class OTRepositoryMySqlTest {
	private static final OTSystem<TestOp> SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private OTRepositoryMySql<TestOp> repository;
	private IdGeneratorStub idGenerator;

	static {
		Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		rootLogger.setLevel(Level.toLevel("TRACE"));
	}

	@Before
	public void before() throws IOException, SQLException {
		idGenerator = new IdGeneratorStub();
		repository = OTRepositoryMySql.create(Eventloop.getCurrentEventloop(), Executors.newFixedThreadPool(4), dataSource("test.properties"), idGenerator,
				createTestOp(), TestOp.class);
		repository.initialize();
		repository.truncateTables();
	}

	@SafeVarargs
	private static <T> Set<T> set(T... values) {
		return Arrays.stream(values).collect(toSet());
	}

	private static int apply(List<TestOp> testOps) {
		TestOpState testOpState = new TestOpState();
		testOps.forEach(testOpState::apply);
		return testOpState.getValue();
	}

	@Test
	public void testJson() throws IOException {
		{
			TestAdd testAdd = new TestAdd(1);
			String json = toJson(testAdd);
			TestOp testAdd2 = fromJson(json);
			assertEquals(testAdd, testAdd2);
		}

		{
			TestSet testSet = new TestSet(0, 4);
			String json = toJson(testSet);
			TestOp testSet2 = fromJson(json);
			assertEquals(testSet, testSet2);
		}
	}

	private static final DslJson<?> DSL_JSON = new DslJson<>();

	private static String toJson(TestOp op) throws IOException {
		JsonWriter jsonWriter = DSL_JSON.newWriter();
		DSL_JSON.serialize(jsonWriter, op);
		return jsonWriter.toString();
	}

	private static TestOp fromJson(String json) throws IOException {
		byte[] bytes = json.getBytes(UTF_8);
		return DSL_JSON.deserialize(TestOp.class, bytes, bytes.length);
	}

	@Test
	public void testRootHeads() {
		Long id = await(repository.createCommitId());
		await(repository.pushAndUpdateHead(ofRoot(id)));

		Set<Long> heads = await(repository.getHeads());
		assertEquals(1, heads.size());
		assertEquals(1, first(heads).intValue());
	}

	@Test
	public void testReplaceHead() {
		Long rootId = await(repository.createCommitId());
		await(repository.pushAndUpdateHead(ofRoot(rootId)));

		Long id = await(repository.createCommitId());

		await(repository.pushAndUpdateHead(ofCommit(0, id, rootId, List.of(new TestSet(0, 5)), id)));

		Set<Long> heads = await(repository.getHeads());
		assertEquals(1, heads.size());
		assertEquals(2, first(heads).intValue());
	}

	@Test
	public void testReplaceHeadsOnMerge() {
		/*
		        / firstId  \
		       /            \
		rootId -- secondId   -- mergeId(HEAD)
		       \            /
		        \ thirdId  /

		 */

		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(1, 3, add(1));
					g.add(1, 4, add(1));
				}))));
		idGenerator.set(4);

		Set<Long> heads = await(repository.getHeads());
		assertEquals(3, heads.size());
		assertEquals(set(2L, 3L, 4L), heads);

		Long mergeId = await(mergeAndUpdateHeads(repository, SYSTEM));

		Set<Long> headsAfterMerge = await(repository.getHeads());
		assertEquals(1, headsAfterMerge.size());
		assertEquals(mergeId, first(headsAfterMerge));
	}

	//	@Test
//	public void testMergeSnapshotOnMergeNodes() throws ExecutionException, InterruptedException {
//		final GraphBuilder<Long, TestOp> graphBuilder = new GraphBuilder<>(repository);
//		final CompletableFuture<Void> graphFuture = graphBuilder.buildGraph(List.of(
//				edge(1, 2, add(1)),
//				edge(1, 3, add(1)),
//				edge(1, 4, add(1)),
//				edge(1, 5, add(1)),
//				edge(2, 6, add(1)),
//				edge(3, 6, add(1)),
//				edge(4, 7, add(1)),
//				edge(5, 7, add(1)),
//				edge(6, 8, add(1)),
//				edge(7, 9, add(1))))
//				.toCompletableFuture();
//
//		eventloop.run();
//		graphFuture.get();
//
//		final CompletableFuture<Long> beforeVirtualЩFutureNode4 = OTAlgorithms
//				.loadAllChanges(repository, keyComparator, otSystem, 6)
//				.map(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		final CompletableFuture<Long> beforeVirtualFutureNode5 = OTAlgorithms
//				.loadAllChanges(repository, keyComparator, otSystem, 8)
//				.map(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		eventloop.run();
//
//		final Long node4Before = beforeVirtualFutureNode4.get();
//		final Long node5Before = beforeVirtualFutureNode5.get();
//		assertEquals(2, node4Before.intValue());
//		assertEquals(3, node5Before.intValue());
//
//		final CompletableFuture<?> virtualFuture = OTAlgorithms
//				.saveCheckpoint(repository, keyComparator, otSystem, newHashSet(6, 7))
//				.toCompletableFuture();
//		eventloop.run();
//		virtualFuture.get();
//
//		final CompletableFuture<Long> afterVirtualFutureNode4 = OTAlgorithms
//				.loadAllChanges(repository, keyComparator, otSystem, 6)
//				.map(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		final CompletableFuture<Long> afterVirtualFutureNode5 = OTAlgorithms
//				.loadAllChanges(repository, keyComparator, otSystem, 8)
//				.map(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		eventloop.run();
//		final Long node4After = afterVirtualFutureNode4.get();
//		final Long node5After = afterVirtualFutureNode5.get();
//
//		assertEquals(node5Before, node5After);
//		assertEquals(node4Before, node4After);
//
//		OTAlgorithms.mergeHeadsAndPush(otSystem, repository, Long::compareTo);
//		// logs
//		eventloop.run();
//	}

	@Test
	public void testForkMerge() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(2, 3, add(1));
					g.add(3, 4, add(1));
					g.add(4, 5, add(1));
					g.add(4, 6, add(1));
					g.add(5, 7, add(1));
					g.add(6, 8, add(1));
				}))));
		idGenerator.set(8);

		await(mergeAndUpdateHeads(repository, SYSTEM));
		//		assertEquals(searchSurface, rootNodesFuture.get());
	}

	@Test
	public void testFindRootNodes() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(1, 3, add(1));
					g.add(2, 4, add(1));
					g.add(3, 4, add(1));
					g.add(2, 5, add(1));
					g.add(3, 5, add(1));
					g.add(4, 6, add(1));
					g.add(5, 7, add(1));
				}))));

		assertEquals(set(2L, 3L), await(findAllCommonParents(repository, SYSTEM, set(6L, 7L))));
		assertEquals(set(6L), await(findAllCommonParents(repository, SYSTEM, set(6L))));
	}

	@Test
	public void testFindRootNodes2() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(2, 3, add(1));
					g.add(3, 4, add(1));
					g.add(4, 5, add(1));
					g.add(4, 6, add(1));
				}))));

		assertEquals(set(4L), await(findAllCommonParents(repository, SYSTEM, set(4L, 5L, 6L))));
	}

	@Test
	public void testFindParentCandidatesSurface() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(1, 3, add(1));
					g.add(2, 4, add(1));
					g.add(3, 4, add(1));
					g.add(2, 5, add(1));
					g.add(3, 5, add(1));
					g.add(4, 6, add(1));
					g.add(5, 7, add(1));
				}))));

		Set<Long> searchSurface = set(2L, 3L);

		Set<Long> rootNodes = await(findCut(repository, SYSTEM, set(6L, 7L),
				commits -> searchSurface.equals(commits.stream().map(OTCommit::getId).collect(toSet()))));

		assertEquals(searchSurface, rootNodes);
	}

	@Test
	public void testSingleCacheCheckpointNode() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(2, 3, add(1));
					g.add(3, 4, add(1));
					g.add(4, 5, add(1));
					g.add(5, 6, add(1));
					g.add(5, 7, add(1));
				}))));
		await(repository.saveSnapshot(1L, List.of()));

		List<TestOp> diffs = await(checkout(repository, SYSTEM, 5L));

		await(repository.saveSnapshot(5L, diffs));
		await(repository.cleanup(5L));

		int result = apply(await(checkout(repository, SYSTEM, 7L)));
		assertEquals(5, result);
	}
}
