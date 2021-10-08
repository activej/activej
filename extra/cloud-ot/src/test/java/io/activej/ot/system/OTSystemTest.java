package io.activej.ot.system;

import io.activej.ot.OTCommit;
import io.activej.ot.OTStateManager;
import io.activej.ot.TransformResult;
import io.activej.ot.repository.OTRepository;
import io.activej.ot.uplink.OTUplinkImpl;
import io.activej.ot.utils.OTRepositoryStub;
import io.activej.ot.utils.TestAdd;
import io.activej.ot.utils.TestOp;
import io.activej.ot.utils.TestOpState;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.ot.OTAlgorithms.mergeAndPush;
import static io.activej.ot.OTAlgorithms.mergeAndUpdateHeads;
import static io.activej.ot.utils.Utils.*;
import static io.activej.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument"})
public final class OTSystemTest {
	private static final OTSystem<TestOp> SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testTransform1() throws Exception {
		List<? extends TestOp> left = asList(add(2), add(1));
		List<? extends TestOp> right = asList(add(1), add(10), add(100));
		TransformResult<TestOp> result = SYSTEM.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);

		assertEquals(right, result.left);
		assertEquals(left, result.right);
	}

	@Test
	public void testTransform2() throws Exception {
		List<? extends TestOp> left = asList(add(2), set(2, 1), add(2), add(10));
		List<? extends TestOp> right = asList(set(0, -20), add(30), add(10));
		TransformResult<TestOp> result = SYSTEM.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);

		assertEquals(asList(set(13, -20), add(30), add(10)), result.left);
		assertTrue(result.right.isEmpty());
	}

	@Test
	public void testSimplify() {
		List<? extends TestOp> arg = asList(add(2), set(2, 1), add(2), add(10));
		List<TestOp> result = SYSTEM.squash(arg);
		System.out.println(result);

		assertEquals(singletonList(set(0, 13)), result);
	}

	@Test
	public void testOtSource2() {
		OTRepositoryStub<String, TestOp> repository = OTRepositoryStub.create(asList("m", "x", "y", "m2"));
		repository.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("a1", "a2", add(2));
			g.add("a2", "a3", add(4));
			g.add("*", "b1", add(10));
			g.add("b1", "b2", add(100));
		});

		TestOpState state = new TestOpState();
		OTUplinkImpl<String, TestOp, OTCommit<String, TestOp>> node = OTUplinkImpl.create(repository, SYSTEM);
		OTStateManager<String, TestOp> stateManager = OTStateManager.create(getCurrentEventloop(), SYSTEM, node, state);

		await(stateManager.checkout());

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();

		await(mergeAndUpdateHeads(repository, SYSTEM));

		System.out.println(await(repository.loadCommit("m")));
		System.out.println(stateManager);
		System.out.println();
		stateManager.add(new TestAdd(50));
		System.out.println(stateManager);

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();
		stateManager.add(new TestAdd(3));
		System.out.println(stateManager);

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();
		System.out.println(repository);
		System.out.println(stateManager);

		await(stateManager.sync());
		System.out.println(repository.loadCommit("x"));
		System.out.println(repository.loadCommit("y"));
		System.out.println(stateManager);
		System.out.println();
		System.out.println(repository);

		await(mergeAndPush(repository, SYSTEM));
		System.out.println(stateManager);
		System.out.println();
	}

	@Test
	public void testOtSource3() {
		OTRepositoryStub<String, TestOp> otSource = OTRepositoryStub.create(asList("m"));
		otSource.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("a1", "a2", add(2));
			g.add("a2", "a3", add(4));
			g.add("a2", "b1", add(10));
		});

		OTUplinkImpl<String, TestOp, OTCommit<String, TestOp>> node = OTUplinkImpl.create(otSource, SYSTEM);
		pullAndThenMergeAndPush(otSource, OTStateManager.create(getCurrentEventloop(), SYSTEM, node, new TestOpState()));
	}

	@Test
	public void testOtSource4() {
		OTRepositoryStub<String, TestOp> otSource = OTRepositoryStub.create(asList("m"));
		otSource.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("*", "b1", add(10));
			g.add("a1", "a2", add(10));
			g.add("b1", "a2", add(1));
			g.add("a1", "b2", add(10));
			g.add("b1", "b2", add(1));
		});

		OTUplinkImpl<String, TestOp, OTCommit<String, TestOp>> node = OTUplinkImpl.create(otSource, SYSTEM);
		pullAndThenMergeAndPush(otSource, OTStateManager.create(getCurrentEventloop(), SYSTEM, node, new TestOpState()));
	}

	private void pullAndThenMergeAndPush(OTRepository<String, TestOp> repository, OTStateManager<String, TestOp> stateManager) {
		await(stateManager.checkout());

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();

		await(mergeAndPush(repository, SYSTEM));
		System.out.println(repository);
		System.out.println(stateManager);
	}
}
