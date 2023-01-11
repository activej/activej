package io.activej.ot.system;

import io.activej.ot.OTCommit;
import io.activej.ot.OTStateManager;
import io.activej.ot.TransformResult;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.ot.uplink.OTUplink_Reactive;
import io.activej.ot.utils.OTRepository_Stub;
import io.activej.ot.utils.OTState_TestOp;
import io.activej.ot.utils.TestAdd;
import io.activej.ot.utils.TestOp;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.ot.OTAlgorithms.mergeAndPush;
import static io.activej.ot.OTAlgorithms.mergeAndUpdateHeads;
import static io.activej.ot.utils.Utils.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class OTSystemTest {
	private static final OTSystem<TestOp> SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testTransform1() throws Exception {
		List<? extends TestOp> left = List.of(add(2), add(1));
		List<? extends TestOp> right = List.of(add(1), add(10), add(100));
		TransformResult<TestOp> result = SYSTEM.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);

		assertEquals(right, result.left);
		assertEquals(left, result.right);
	}

	@Test
	public void testTransform2() throws Exception {
		List<? extends TestOp> left = List.of(add(2), set(2, 1), add(2), add(10));
		List<? extends TestOp> right = List.of(set(0, -20), add(30), add(10));
		TransformResult<TestOp> result = SYSTEM.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);

		assertEquals(List.of(set(13, -20), add(30), add(10)), result.left);
		assertTrue(result.right.isEmpty());
	}

	@Test
	public void testSimplify() {
		List<? extends TestOp> arg = List.of(add(2), set(2, 1), add(2), add(10));
		List<TestOp> result = SYSTEM.squash(arg);
		System.out.println(result);

		assertEquals(List.of(set(0, 13)), result);
	}

	@Test
	public void testOtSource2() {
		OTRepository_Stub<String, TestOp> repository = OTRepository_Stub.create(List.of("m", "x", "y", "m2"));
		repository.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("a1", "a2", add(2));
			g.add("a2", "a3", add(4));
			g.add("*", "b1", add(10));
			g.add("b1", "b2", add(100));
		});

		OTState_TestOp state = new OTState_TestOp();
		OTUplink_Reactive<String, TestOp, OTCommit<String, TestOp>> node = OTUplink_Reactive.create(repository, SYSTEM);
		OTStateManager<String, TestOp> stateManager = OTStateManager.create(getCurrentReactor(), SYSTEM, node, state);

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
		OTRepository_Stub<String, TestOp> otSource = OTRepository_Stub.create(List.of("m"));
		otSource.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("a1", "a2", add(2));
			g.add("a2", "a3", add(4));
			g.add("a2", "b1", add(10));
		});

		OTUplink_Reactive<String, TestOp, OTCommit<String, TestOp>> node = OTUplink_Reactive.create(otSource, SYSTEM);
		pullAndThenMergeAndPush(otSource, OTStateManager.create(getCurrentReactor(), SYSTEM, node, new OTState_TestOp()));
	}

	@Test
	public void testOtSource4() {
		OTRepository_Stub<String, TestOp> otSource = OTRepository_Stub.create(List.of("m"));
		otSource.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("*", "b1", add(10));
			g.add("a1", "a2", add(10));
			g.add("b1", "a2", add(1));
			g.add("a1", "b2", add(10));
			g.add("b1", "b2", add(1));
		});

		OTUplink_Reactive<String, TestOp, OTCommit<String, TestOp>> node = OTUplink_Reactive.create(otSource, SYSTEM);
		pullAndThenMergeAndPush(otSource, OTStateManager.create(getCurrentReactor(), SYSTEM, node, new OTState_TestOp()));
	}

	private void pullAndThenMergeAndPush(AsyncOTRepository<String, TestOp> repository, OTStateManager<String, TestOp> stateManager) {
		await(stateManager.checkout());

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();

		await(mergeAndPush(repository, SYSTEM));
		System.out.println(repository);
		System.out.println(stateManager);
	}
}
