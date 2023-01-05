package io.activej.ot;

import io.activej.async.function.AsyncSupplier;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.AsyncOTUplink;
import io.activej.ot.uplink.OTUplink;
import io.activej.ot.utils.OTRepositoryStub;
import io.activej.ot.utils.TestOp;
import io.activej.ot.utils.TestOpState;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static io.activej.ot.OTCommit.ofCommit;
import static io.activej.ot.OTCommit.ofRoot;
import static io.activej.ot.utils.Utils.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static org.junit.Assert.*;

public class OTStateManagerTest {
	private static final ExpectedException FAILED = new ExpectedException();
	private static final OTSystem<TestOp> SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private OTRepositoryStub<Integer, TestOp> repository;
	private OTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink;
	private OTStateManager<Integer, TestOp> stateManager;
	private TestOpState testOpState;
	private boolean alreadyFailed = false;

	@Before
	public void before() {
		Random random = new Random();
		repository = OTRepositoryStub.create();
		repository.revisionIdSupplier = () -> random.nextInt(1000) + 1000;
		testOpState = new TestOpState();
		uplink = OTUplink.create(this.repository, SYSTEM);
		stateManager = OTStateManager.create(getCurrentReactor(), SYSTEM, uplink, testOpState);

		initializeRepository(this.repository, stateManager);
	}

	@Test
	public void testSyncBeforeSyncFinished() {
		repository.revisionIdSupplier = () -> 2;
		AsyncOTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink = new OTUplinkDecorator(OTStateManagerTest.this.uplink) {
			@Override
			public Promise<FetchData<Integer, TestOp>> fetch(Integer currentCommitId) {
				return super.fetch(currentCommitId)
						.then(fetchData -> Promises.delay(100L, fetchData));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = OTStateManager.create(getCurrentReactor(), SYSTEM, uplink, testOpState);

		initializeRepository(repository, stateManager);
		stateManager.add(add(1));
		stateManager.sync();
		await(stateManager.sync());

		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());
		assertEquals((Integer) 2, stateManager.getCommitId());
		assertEquals(1, testOpState.getValue());
	}

	@Test
	public void testSyncFullHistory() {
		for (int i = 1; i <= 5; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());

		await(stateManager.sync());
		assertEquals(5, testOpState.getValue());
	}

	@Test
	public void testApplyDiffBeforeSync() {
		repository.revisionIdSupplier = () -> 11;
		for (int i = 1; i <= 10; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());
		stateManager.add(add(1));
		assertEquals(1, testOpState.getValue());

		await(stateManager.sync());
		assertEquals(11, testOpState.getValue());
	}

	@Test
	public void testMultipleSyncs() {
		for (int i = 1; i <= 20; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
			if (i == 5 || i == 15) {
				await(stateManager.sync());
			}
			if (i == 10 || i == 20) {
				await(stateManager.sync());
			}
		}

		assertEquals(20, testOpState.getValue());
	}

	@Test
	public void testMultibinders() {
		repository.addGraph(g -> g.add(0, 1, List.of(add(5))));

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(3));
		await(stateManager.sync());

		assertEquals(8, testOpState.getValue());
	}

	@Test
	public void testMultibinders2() {
		repository.addGraph(g -> g.add(0, 1, set(0, 15)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		await(stateManager.sync());

		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testMultibinders3() {
		repository.addGraph(g -> g.add(0, 1, set(0, 10)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		await(stateManager.sync());

		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testMultibinders4() {
		repository.addGraph(g -> g.add(0, 1, add(5)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		await(stateManager.sync());

		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testMultibinders5() {
		repository.addGraph(g -> g.add(0, 1, add(10)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		await(stateManager.sync());

		assertEquals(15, testOpState.getValue());
	}

	@Test
	public void testSyncAfterFailedCommit() {
		repository.revisionIdSupplier = () -> 1;
		AsyncOTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink = new OTUplinkDecorator(OTStateManagerTest.this.uplink) {
			@Override
			public Promise<OTCommit<Integer, TestOp>> createProtoCommit(Integer parent, List<TestOp> diffs, long parentLevel) {
				return failOnce(() -> super.createProtoCommit(parent, diffs, parentLevel));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = OTStateManager.create(getCurrentReactor(), SYSTEM, uplink, testOpState);
		initializeRepository(repository, stateManager);

		stateManager.add(add(1));
		Exception exception = awaitException(stateManager.sync());

		assertEquals(FAILED, exception);
		assertEquals((Integer) 0, stateManager.getCommitId());
		assertFalse(stateManager.hasPendingCommits());
		assertTrue(stateManager.hasWorkingDiffs());

		// new ops added in the meantime
		stateManager.add(add(100));

		await(stateManager.sync());
		assertEquals((Integer) 1, stateManager.getCommitId());
		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(Set.of(1), heads);
		assertEquals(101, testOpState.getValue());
	}

	@Test
	public void testSyncAfterFailedPull() {
		repository.revisionIdSupplier = () -> 3;
		AsyncOTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink = new OTUplinkDecorator(OTStateManagerTest.this.uplink) {
			@Override
			public Promise<FetchData<Integer, TestOp>> fetch(Integer currentCommitId) {
				return failOnce(() -> super.fetch(currentCommitId));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = OTStateManager.create(getCurrentReactor(), SYSTEM, uplink, testOpState);
		initializeRepository(repository, stateManager);
		repository.setGraph(g -> {
			g.add(0, 1, add(10));
			g.add(1, 2, add(20));
		});

		stateManager.add(add(1));
		Exception exception = awaitException(stateManager.sync());

		assertEquals(FAILED, exception);
		assertEquals((Integer) 0, stateManager.getCommitId());
		assertFalse(stateManager.hasPendingCommits());
		assertTrue(stateManager.hasWorkingDiffs());

		// new ops added in the meantime
		stateManager.add(add(100));

		await(stateManager.sync());
		assertEquals((Integer) 3, stateManager.getCommitId());
		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(Set.of(3), heads);
		assertEquals(131, testOpState.getValue());
	}

	@Test
	public void testSyncAfterFailedPush() {
		repository.revisionIdSupplier = List.of(3, 4, 5).iterator()::next;
		AsyncOTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink = new OTUplinkDecorator(OTStateManagerTest.this.uplink) {
			@Override
			public Promise<FetchData<Integer, TestOp>> push(OTCommit<Integer, TestOp> protoCommit) {
				return failOnce(() -> super.push(protoCommit));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = OTStateManager.create(getCurrentReactor(), SYSTEM, uplink, testOpState);
		initializeRepository(repository, stateManager);

		stateManager.add(add(1));
		Exception exception = awaitException(stateManager.sync());

		assertEquals(FAILED, exception);
		assertEquals((Integer) 0, stateManager.getCommitId());
		assertTrue(stateManager.hasPendingCommits());
		assertFalse(stateManager.hasWorkingDiffs());

		// new ops added in the meantime, repo changed
		stateManager.add(add(100));
		repository.setGraph(g -> {
			g.add(0, 1, add(10));
			g.add(1, 2, add(20));

		});

		await(stateManager.sync());
		assertEquals((Integer) 5, stateManager.getCommitId());
		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(Set.of(5), heads);
		assertEquals(131, testOpState.getValue());
	}

	@Test
	public void fetchSimple() {
		for (int i = 1; i <= 3; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
		}
		await(stateManager.sync());

		assertFalse(await(stateManager.fetch()));

		assertEquals(3, testOpState.getValue());
		assertEquals(Integer.valueOf(3), stateManager.getCommitId());
		assertSame(stateManager.getCommitId(), stateManager.getOriginCommitId());

		for (int i = 4; i <= 10; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
		}
		assertTrue(await(stateManager.fetch()));

		assertEquals(3, testOpState.getValue());
		assertEquals(Integer.valueOf(3), stateManager.getCommitId());
		assertNotSame(stateManager.getCommitId(), stateManager.getOriginCommitId());
		assertEquals(Integer.valueOf(10), stateManager.getOriginCommitId());

		await(stateManager.sync());
		assertEquals(10, testOpState.getValue());
		assertEquals(Integer.valueOf(10), stateManager.getCommitId());
		assertSame(stateManager.getCommitId(), stateManager.getOriginCommitId());
	}

	@Test
	public void fetchWithWorkingDiffs() {
		repository.revisionIdSupplier = List.of(11, 16).iterator()::next;
		for (int i = 1; i <= 3; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
		}
		await(stateManager.sync());

		assertEquals(3, testOpState.getValue());
		assertEquals(Integer.valueOf(3), stateManager.getCommitId());
		assertSame(stateManager.getCommitId(), stateManager.getOriginCommitId());

		for (int i = 4; i <= 10; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
		}
		stateManager.add(add(-5));
		assertEquals(-2, testOpState.getValue());
		assertTrue(await(stateManager.fetch()));

		assertEquals(-2, testOpState.getValue());
		assertEquals(Integer.valueOf(3), stateManager.getCommitId());
		assertNotSame(stateManager.getCommitId(), stateManager.getOriginCommitId());
		assertEquals(Integer.valueOf(10), stateManager.getOriginCommitId());

		await(stateManager.sync());
		assertEquals(5, testOpState.getValue());
		assertEquals(Integer.valueOf(11), stateManager.getCommitId());
		assertSame(stateManager.getCommitId(), stateManager.getOriginCommitId());

		for (int i = 12; i <= 15; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
		}
		assertTrue(await(stateManager.fetch()));

		assertEquals(5, testOpState.getValue());
		assertEquals(Integer.valueOf(11), stateManager.getCommitId());
		assertNotSame(stateManager.getCommitId(), stateManager.getOriginCommitId());
		assertEquals(Integer.valueOf(15), stateManager.getOriginCommitId());

		stateManager.add(add(-3));
		assertEquals(2, testOpState.getValue());

		await(stateManager.sync());

		assertEquals(6, testOpState.getValue());
		assertEquals(Integer.valueOf(16), stateManager.getCommitId());
		assertSame(stateManager.getCommitId(), stateManager.getOriginCommitId());
	}

	@Test
	public void fetchWithBranching() {
		repository.revisionIdSupplier = List.of(17, 18).iterator()::next;
		for (int i = 1; i <= 3; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
		}
		await(stateManager.sync());

		assertEquals(3, testOpState.getValue());
		assertEquals(Integer.valueOf(3), stateManager.getCommitId());
		assertSame(stateManager.getCommitId(), stateManager.getOriginCommitId());

		stateManager.add(add(100));
		assertEquals(103, testOpState.getValue());

		for (int i = 4; i <= 10; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(1)), i + 1L));
		}

		assertTrue(await(stateManager.fetch()));
		assertEquals(Integer.valueOf(10), stateManager.getOriginCommitId());

		repository.doPushAndUpdateHead(ofCommit(0, 11, 3, List.of(add(10)), 5));
		for (int i = 12; i <= 16; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(10)), i - 6));
		}
		assertFalse(await(stateManager.fetch()));
		assertEquals(Integer.valueOf(10), stateManager.getOriginCommitId());

		await(stateManager.sync());

		assertEquals(170, testOpState.getValue());
		assertEquals(Integer.valueOf(18), stateManager.getCommitId());
		assertSame(stateManager.getCommitId(), stateManager.getOriginCommitId());
	}

	@Test
	public void fetchWithPendingCommit() {
		repository.revisionIdSupplier = List.of(1, 6).iterator()::next;
		AsyncOTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink = new OTUplinkDecorator(OTStateManagerTest.this.uplink) {
			@Override
			public Promise<FetchData<Integer, TestOp>> push(OTCommit<Integer, TestOp> protoCommit) {
				return failOnce(() -> super.push(protoCommit));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = OTStateManager.create(getCurrentReactor(), SYSTEM, uplink, testOpState);
		initializeRepository(repository, stateManager);

		stateManager.add(add(1));
		Exception exception = awaitException(stateManager.sync());
		assertSame(FAILED, exception);
		assertTrue(stateManager.hasPendingCommits());

		assertFalse(await(stateManager.fetch()));

		repository.doPushAndUpdateHead(ofCommit(0, 2, 0, List.of(add(10)), 2));
		for (int i = 3; i <= 5; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, List.of(add(10)), i));
		}
		assertFalse(await(stateManager.fetch()));

		assertSame(stateManager.getCommitId(), stateManager.getOriginCommitId());

		await(stateManager.sync());
		assertFalse(await(stateManager.fetch()));

		assertEquals(Integer.valueOf(6), stateManager.getCommitId());
		assertEquals(41, testOpState.getValue());
	}

	static class OTUplinkDecorator implements AsyncOTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> {
		private final AsyncOTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> node;

		OTUplinkDecorator(AsyncOTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> node) {
			this.node = node;
		}

		@Override
		public Promise<OTCommit<Integer, TestOp>> createProtoCommit(Integer parent, List<TestOp> diffs, long parentLevel) {
			return node.createProtoCommit(parent, diffs, parentLevel);
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> push(OTCommit<Integer, TestOp> protoCommit) {
			return node.push(protoCommit);
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> checkout() {
			return node.checkout();
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> fetch(Integer currentCommitId) {
			return node.fetch(currentCommitId);
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> poll(Integer currentCommitId) {
			return node.poll(currentCommitId);
		}
	}

	private <T> Promise<T> failOnce(AsyncSupplier<T> supplier) {
		if (alreadyFailed) {
			return supplier.get();
		} else {
			alreadyFailed = true;
			return Promise.ofException(FAILED);
		}
	}

	private void initializeRepository(AsyncOTRepository<Integer, TestOp> repository, OTStateManager<Integer, TestOp> stateManager) {
		await(repository.pushAndUpdateHead(ofRoot(0)), repository.saveSnapshot(0, List.of()));
		await(stateManager.checkout());
	}

}
