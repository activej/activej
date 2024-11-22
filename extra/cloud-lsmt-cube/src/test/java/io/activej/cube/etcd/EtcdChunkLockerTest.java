package io.activej.cube.etcd;

import io.activej.cube.aggregation.ChunksAlreadyLockedException;
import io.activej.reactor.Reactor;
import io.activej.test.rules.DescriptionRule;
import io.activej.test.rules.EventloopRule;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.activej.common.collection.CollectionUtils.union;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EtcdChunkLockerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public final DescriptionRule descriptionRule = new DescriptionRule();

	public static final Client ETCD_CLIENT = Client.builder().waitForReady(false).endpoints("http://127.0.0.1:2379").build();

	private EtcdChunkLocker lockerA;
	private EtcdChunkLocker lockerB;

	@Before
	public void before() throws IOException, SQLException, ExecutionException, InterruptedException {
		Reactor reactor = Reactor.getCurrentReactor();
		Description description = descriptionRule.getDescription();
		ByteSequence root = byteSequenceFrom("test." + description.getClassName() + "#" + description.getMethodName());

		lockerA = EtcdChunkLocker.create(reactor, ETCD_CLIENT, root);
		lockerB = EtcdChunkLocker.create(reactor, ETCD_CLIENT, root);

		lockerA.delete();
	}

	@Test
	public void lock() {
		assertTrue(await(lockerA.getLockedChunks()).isEmpty());
		assertTrue(await(lockerB.getLockedChunks()).isEmpty());

		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));
		assertEquals(lockedByA, await(lockerB.getLockedChunks()));

		Set<Long> lockedByB = Set.of(4L, 5L, 6L);
		await(lockerB.lockChunks(lockedByB));

		Set<Long> lockedByBoth = union(lockedByA, lockedByB);

		assertEquals(lockedByBoth, await(lockerA.getLockedChunks()));
		assertEquals(lockedByBoth, await(lockerB.getLockedChunks()));
	}

	@Test
	public void release() {
		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));
		Set<Long> lockedByB = Set.of(4L, 5L, 6L);
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
		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		await(lockerA.lockChunks(lockedByA));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));
		assertEquals(lockedByA, await(lockerB.getLockedChunks()));

		Set<Long> lockedByB = Set.of(4L, 5L, 6L, 1L);

		Exception exception = awaitException(lockerB.lockChunks(lockedByB));
		assertThat(exception, instanceOf(ChunksAlreadyLockedException.class));

		assertEquals(lockedByA, await(lockerA.getLockedChunks()));
		assertEquals(lockedByA, await(lockerB.getLockedChunks()));
	}

	@Test
	public void releaseChunksLockedByOtherShouldNotRelease() {
		Set<Long> lockedByA = Set.of(1L, 2L, 3L);
		Set<Long> lockedByB = Set.of(4L, 5L, 6L);
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
	public void releasePartiallyShouldRelease() {
		Set<Long> lockedByA1 = Set.of(1L, 2L);
		await(lockerA.lockChunks(lockedByA1));
		assertEquals(lockedByA1, await(lockerA.getLockedChunks()));

		Set<Long> lockedByA2 = Set.of(3L, 4L);
		await(lockerA.lockChunks(lockedByA2));
		assertEquals(union(lockedByA1, lockedByA2), await(lockerA.getLockedChunks()));

		Set<Long> lockedByA3 = Set.of(5L, 6L);
		await(lockerA.lockChunks(lockedByA3));
		assertEquals(union(union(lockedByA1, lockedByA2), lockedByA3), await(lockerA.getLockedChunks()));

		await(lockerA.releaseChunks(Set.of(1L, 2L, 3L)));

		assertEquals(Set.of(4L, 5L, 6L), await(lockerA.getLockedChunks()));

		await(lockerA.releaseChunks(Set.of(4L)));
		assertEquals(Set.of(5L, 6L), await(lockerA.getLockedChunks()));

		await(lockerA.releaseChunks(Set.of(5L, 6L)));
		assertTrue(await(lockerA.getLockedChunks()).isEmpty());

	}

}
