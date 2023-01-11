package io.activej.ot.uplink;

import io.activej.ot.OTCommit;
import io.activej.ot.exception.GraphExhaustedException;
import io.activej.ot.uplink.AsyncOTUplink.FetchData;
import io.activej.ot.utils.OTGraphBuilder;
import io.activej.ot.utils.OTRepository_Stub;
import io.activej.ot.utils.OTState_TestOp;
import io.activej.ot.utils.TestOp;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static io.activej.ot.OTCommit.ofRoot;
import static io.activej.ot.utils.Utils.add;
import static io.activej.ot.utils.Utils.createTestOp;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

public class OTUplink_Reactive_Test {
	private static final OTState_TestOp state = new OTState_TestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private final OTRepository_Stub<Integer, TestOp> REPOSITORY = OTRepository_Stub.create();

	private AsyncOTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> node;

	@Before
	public void setUp() {
		REPOSITORY.reset();
		node = OTUplink_Reactive.create(REPOSITORY, createTestOp());
		resetRepo(null);

	}

	@Test
	public void testFetchLinearGraph() {
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));
			g.add(3, 4, add(4));
			g.add(4, 5, add(5));
			g.add(5, 6, add(6));
		});

		FetchData<Integer, TestOp> fetchData1 = await(node.fetch(0));
		assertFetchData(6, 7, 21, fetchData1);

		FetchData<Integer, TestOp> fetchData2 = await(node.fetch(3));
		assertFetchData(6, 7, 15, fetchData2);
	}

	@Test
	public void testFetch2BranchesGraph() {
		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));
		});

		FetchData<Integer, TestOp> fetchData1 = await(node.fetch(0));
		assertFetchData(3, 4, 6, fetchData1);

		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));
		});

		FetchData<Integer, TestOp> fetchData2 = await(node.fetch(1));
		assertFetchData(3, 4, 5, fetchData2);

		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));
		});

		FetchData<Integer, TestOp> fetchData3 = await(node.fetch(4));
		assertFetchData(5, 3, 5, fetchData3);
	}

	@Test
	public void testFetchSplittingGraph() {
		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));

			g.add(3, 6, add(9));
			g.add(5, 6, add(6));

			g.add(6, 7, add(7));
		});

		FetchData<Integer, TestOp> fetchData1 = await(node.fetch(0));
		assertFetchData(7, 6, 22, fetchData1);

		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));

			g.add(3, 6, add(9));
			g.add(5, 6, add(6));

			g.add(6, 7, add(7));
		});

		FetchData<Integer, TestOp> fetchData2 = await(node.fetch(1));
		assertFetchData(7, 6, 21, fetchData2);

		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));

			g.add(3, 6, add(9));
			g.add(5, 6, add(6));

			g.add(6, 7, add(7));
		});

		FetchData<Integer, TestOp> fetchData3 = await(node.fetch(4));
		assertFetchData(7, 6, 18, fetchData3);
	}

	@Test
	public void testFetchInvalidRevision() {
		Exception exception = awaitException(node.fetch(100));
		assertThat(exception, instanceOf(GraphExhaustedException.class));
	}

	@Test
	public void testCheckoutEmptyGraph() {
		FetchData<Integer, TestOp> fetchData = await(node.checkout());
		assertFetchData(0, 1, 0, fetchData);
	}

	@Test
	public void testCheckoutLinearGraph() {
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));
			g.add(3, 4, add(4));
			g.add(4, 5, add(5));
			g.add(5, 6, add(6));
		});

		FetchData<Integer, TestOp> fetchData = await(node.checkout());
		assertFetchData(6, 7, 21, fetchData);
	}

/*
	@Test
	public void testCheckout2BranchesGraph() {
		REPOSITORY.revisionIdSupplier = () -> 6; // id of merge commit
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));
		});

		FetchData<Integer, TestOp> fetchData = await(node.checkout());
		assertFetchData(6, 5, 15, fetchData);

		// Additional snapshot in branch1
		REPOSITORY.saveSnapshot(4, singletonList(add(4)));

		FetchData<Integer, TestOp> fetchData2 = await(node.checkout());
		assertFetchData(6, 5, 15, fetchData2);

		// Additional snapshot in branch2
		REPOSITORY.saveSnapshot(1, singletonList(add(1)));

		FetchData<Integer, TestOp> fetchData3 = await(node.checkout());
		assertFetchData(6, 5, 15, fetchData3);

	}
*/

	private static void assertFetchData(Integer expectedId, long expectedLevel, Integer expectedState, FetchData<Integer, TestOp> fetchData) {
		assertEquals(expectedId, fetchData.getCommitId());
		assertEquals(expectedLevel, fetchData.getLevel());
		state.init();
		fetchData.getDiffs().forEach(state::apply);
		assertEquals(expectedState, (Integer) state.getValue());
	}

	private void resetRepo(@Nullable Consumer<OTGraphBuilder<Integer, TestOp>> builder) {
		// Initializing repo
		REPOSITORY.reset();
		REPOSITORY.doPushAndUpdateHeads(Set.of(ofRoot(0)));
		await(REPOSITORY.saveSnapshot(0, List.of()));

		if (builder != null) {
			REPOSITORY.setGraph(builder);
		}
	}

}
