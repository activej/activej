package io.activej.ot;

import io.activej.common.tuple.Tuple3;
import io.activej.ot.exception.TransformException;
import io.activej.ot.system.OTSystem;
import io.activej.ot.system.OTSystemImpl;
import io.activej.ot.utils.TestAdd;
import io.activej.ot.utils.TestSet;
import io.activej.ot.utils.TestSetName;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.util.List;

import static io.activej.common.Utils.concat;
import static io.activej.ot.TransformResult.left;
import static io.activej.ot.TransformResult.right;
import static io.activej.ot.system.MergedOTSystem.mergeOtSystems;
import static io.activej.ot.utils.TestSetName.setName;
import static io.activej.ot.utils.Utils.add;
import static io.activej.ot.utils.Utils.set;
import static org.junit.Assert.*;

public final class MergedOTSystemTest {
	private static final OTSystem<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> MERGED = mergeOtSystems(Tuple3::new,
			Tuple3::value1, createAddIntSystem(),
			Tuple3::value2, createTestSetSystem(),
			Tuple3::value3, createTestSetNameSystem());

	@Test
	public void testIsEmpty() {
		assertTrue(MERGED.isEmpty(ops(null, null, null)));
		assertTrue(MERGED.isEmpty(ops(add(0), null, null)));
		assertTrue(MERGED.isEmpty(ops(null, set(12, 12), null)));
		assertTrue(MERGED.isEmpty(ops(null, null, setName("test", "test"))));
		assertTrue(MERGED.isEmpty(ops(add(0), set(12, 12), setName("test", "test"))));

		assertFalse(MERGED.isEmpty(ops(add(1), null, null)));
		assertFalse(MERGED.isEmpty(ops(null, set(12, 13), null)));
		assertFalse(MERGED.isEmpty(ops(null, null, setName("test", "test1"))));
		assertFalse(MERGED.isEmpty(ops(add(1), set(12, 12), setName("test", "test"))));
		assertFalse(MERGED.isEmpty(ops(add(0), set(12, 13), setName("test", "test"))));
		assertFalse(MERGED.isEmpty(ops(add(0), set(12, 12), setName("test", "test1"))));
		assertFalse(MERGED.isEmpty(ops(add(1), set(12, 13), setName("test", "test1"))));
	}

	@Test
	public void testInvert() {
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> ops = List.of(
				ops(add(1), null, null),
				ops(null, set(0, 3), null),
				ops(null, null, setName("", "test1")),
				ops(null, null, null),
				ops(add(12), set(3, 12), setName("test1", "test2")),
				ops(add(-5), set(12, 123), setName("test2", "test3"))
		);
		State state = new State();
		state.init();
		ops.forEach(state::apply);
		assertState("test3", 8, 123, state);

		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> inverted = MERGED.invert(ops);
		System.out.println(inverted);
		inverted.forEach(state::apply);
		assertTrue(state.isEmpty());
	}

	@Test
	public void testSquash() {
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> ops = List.of(
				ops(add(1), null, null),
				ops(null, set(0, 3), null),
				ops(null, null, setName("", "test1")),
				ops(null, null, null),
				ops(add(12), set(3, 12), setName("test1", "test2")),
				ops(add(-5), set(12, 123), setName("test2", "test3"))
		);
		State state1 = new State();
		state1.init();
		ops.forEach(state1::apply);
		assertState("test3", 8, 123, state1);

		State state2 = new State();
		state2.init();
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> squashed = MERGED.squash(ops);

		assertEquals(List.of(ops(add(8), set(0, 123), setName("", "test3"))), squashed);

		System.out.println(squashed);
		squashed.forEach(state2::apply);

		assertEquals(state1, state2);
	}

	@Test
	public void testSquashAddSetAreEmpty() {
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> ops = List.of(
				ops(null, null, setName("", "aa")),
				ops(null, null, setName("aa", "aaa"))
		);
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> squashed = MERGED.squash(ops);
		assertEquals(List.of(ops(null, null, setName("", "aaa"))), squashed);
	}

	@Test
	public void testTransform() throws TransformException {
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> left = List.of(
				new Tuple3<>(List.of(add(12), add(13), add(-2)), List.of(), List.of()),
				new Tuple3<>(List.of(), List.of(set(0, 10), set(10, 14)), List.of()),
				new Tuple3<>(List.of(), List.of(), List.of(setName("", "left1"), setName("left1", "left2"))),
				new Tuple3<>(List.of(add(100), add(23)), List.of(set(14, 50), set(50, 3)), List.of(setName("left2", "left3"), setName("left3", "left4")))
		);
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> right = List.of(
				new Tuple3<>(List.of(), List.of(set(0, 3), set(3, 13)), List.of()),
				new Tuple3<>(List.of(), List.of(), List.of(setName("", "right1"), setName("right1", "right2"))),
				new Tuple3<>(List.of(add(-4), add(43)), List.of(), List.of()),
				new Tuple3<>(List.of(add(1000), add(-123)), List.of(set(13, 11), set(11, 22)), List.of(setName("right2", "right3"), setName("right3", "right4")))
		);

		State stateLeft = new State();
		stateLeft.init();

		State stateRight = new State();
		stateRight.init();

		left.forEach(stateLeft::apply);
		right.forEach(stateRight::apply);

		assertState("left4", 146, 3, stateLeft);
		assertState("right4", 916, 22, stateRight);

		TransformResult<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> transform = MERGED.transform(left, right);
		System.out.println(transform.left);
		System.out.println(transform.right);

		transform.left.forEach(stateLeft::apply);
		transform.right.forEach(stateRight::apply);

		assertEquals(stateLeft, stateRight);
	}

	@Test
	public void testTransformWithEmptyDiffs() throws TransformException {
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> diffs = List.of(
				new Tuple3<>(List.of(add(12), add(13), add(-2)), List.of(), List.of()),
				new Tuple3<>(List.of(), List.of(set(0, 10), set(10, 14)), List.of())
		);

		TransformResult<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> transform1 = MERGED.transform(diffs, List.of());
		assertTrue(transform1.left.isEmpty());

		TransformResult<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> transform2 = MERGED.transform(List.of(), List.of());
		assertTrue(transform2.left.isEmpty());
		assertTrue(transform2.right.isEmpty());
	}

	@Test
	public void testSquashWithEmptyDiffs() {
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> squashed = MERGED.squash(List.of());
		assertTrue(squashed.isEmpty());

		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> diffs = List.of(
				new Tuple3<>(List.of(add(12), add(13), add(-2)), List.of(), List.of())
		);
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> inverted = MERGED.invert(diffs);

		assertTrue(MERGED.squash(concat(diffs, inverted)).isEmpty());
	}

	@Test
	public void testInvertWithEmptyDiffs() {
		List<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> invert = MERGED.invert(List.of());
		assertTrue(invert.isEmpty());
	}

	// region helpers
	private static OTSystem<TestAdd> createAddIntSystem() {
		return OTSystemImpl.<TestAdd>create()
				.withEmptyPredicate(TestAdd.class, op -> op.getDelta() == 0)
				.withInvertFunction(TestAdd.class, op -> List.of(add(-op.getDelta())))
				.withSquashFunction(TestAdd.class, TestAdd.class, (first, second) -> add(first.getDelta() + second.getDelta()))
				.withTransformFunction(TestAdd.class, TestAdd.class, (left, right) -> TransformResult.of(right, left));
	}

	private static OTSystem<TestSet> createTestSetSystem() {
		return OTSystemImpl.<TestSet>create()
				.withEmptyPredicate(TestSet.class, op -> op.getPrev() == op.getNext())
				.withInvertFunction(TestSet.class, op -> List.of(set(op.getNext(), op.getPrev())))
				.withSquashFunction(TestSet.class, TestSet.class, (first, second) -> set(first.getPrev(), second.getNext()))
				.withTransformFunction(TestSet.class, TestSet.class, (left, right) -> {
					if (left.getNext() > right.getNext()) return right(set(right.getNext(), left.getNext()));
					if (left.getNext() < right.getNext()) return left(set(left.getNext(), right.getNext()));
					return TransformResult.empty();
				});
	}

	private static OTSystem<TestSetName> createTestSetNameSystem() {
		return OTSystemImpl.<TestSetName>create()
				.withEmptyPredicate(TestSetName.class, op -> op.getPrev().equals(op.getNext()))
				.withInvertFunction(TestSetName.class, op -> List.of(setName(op.getNext(), op.getPrev())))
				.withSquashFunction(TestSetName.class, TestSetName.class, (first, second) -> setName(first.getPrev(), second.getNext()))
				.withTransformFunction(TestSetName.class, TestSetName.class, (left, right) -> {
					if (left.getNext().compareTo(right.getNext()) > 0)
						return right(setName(right.getNext(), left.getNext()));
					if (left.getNext().compareTo(right.getNext()) < 0)
						return left(setName(left.getNext(), right.getNext()));
					return TransformResult.empty();
				});
	}

	private static void assertState(String name, int addVal, int setVal, State state) {
		assertEquals(name, state.getName());
		assertEquals(addVal, state.getAddVal());
		assertEquals(setVal, state.getSetVal());
	}

	private static Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>> ops(@Nullable TestAdd add, @Nullable TestSet set,
			@Nullable TestSetName setName) {
		List<TestAdd> testAdds = add == null ? List.of() : List.of(add);
		List<TestSet> testSets = set == null ? List.of() : List.of(set);
		List<TestSetName> testSetNames = setName == null ? List.of() : List.of(setName);
		return new Tuple3<>(testAdds, testSets, testSetNames);
	}

	static class State implements OTState<Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>>> {
		private String name;
		private int addVal;
		private int setVal;

		@Override
		public void init() {
			name = "";
			addVal = 0;
			setVal = 0;
		}

		@Override
		public void apply(Tuple3<List<TestAdd>, List<TestSet>, List<TestSetName>> op) {
			op.value1().forEach(add -> addVal += add.getDelta());
			op.value2().forEach(set -> {
				assert setVal == set.getPrev();
				setVal = set.getNext();
			});
			op.value3().forEach(setName -> {
				assert name.equals(setName.getPrev());
				name = setName.getNext();
			});
		}

		public int getAddVal() {
			return addVal;
		}

		public int getSetVal() {
			return setVal;
		}

		public String getName() {
			return name;
		}

		public boolean isEmpty() {
			return addVal == 0 && setVal == 0.0 && name.equals("");
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			State state = (State) o;

			if (addVal != state.addVal) return false;
			if (Double.compare(state.setVal, setVal) != 0) return false;
			return name.equals(state.name);

		}

		@Override
		public int hashCode() {
			int result;
			long temp;
			result = name.hashCode();
			result = 31 * result + addVal;
			temp = Double.doubleToLongBits(setVal);
			result = 31 * result + (int) (temp ^ (temp >>> 32));
			return result;
		}
	}
	// endregion
}
