package io.activej.ot.utils;

import static io.activej.common.Preconditions.checkState;

public class TestSet implements TestOp {
	private final int prev;
	private final int next;

	public TestSet(int prev, int next) {
		this.prev = prev;
		this.next = next;
	}

	public TestSet inverse() {
		return new TestSet(next, prev);
	}

	public int getPrev() {
		return prev;
	}

	public int getNext() {
		return next;
	}

	@Override
	public String toString() {
		return prev + ":=" + next;
	}

	@Override
	public int apply(int prev) {
		checkState(prev == this.prev);
		return next;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TestSet testSet = (TestSet) o;

		if (prev != testSet.prev) return false;
		return next == testSet.next;
	}

	@Override
	public int hashCode() {
		int result = prev;
		result = 31 * result + next;
		return result;
	}
}
