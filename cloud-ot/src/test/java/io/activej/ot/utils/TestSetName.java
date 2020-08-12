package io.activej.ot.utils;

public final class TestSetName {
	private final String prev;
	private final String next;

	private TestSetName(String prev, String next) {
		this.prev = prev;
		this.next = next;
	}

	public static TestSetName setName(String prev, String next) {
		return new TestSetName(prev, next);
	}

	public String getPrev() {
		return prev;
	}

	public String getNext() {
		return next;
	}

	@Override
	@SuppressWarnings("RedundantIfStatement")
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TestSetName setName = (TestSetName) o;

		if (!prev.equals(setName.prev)) return false;
		if (!next.equals(setName.next)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = prev.hashCode();
		result = 31 * result + next.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return prev + ":=" + next;
	}
}
