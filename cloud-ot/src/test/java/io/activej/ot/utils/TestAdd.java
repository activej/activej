package io.activej.ot.utils;

import java.util.Objects;

public class TestAdd implements TestOp {
	private final int delta;

	public TestAdd(int delta) {
		this.delta = delta;
	}

	public TestAdd inverse() {
		return new TestAdd(-delta);
	}

	public int getDelta() {
		return delta;
	}

	@Override
	public String toString() {
		return (delta > 0 ? "+" : "") + delta;
	}

	@Override
	public int apply(int value) {
		return value + delta;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TestAdd testAdd = (TestAdd) o;
		return delta == testAdd.delta;
	}

	@Override
	public int hashCode() {

		return Objects.hash(delta);
	}
}
