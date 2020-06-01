package io.activej.specializer;

public class TestClass0 implements TestInterface {
	public final TestClass0 next;
	public final int finalX;

	public TestClass0(TestClass0 next) {
		this.next = next;
		this.finalX = 0;
	}

	public TestClass0(int x) {
		this.next = null;
		this.finalX = x;
	}

	@Override
	public int apply(int v) {
		return next != null ? v + next.apply(v) : v + finalX;
	}
}
