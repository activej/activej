package io.activej.specializer;

public class TestClass0b implements TestInterface {
	public final int finalX;

	public TestClass0b(int x) {
		finalX = x;
	}

	@Override
	public int apply(int arg) {
		return finalX;
	}
}
