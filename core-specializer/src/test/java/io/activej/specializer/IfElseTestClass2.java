package io.activej.specializer;

public class IfElseTestClass2 implements TestInterface {

	@Override
	public int apply(int arg) {
		Object rhsObj = null;

		Integer value = 0;
		if (value != null) {
			rhsObj = value;
		}
		return x(rhsObj);
	}

	private int x(Object val) {
		return 0;
	}
}
