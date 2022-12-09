package io.activej.specializer;

public class IfElseTestClass3 implements TestInterface {

	@Override
	public int apply(int arg) {
		long rhsObj = 0L;

		int value = 0;
		if (value != 0) {
			rhsObj = value;
		}
		return x(rhsObj);
	}

	private int x(Object val) {
		return 0;
	}
}
