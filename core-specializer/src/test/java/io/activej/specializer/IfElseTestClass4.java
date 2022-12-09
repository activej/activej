package io.activej.specializer;

public class IfElseTestClass4 implements TestInterface {

	@Override
	public int apply(int arg) {
		int rhsObj = 0;

		short value = 0;
		if (value != 0) {
			rhsObj = value;
		}
		return x(rhsObj);
	}

	private int x(Object val) {
		return 0;
	}
}
