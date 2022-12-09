package io.activej.specializer;

public class IfElseTestClass5 implements TestInterface {

	@Override
	public int apply(int arg) {
		Integer rhsObj = 0;

		Object value = 0;
		if (value != null) {
			rhsObj = (Integer) value;
		}
		return x(rhsObj);
	}

	private int x(Object val) {
		return 0;
	}
}
