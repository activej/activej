package io.activej.specializer;

public class StaticFieldTestClass implements TestInterface {
	private static int x = 1;

	@Override
	public int apply(int arg) {
		int a = x;
		x = -3;

		int b = a + x;

		int c = StaticHolder.field;
		StaticHolder.field = -1;

		int d = c + StaticHolder.field;

		x = 1;
		StaticHolder.field = 10;

		return a + b + c + d + arg;
	}
}
