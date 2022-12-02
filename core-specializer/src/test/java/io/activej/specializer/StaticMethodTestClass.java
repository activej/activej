package io.activej.specializer;

public class StaticMethodTestClass implements TestInterface {
	@Override
	public int apply(int arg) {
		int a = getValue();

		int b = StaticHolder.getValue();

		int convert1 = convertValue(arg);
		int convert2 = StaticHolder.convertValue(convert1);

		return a + b + convert1 + convert2 + arg;
	}

	public static int getValue() {
		return 10;
	}

	public static int convertValue(int x) {
		return x * 2;
	}
}
