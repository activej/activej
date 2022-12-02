package io.activej.specializer;

public class StaticHolder {
	public static int field = 10;

	public static int getValue() {
		return field;
	}

	public static int convertValue(int x) {
		return x * 3;
	}
}
