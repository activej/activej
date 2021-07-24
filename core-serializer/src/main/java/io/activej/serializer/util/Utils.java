package io.activej.serializer.util;

import java.util.function.Supplier;

public class Utils {
	public static <T> T of(Supplier<T> supplier) {
		return supplier.get();
	}
}
