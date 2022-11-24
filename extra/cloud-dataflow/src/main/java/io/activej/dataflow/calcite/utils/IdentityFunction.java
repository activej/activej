package io.activej.dataflow.calcite.utils;

import java.util.function.Function;

public final class IdentityFunction<T> implements Function<T, T> {
	private static final IdentityFunction<Object> INSTANCE = new IdentityFunction<>();

	private IdentityFunction() {
	}

	public static <T> IdentityFunction<T> getInstance() {
		//noinspection unchecked
		return (IdentityFunction<T>) INSTANCE;
	}

	@Override
	public T apply(T t) {
		return t;
	}
}
