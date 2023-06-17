package io.activej.serializer.stream;

import java.util.Collection;
import java.util.function.IntFunction;

interface SizedCollector<T, A, R> {
	default R create0() {
		A acc = accumulator(0);
		return result(acc);
	}

	default R create1(T item) {
		A acc = accumulator(1);
		accumulate(acc, 0, item);
		return result(acc);
	}

	A accumulator(int size);

	void accumulate(A accumulator, int index, T item);

	R result(A accumulator);

	static <T, C extends Collection<T>> SizedCollector<T, C, C> toCollection(IntFunction<C> factory) {
		return new SizedCollector<>() {
			@Override
			public C accumulator(int size) {
				return factory.apply(size);
			}

			@Override
			public void accumulate(C collection, int index, T item) {
				collection.add(item);
			}

			@Override
			public C result(C collection) {
				return collection;
			}
		};
	}
}
