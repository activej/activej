package io.activej.serializer.stream;

import java.util.Map;
import java.util.function.IntFunction;

interface SizedCollectorKV<K, V, A, R> {
	default R create0() {
		A acc = accumulator(0);
		return result(acc);
	}

	default R create1(K key, V value) {
		A acc = accumulator(1);
		accumulate(acc, 0, key, value);
		return result(acc);
	}

	A accumulator(int size);

	void accumulate(A accumulator, int index, K key, V value);

	R result(A accumulator);

	static <K, V, M extends Map<K, V>> SizedCollectorKV<K, V, M, M> toMap(IntFunction<M> factory) {
		return new SizedCollectorKV<>() {
			@Override
			public M accumulator(int size) {
				return factory.apply(size);
			}

			@Override
			public void accumulate(M map, int index, K key, V value) {
				map.put(key, value);
			}

			@Override
			public M result(M map) {
				return map;
			}
		};
	}
}
