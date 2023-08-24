package io.activej.serializer.stream;

public interface SizedCollectorKV<K, V, A, R> {
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

}
