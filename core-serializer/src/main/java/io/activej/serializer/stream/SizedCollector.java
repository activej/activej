package io.activej.serializer.stream;

public interface SizedCollector<T, A, R> {
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

}
