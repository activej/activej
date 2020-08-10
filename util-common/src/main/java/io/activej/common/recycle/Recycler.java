package io.activej.common.recycle;

public interface Recycler<T> {
	void recycle(T item);
}
