package io.activej.common.recycle;

/**
 * A generic interface that describes how an item should be recycled
 *
 * @param <T> a type of object to be recycled
 */
public interface Recycler<T> {
	/**
	 * Recycles an item
	 * <p>
	 * Frees some resource that this item possesses, e.g. returns an item to the pool, etc.
	 *
	 * @param item an item to be recycled
	 */
	void recycle(T item);
}
