package io.activej.datastream.processor.join;

import io.activej.datastream.supplier.StreamDataAcceptor;

/**
 * It is the primary interface of a left joiner. It contains methods which will left join streams
 */
public interface LeftJoiner<K, L, R, V> {
	/**
	 * Streams objects with all fields from both received streams as long as there is a match
	 * between the keys in both items.
	 */
	void onInnerJoin(K key, L left, R right, StreamDataAcceptor<V> output);

	/**
	 * Streams objects with all fields from the left stream, with the matching key - fields in the
	 * right stream. The field of result object is NULL in the right stream when there is no match.
	 */
	void onOuterJoin(K key, L left, StreamDataAcceptor<V> output);
}
