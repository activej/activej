package io.activej.datastream.processor.join;

import io.activej.datastream.supplier.StreamDataAcceptor;

/**
 * Represents a left joiner that produces only left inner joins
 */
public abstract class LeftInnerLeftJoiner<K, L, R, V> implements LeftJoiner<K, L, R, V> {
	/**
	 * Left outer join does nothing for absence null fields in result left inner join
	 */
	@Override
	public void onOuterJoin(K key, L left, StreamDataAcceptor<V> output) {
	}
}
